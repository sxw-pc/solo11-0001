"""翻页状态机边界测试：空页 / 错页 / 整数倍 / 重复页，API 与 HTML 共用一套逻辑。

同时覆盖：
- 请求计数与 HTTP 客户端逐次对齐（列表请求次数 == 状态机 pages_fetched）；
- dry-run 与正式执行的翻页行为一致；
- 翻页状态快照可用于断点续采 / 失败重试；
- 同一源并发两个翻页任务状态互不污染；
- 500 页上限保护保留；
- 正常翻页与 limit/skipped 数量语义不回归。
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from typing import Any

import pytest

from crawl_flow.errors import FetchError, PaginationError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.pipeline.run import run_source
from crawl_flow.sources import (
    DemoApiAdapter,
    DemoHtmlAdapter,
    PaginationState,
    PaginationStateMachine,
)
from crawl_flow.sources.base import MAX_PAGES, ListItem
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


@dataclass
class RecordedRequest:
    url: str
    params: dict[str, object] | None


class RecordingFetcher:
    """记录每一次 GET 的包装器，请求计数可与真实 HTTP 客户端日志逐次对齐。"""

    def __init__(self, inner: Fetcher) -> None:
        self._inner = inner
        self.requests: list[RecordedRequest] = []

    def get(self, url: str, params: dict[str, object] | None = None) -> FetchResult:
        self.requests.append(RecordedRequest(url=url, params=dict(params or {})))
        return self._inner.get(url, params=params)

    def close(self) -> None:
        self._inner.close()


def _pages_and_state(adapter: Any) -> tuple[list[Any], PaginationState]:
    machine = PaginationStateMachine(adapter, max_pages=MAX_PAGES)
    pages = list(machine.run())
    return pages, machine.state


# ---------------------------------------------------------------------------
# 1. 空页：立即终止且不再发请求
# ---------------------------------------------------------------------------


def test_api_empty_page_stops_without_extra_request(
    api_source: Any, fetcher: Fetcher
) -> None:
    source = api_source.model_copy(update={"list_path": "/api/items?empty=1"})
    recording = RecordingFetcher(fetcher)
    adapter = DemoApiAdapter(source, recording)

    pages, state = _pages_and_state(adapter)

    assert len(pages) == 1
    assert pages[0].page == 1
    assert pages[0].items == []
    assert len(recording.requests) == 1
    assert state.pages_fetched == 1
    assert state.finished is True
    assert state.stop_reason == "empty_page"


def test_html_empty_page_stops_without_extra_request(
    html_source: Any, fetcher: Fetcher
) -> None:
    source = html_source.model_copy(update={"list_path": "/list?empty=1"})
    recording = RecordingFetcher(fetcher)
    adapter = DemoHtmlAdapter(source, recording)

    pages, state = _pages_and_state(adapter)

    assert len(pages) == 1
    assert pages[0].items == []
    assert len(recording.requests) == 1
    assert state.pages_fetched == 1
    assert state.stop_reason == "empty_page"


def test_empty_source_run_summary_is_all_zero(
    api_source: Any, db: Database, raw_store: RawStore, fetcher: Fetcher
) -> None:
    source = api_source.model_copy(update={"list_path": "/api/items?empty=1"})
    summary = run_source(source, db, raw_store, fetcher=fetcher)

    assert (summary.ok, summary.failed, summary.skipped) == (0, 0, 0)
    assert summary.fatal_error is None


# ---------------------------------------------------------------------------
# 2. 错页：响应页码与请求页码不一致 → 抓取错误终止整轮，不触发 500 页保护
# ---------------------------------------------------------------------------


def test_api_response_page_mismatch_aborts_immediately(
    api_source: Any, fetcher: Fetcher
) -> None:
    source = api_source.model_copy(update={"list_path": "/api/items?badpage=1"})
    recording = RecordingFetcher(fetcher)
    adapter = DemoApiAdapter(source, recording)

    with pytest.raises(PaginationError) as excinfo:
        list(adapter.iter_list_pages())

    # 第 1 页响应自报第 2 页：只请求 1 次就终止，而不是重复到 500 页上限。
    assert "request_page=1" in str(excinfo.value)
    assert "response_page=2" in str(excinfo.value)
    assert len(recording.requests) == 1
    assert recording.requests[0].params == {"page": 1, "page_size": 5}


def test_html_response_page_mismatch_aborts_immediately(
    html_source: Any, fetcher: Fetcher
) -> None:
    source = html_source.model_copy(update={"list_path": "/list?badpage=1"})
    recording = RecordingFetcher(fetcher)
    adapter = DemoHtmlAdapter(source, recording)

    with pytest.raises(PaginationError) as excinfo:
        list(adapter.iter_list_pages())

    assert "request_page=1" in str(excinfo.value)
    assert "response_page=2" in str(excinfo.value)
    assert len(recording.requests) == 1


def test_mismatch_is_fetch_error_and_marks_round_fatal(
    api_source: Any, db: Database, raw_store: RawStore, fetcher: Fetcher
) -> None:
    source = api_source.model_copy(update={"list_path": "/api/items?badpage=1"})
    with pytest.raises(FetchError):
        run_source(source, db, raw_store, fetcher=fetcher)


# ---------------------------------------------------------------------------
# 3. 整数倍：limit 恰好是每页条数整数倍时不产生多余请求，skipped 准确
# ---------------------------------------------------------------------------


def test_api_integer_multiple_makes_no_extra_page_request(
    api_source: Any, fetcher: Fetcher
) -> None:
    recording = RecordingFetcher(fetcher)
    adapter = DemoApiAdapter(api_source, recording)

    pages, state = _pages_and_state(adapter)

    # 15 条 / 每页 5 条：3 页取满即停，不再请求第 4 页空页。
    assert [page.page for page in pages] == [1, 2, 3]
    assert [len(page.items) for page in pages] == [5, 5, 5]
    assert len(recording.requests) == 3
    assert [req.params["page"] for req in recording.requests] == [1, 2, 3]
    assert state.items_seen == 15
    assert state.stop_reason == "total_reached"


def test_html_integer_multiple_stops_on_last_page(
    html_source: Any, fetcher: Fetcher
) -> None:
    recording = RecordingFetcher(fetcher)
    adapter = DemoHtmlAdapter(html_source, recording)

    pages, state = _pages_and_state(adapter)

    # HTML 没有总数信号：第 3 页没有「下一页」链接，处理完即停，不请求第 4 页。
    assert [page.page for page in pages] == [1, 2, 3]
    assert len(recording.requests) == 3
    assert state.items_seen == 15
    assert state.stop_reason == "no_next_page"


def test_limit_integer_multiple_skipped_count_is_exact(
    api_source: Any, db: Database, raw_store: RawStore, fetcher: Fetcher
) -> None:
    summary = run_source(api_source, db, raw_store, fetcher=fetcher, limit=5)

    # 现有数量语义不回归：处理 5 条，剩余 10 条全部计 skipped。
    assert (summary.ok, summary.failed, summary.skipped) == (5, 0, 10)


def test_dry_run_and_real_run_share_pagination(
    api_source: Any, db: Database, raw_store: RawStore, fetcher: Fetcher
) -> None:
    dry = run_source(
        api_source, db, raw_store, fetcher=fetcher, limit=5, dry_run=True
    )
    real = run_source(api_source, db, raw_store, fetcher=fetcher, limit=5)

    assert (dry.ok, dry.failed, dry.skipped) == (5, 0, 10)
    assert (real.ok, real.failed, real.skipped) == (5, 0, 10)
    assert dry.fatal_error is None and real.fatal_error is None


# ---------------------------------------------------------------------------
# 4. 重复页：连续两页指纹完全相同 → 识别并停止
# ---------------------------------------------------------------------------


def test_api_repeated_page_is_detected_and_stops(
    api_source: Any, fetcher: Fetcher
) -> None:
    source = api_source.model_copy(update={"list_path": "/api/items?dup=1"})
    recording = RecordingFetcher(fetcher)
    adapter = DemoApiAdapter(source, recording)

    with pytest.raises(PaginationError) as excinfo:
        list(adapter.iter_list_pages())

    assert "连续重复" in str(excinfo.value)
    # 第 1 页正常产出，第 2 页与第 1 页相同即报错；不会打到 500 页。
    assert len(recording.requests) == 2
    assert [req.params["page"] for req in recording.requests] == [1, 2]


def test_html_repeated_page_is_detected_and_stops(
    html_source: Any, fetcher: Fetcher
) -> None:
    source = html_source.model_copy(update={"list_path": "/list?dup=1"})
    recording = RecordingFetcher(fetcher)
    adapter = DemoHtmlAdapter(source, recording)

    with pytest.raises(PaginationError) as excinfo:
        list(adapter.iter_list_pages())

    assert "连续重复" in str(excinfo.value)
    assert len(recording.requests) == 2


# ---------------------------------------------------------------------------
# 请求计数与 HTTP 客户端逐次对齐
# ---------------------------------------------------------------------------


def test_request_count_matches_client_calls(
    api_source: Any, fetcher: Fetcher
) -> None:
    recording = RecordingFetcher(fetcher)
    adapter = DemoApiAdapter(api_source, recording)

    machine = PaginationStateMachine(adapter, max_pages=MAX_PAGES)
    list(machine.run())

    assert machine.state.pages_fetched == len(recording.requests) == 3
    assert [req.params["page"] for req in recording.requests] == [1, 2, 3]


# ---------------------------------------------------------------------------
# 断点续采 / 失败重试：状态快照可序列化、可恢复
# ---------------------------------------------------------------------------


def test_pagination_state_snapshot_roundtrip() -> None:
    state = PaginationState(
        page=3,
        pages_fetched=2,
        items_seen=10,
        total_items=15,
        last_fingerprint=("api-6", "api-7"),
        finished=False,
        stop_reason=None,
    )

    restored = PaginationState.from_dict(state.to_dict())

    assert restored == state
    assert isinstance(restored.last_fingerprint, tuple)


def test_pagination_resumes_from_snapshot(
    api_source: Any, fetcher: Fetcher
) -> None:
    recording = RecordingFetcher(fetcher)
    adapter = DemoApiAdapter(api_source, recording)

    # 模拟前两轮只翻到第 2 页后中断，持久化快照。
    first = PaginationStateMachine(adapter, max_pages=MAX_PAGES)
    pages: list[Any] = []
    for index, page in enumerate(first.run()):
        pages.append(page)
        if index == 1:
            break
    snapshot = first.snapshot()
    # 第 2 页处理完后页码已推进到 3，累计 10 条。
    assert snapshot["page"] == 3
    assert snapshot["items_seen"] == 10
    assert snapshot["pages_fetched"] == 2

    # 用快照恢复后继续翻页：只补发第 3 页，且不重复第 1、2 页的请求。
    resumed = PaginationStateMachine.restore(
        adapter, snapshot, max_pages=MAX_PAGES
    )
    remaining = list(resumed.run())

    assert [page.page for page in remaining] == [3]
    assert resumed.state.finished is True
    assert resumed.state.items_seen == 15
    assert [req.params["page"] for req in recording.requests] == [1, 2, 3]


def test_restored_finished_state_does_not_refetch(
    api_source: Any, fetcher: Fetcher
) -> None:
    recording = RecordingFetcher(fetcher)
    adapter = DemoApiAdapter(api_source, recording)
    machine = PaginationStateMachine(adapter, max_pages=MAX_PAGES)
    list(machine.run())

    resumed = PaginationStateMachine.restore(
        adapter, machine.snapshot(), max_pages=MAX_PAGES
    )
    assert list(resumed.run()) == []
    # 恢复已完成的状态不会再发起任何请求。
    assert len(recording.requests) == 3


# ---------------------------------------------------------------------------
# 同一源并发两个翻页任务状态互不污染
# ---------------------------------------------------------------------------


def test_concurrent_iterations_do_not_share_state(
    api_source: Any, fetcher: Fetcher
) -> None:
    adapter = DemoApiAdapter(api_source, fetcher)
    outcomes: dict[str, list[Any]] = {"a": [], "b": []}
    errors: list[BaseException] = []

    def worker(name: str) -> None:
        try:
            for page in adapter.iter_list_pages():
                outcomes[name].append(page.page)
        except BaseException as exc:  # pragma: no cover - 并发失败时留证
            errors.append(exc)

    thread_a = threading.Thread(target=worker, args=("a",))
    thread_b = threading.Thread(target=worker, args=("b",))
    thread_a.start()
    thread_b.start()
    thread_a.join(timeout=10)
    thread_b.join(timeout=10)

    assert not errors
    assert outcomes["a"] == [1, 2, 3]
    assert outcomes["b"] == [1, 2, 3]


# ---------------------------------------------------------------------------
# 500 页上限保护保留
# ---------------------------------------------------------------------------


class _LoopingFetcher:
    """永不终止的假 HTTP 客户端：每页返回不同 id，无页码 / 总数 / 下一页信号。"""

    def __init__(self) -> None:
        self.calls = 0

    def get(self, url: str, params: dict[str, object] | None = None) -> FetchResult:
        self.calls += 1
        page = int((params or {})["page"])
        # 每页 2 条且 id 随页码变化，既不触发短页终止也不触发重复页检测。
        body = json.dumps(
            {"items": [{"id": f"loop-{page}-1"}, {"id": f"loop-{page}-2"}]}
        )
        return FetchResult(url=url, status_code=200, body=body)

    def close(self) -> None:
        pass


class _LoopingAdapter(DemoApiAdapter):
    """每页都返回新 id 且永不终止的适配器，用于验证 500 页上限。"""

    def list_response_page(self, result: FetchResult) -> int | None:
        return None

    def list_total_items(self, result: FetchResult) -> int | None:
        # 假响应不含 total，显式屏蔽父类的 JSON 总数钩子。
        return None

    def list_has_next(self, result: FetchResult) -> bool | None:
        return None

    @property
    def page_size_hint(self) -> int:
        return 2

    def parse_list(self, result: FetchResult) -> list[ListItem]:
        ids = [item["id"] for item in json.loads(result.body)["items"]]
        return [ListItem(external_id=item_id, url=f"http://mock/item/{item_id}") for item_id in ids]


def test_max_pages_guard_is_retained(api_source: Any) -> None:
    fetcher = _LoopingFetcher()
    adapter = _LoopingAdapter(api_source, fetcher)

    with pytest.raises(FetchError) as excinfo:
        list(adapter.iter_list_pages())

    assert f"max_pages={MAX_PAGES}" in str(excinfo.value)
    assert not isinstance(excinfo.value, PaginationError)
    # 500 页保护在第 501 次请求发起前生效，实际请求恰好 500 次。
    assert fetcher.calls == MAX_PAGES


# ---------------------------------------------------------------------------
# 正常翻页不回归：HTML 短页（无下一页信号的适配器）也能正常终止
# ---------------------------------------------------------------------------


class _ShortPageAdapter(DemoApiAdapter):
    """不提供总数 / 下一页信号，依赖短页规则终止。"""

    def list_response_page(self, result: FetchResult) -> int | None:
        return None

    def list_total_items(self, result: FetchResult) -> int | None:
        return None

    def list_has_next(self, result: FetchResult) -> bool | None:
        return None


def test_short_page_termination_when_no_signals(
    api_source: Any, fetcher: Fetcher
) -> None:
    # page_size=10：15 条数据 → 第 1 页 10 条（整页），第 2 页 5 条（短页）终止。
    source = api_source.model_copy(update={"page_size": 10})
    recording = RecordingFetcher(fetcher)
    adapter = _ShortPageAdapter(source, recording)

    pages, state = _pages_and_state(adapter)

    assert [len(page.items) for page in pages] == [10, 5]
    assert len(recording.requests) == 2
    assert state.stop_reason == "short_page"
    assert state.items_seen == 15
