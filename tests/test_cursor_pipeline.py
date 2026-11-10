"""游标与采集主流程的集成测试：断点续采、跳过一致性、空列表、重置、单源隔离。

覆盖验收点 1/2/3/5/6 与「游标在条目处理完成后再写入」的隐性约束。
"""

from __future__ import annotations

from typing import Any

from crawl_flow.cursor.store import CursorStore
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


class CountingFetcher:
    """记录每条详情 URL 请求次数的 Fetcher 包装，用于断言「不重复请求」。"""

    def __init__(self, inner: Fetcher) -> None:
        self._inner = inner
        self.detail_calls: dict[str, int] = {}
        self.detail_order: list[str] = []

    def get(self, url: str, params: dict[str, object] | None = None) -> FetchResult:
        if "/item/" in url or "/api/items/" in url:
            self.detail_calls[url] = self.detail_calls.get(url, 0) + 1
            self.detail_order.append(url)
        return self._inner.get(url, params=params)

    def close(self) -> None:
        self._inner.close()

    def __enter__(self) -> "CountingFetcher":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def _cursor_payload(store: CursorStore, source: str = "demo_api") -> dict[str, Any]:
    payload = store.load(source)
    assert payload is not None
    return payload


def test_first_run_initializes_and_second_run_skips_completed(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    source = make_source(cursor={"enabled": True, "rule": "completed_ids"})

    first = run_source(
        source, db, raw_store, fetcher=fetcher, limit=5, cursor_store=cursor_store
    )
    assert (first.ok, first.cursor_skipped) == (5, 0)
    payload = _cursor_payload(cursor_store)
    assert payload["rule"] == "completed_ids"
    assert len(payload["state"]["completed_ids"]) == 5

    second = run_source(
        source, db, raw_store, fetcher=fetcher, cursor_store=cursor_store
    )
    # 二次运行：首轮完成的 5 条被游标跳过，剩余 10 条继续处理。
    assert second.ok == 10
    assert second.failed == 0
    assert second.cursor_skipped == 5
    assert db.count_records("demo_api") == 15


def test_resume_does_not_reissue_requests_for_completed_items(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    source = make_source(cursor={"enabled": True, "rule": "completed_ids"})
    counter = CountingFetcher(fetcher)

    run_source(
        source, db, raw_store, fetcher=counter, limit=5, cursor_store=cursor_store
    )
    requested_first = dict(counter.detail_calls)
    assert len(requested_first) == 5

    run_source(source, db, raw_store, fetcher=counter, cursor_store=cursor_store)
    # 续跑时已完成的 5 条详情一次都不会再请求。
    for url, count in requested_first.items():
        assert counter.detail_calls.get(url, 0) == count
    # 剩余 10 条各请求一次。
    new_urls = [url for url in counter.detail_calls if url not in requested_first]
    assert len(new_urls) == 10
    assert all(counter.detail_calls[url] == 1 for url in new_urls)


def test_resume_continues_from_interrupted_position(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    """模拟在处理第 3 条期间强制终止：前 2 条游标已落盘，第 3 条未推进。"""

    source = make_source(cursor={"enabled": True, "rule": "completed_ids"})

    class InterruptError(RuntimeError):
        pass

    class InterruptFetcher(CountingFetcher):
        def get(self, url: str, params: dict[str, object] | None = None) -> FetchResult:
            result = super().get(url, params)
            if url.endswith("/api/items/api-3"):
                raise InterruptError("simulated SIGTERM during detail fetch")
            return result

    interrupter = InterruptFetcher(fetcher)
    try:
        run_source(
            source,
            db,
            raw_store,
            fetcher=interrupter,
            cursor_store=cursor_store,
        )
    except InterruptError:
        pass

    # 中断点：仅 api-1/api-2 完成并落盘，api-3 在处理中崩溃、游标未推进。
    payload = _cursor_payload(cursor_store)
    assert set(payload["state"]["completed_ids"]) == {"api-1", "api-2"}

    # 重启后续跑：api-1/api-2 跳过，api-3 重新处理（崩溃当次未完成），之后继续。
    counter = CountingFetcher(fetcher)
    resumed = run_source(
        source, db, raw_store, fetcher=counter, cursor_store=cursor_store
    )
    assert resumed.cursor_skipped == 2
    assert resumed.ok == 13
    base = source.base_url
    re_requested = [
        url
        for url in counter.detail_order
        if url in (f"{base}/api/items/api-1", f"{base}/api/items/api-2")
    ]
    assert re_requested == []
    assert f"{base}/api/items/api-3" in counter.detail_order


def test_cursor_recorded_count_equals_actual_skipped_count(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    source = make_source(cursor={"enabled": True, "rule": "completed_ids"})
    run_source(
        source, db, raw_store, fetcher=fetcher, limit=7, cursor_store=cursor_store
    )
    recorded = len(_cursor_payload(cursor_store)["state"]["completed_ids"])

    second = run_source(
        source, db, raw_store, fetcher=fetcher, limit=7, cursor_store=cursor_store
    )
    # 游标记录的条目数量与实际跳过数量必须完全一致。
    assert recorded == 7
    assert second.cursor_skipped == recorded


def test_empty_list_does_not_advance_cursor(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    source = make_source(list_path="/api/items?empty=1", cursor={"enabled": True})
    summary = run_source(
        source, db, raw_store, fetcher=fetcher, cursor_store=cursor_store
    )
    assert (summary.ok, summary.failed, summary.cursor_skipped) == (0, 0, 0)
    # 空列表不会产生游标文件，游标不会空转推进。
    assert cursor_store.list_sources() == []


def test_manual_reset_returns_to_full_crawl(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    source = make_source(cursor={"enabled": True, "rule": "completed_ids"})
    run_source(
        source, db, raw_store, fetcher=fetcher, limit=4, cursor_store=cursor_store
    )
    assert cursor_store.reset("demo_api") is True

    full = run_source(
        source, db, raw_store, fetcher=fetcher, limit=4, cursor_store=cursor_store
    )
    # 重置后回到全量：没有跳过，前 4 条重新处理。
    assert full.cursor_skipped == 0
    assert full.ok == 4


def test_dry_run_does_not_write_or_read_cursor(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    source = make_source(cursor={"enabled": True})
    run_source(
        source,
        db,
        raw_store,
        fetcher=fetcher,
        limit=3,
        dry_run=True,
        cursor_store=cursor_store,
    )
    assert cursor_store.list_sources() == []


def test_corrupt_cursor_degrades_and_run_completes(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    source = make_source(cursor={"enabled": True})
    cursor_store.path_for("demo_api").parent.mkdir(parents=True, exist_ok=True)
    cursor_store.path_for("demo_api").write_text("<<<not json>>>", encoding="utf-8")

    summary = run_source(
        source, db, raw_store, fetcher=fetcher, limit=3, cursor_store=cursor_store
    )
    # 损坏降级为全量采集：任务不崩，前 3 条正常处理、无跳过。
    assert summary.ok == 3
    assert summary.cursor_skipped == 0


def test_list_position_rule_resumes_by_offset(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    source = make_source(cursor={"enabled": True, "rule": "list_position"})
    first = run_source(
        source, db, raw_store, fetcher=fetcher, limit=6, cursor_store=cursor_store
    )
    assert first.ok == 6
    payload = _cursor_payload(cursor_store)
    assert payload["rule"] == "list_position"
    assert payload["state"]["completed_count"] == 6

    second = run_source(
        source, db, raw_store, fetcher=fetcher, cursor_store=cursor_store
    )
    assert second.cursor_skipped == 6
    assert second.ok == 9


def test_failed_items_are_marked_and_not_retried_on_resume(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    # 详情 500 的故障源：失败条目走完隔离流程后游标同样推进，续跑不重复请求。
    source = make_source(
        "demo_api_flaky",
        detail_path="/api/items/{id}?fail=1",
        cursor={"enabled": True, "rule": "completed_ids"},
    )
    first = run_source(
        source, db, raw_store, fetcher=fetcher, limit=3, cursor_store=cursor_store
    )
    assert (first.ok, first.failed) == (0, 3)
    assert len(_cursor_payload(cursor_store, "demo_api_flaky")["state"]["completed_ids"]) == 3

    counter = CountingFetcher(fetcher)
    second = run_source(
        source, db, raw_store, fetcher=counter, cursor_store=cursor_store
    )
    # 前 3 条失败但已走完流程并记录游标，续跑时跳过、不再发请求；其余 12 条继续。
    assert second.cursor_skipped == 3
    assert second.failed == 12
    base = source.base_url
    skipped_urls = {f"{base}/api/items/api-{i}?fail=1" for i in range(1, 4)}
    assert not (skipped_urls & set(counter.detail_calls))
    assert len(counter.detail_calls) == 12


def test_disabled_cursor_runs_full_every_time(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    cursor_store: CursorStore,
    fetcher: Fetcher,
) -> None:
    source = make_source(cursor={"enabled": False})
    run_source(
        source, db, raw_store, fetcher=fetcher, limit=3, cursor_store=cursor_store
    )
    assert cursor_store.list_sources() == []
    again = run_source(
        source, db, raw_store, fetcher=fetcher, limit=3, cursor_store=cursor_store
    )
    assert again.cursor_skipped == 0
    assert again.ok == 3


def test_cursor_without_store_keeps_legacy_full_crawl(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    fetcher: Fetcher,
) -> None:
    # 即使配置了 cursor，未传入 cursor_store（旧调用方式）也保持全量行为。
    source = make_source(cursor={"enabled": True})
    summary = run_source(source, db, raw_store, fetcher=fetcher, limit=2)
    assert (summary.ok, summary.cursor_skipped) == (2, 0)
