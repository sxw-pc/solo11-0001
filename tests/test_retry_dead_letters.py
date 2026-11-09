"""指数退避重试与 dead_letters 死信队列。

覆盖：
- 详情持续失败 → 按 1s/2s/4s 退避重试 3 次（共 4 次尝试）后写入 dead_letters；
- 重试后成功 → 正常入库且不留死信，尝试次数被记录；
- 后续重跑成功 → 旧死信被删除；
- dead_letters 按 (source, external_id) upsert，不重复建行；
- ``crawl-flow dead-letters list`` 按 source 查看；
- dry-run 不写死信；records 的幂等语义不受重试影响。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.config import RetryConfig, SourceConfig
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.pipeline.retry import DetailAttemptError, backoff_delay, run_with_retries
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


class FlakyFetcher:
    """列表页走真实 mock；详情前 ``fail_times`` 次返回 500，之后返回 200。"""

    def __init__(self, base_url: str, *, fail_times: int) -> None:
        self._base_url = base_url.rstrip("/")
        self._fail_times = fail_times
        self._delegate = Fetcher(timeout=5.0)
        self.calls: dict[str, int] = {}

    def get(self, url: str, params: dict[str, object] | None = None) -> FetchResult:
        if "/api/items/" not in url:
            return self._delegate.get(url, params=params)
        self.calls[url] = self.calls.get(url, 0) + 1
        if self.calls[url] <= self._fail_times:
            return FetchResult(
                url=url,
                status_code=500,
                headers={"content-type": "application/json"},
                body=json.dumps({"error": "simulated detail failure"}),
            )
        external_id = url.rstrip("/").split("/")[-1]
        body = json.dumps(
            {
                "id": external_id,
                "title": f"重试成功条目 {external_id}",
                "category": "技术",
                "tags": ["重试"],
                "published_at": "2024-03-01 09:30:00",
                "content": "<p>临时故障恢复后抓到的正文。</p>",
            },
            ensure_ascii=False,
        )
        return FetchResult(
            url=url,
            status_code=200,
            headers={"content-type": "application/json"},
            body=body,
        )

    def close(self) -> None:
        self._delegate.close()


class BrokenDetailFetcher:
    """列表页走真实 mock；详情始终返回 200 但正文缺少 h1.title（解析失败）。"""

    def __init__(self, base_url: str) -> None:
        self._delegate = Fetcher(timeout=5.0)
        self.detail_calls = 0

    def get(self, url: str, params: dict[str, object] | None = None) -> FetchResult:
        if "/item/" not in url:
            return self._delegate.get(url, params=params)
        self.detail_calls += 1
        return FetchResult(
            url=url,
            status_code=200,
            headers={"content-type": "text/html; charset=utf-8"},
            body="<html><body><div class='content'>没有标题的正文</div></body></html>",
        )

    def close(self) -> None:
        self._delegate.close()


def _no_sleep(_: float) -> None:
    return None


def test_default_backoff_delays_are_1_2_4_seconds() -> None:
    config = RetryConfig()
    assert config.max_retries == 3
    assert [backoff_delay(config, failed) for failed in (1, 2, 3)] == [1.0, 2.0, 4.0]


def test_persistent_failure_retries_four_times_and_enters_dead_letters(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    flaky = make_source(
        "demo_api_flaky",
        detail_path="/api/items/{id}?fail=1",
        retry={"max_retries": 3, "base_delay_seconds": 1, "backoff_factor": 2},
    )
    delays: list[float] = []

    summary = run_source(
        flaky,
        db,
        raw_store,
        limit=2,
        sleep=delays.append,
    )

    assert (summary.ok, summary.failed) == (0, 2)
    assert delays == [1.0, 2.0, 4.0, 1.0, 2.0, 4.0]
    assert all(outcome.attempts == 4 for outcome in summary.results)
    assert all(outcome.status == "failed" for outcome in summary.results)

    letters = db.list_dead_letters("demo_api_flaky")
    assert sorted(letter.external_id for letter in letters) == ["api-1", "api-2"]
    for letter in letters:
        assert letter.attempts == 4
        assert "HTTP 500" in letter.error
        assert letter.url.endswith("?fail=1")
        assert letter.failed_at is not None

    # 死信表字段与约束：同 source/external_id 再次失败只 upsert 不新增行。
    run_source(flaky, db, raw_store, limit=2, sleep=_no_sleep)
    letters = db.list_dead_letters("demo_api_flaky")
    assert len(letters) == 2
    assert {letter.attempts for letter in letters} == {4}
    reopened = Database(db.path)
    reopened.init()
    try:
        assert len(reopened.list_dead_letters("demo_api_flaky")) == 2
    finally:
        reopened.close()

    # 既有 raw_records 失败留档语义保持不变。
    assert db.count_raw("demo_api_flaky", "failed") == 2
    assert db.count_records("demo_api_flaky") == 0


def test_successful_retry_skips_dead_letters_and_records_item(
    db: Database,
    raw_store: RawStore,
    mock_base_url: str,
) -> None:
    source = SourceConfig.model_validate(
        {
            "name": "demo_api_recovering",
            "type": "api",
            "base_url": mock_base_url,
            "retry": {"max_retries": 3, "base_delay_seconds": 0.01},
        }
    )
    fetcher = FlakyFetcher(mock_base_url, fail_times=2)
    delays: list[float] = []

    summary = run_source(
        source,
        db,
        raw_store,
        fetcher=fetcher,
        limit=1,
        sleep=delays.append,
    )

    assert (summary.ok, summary.failed) == (1, 0)
    assert summary.results[0].attempts == 3
    assert delays == [pytest.approx(0.01), pytest.approx(0.02)]
    assert db.list_dead_letters("demo_api_recovering") == []
    assert db.count_records("demo_api_recovering") == 1
    assert db.count_raw("demo_api_recovering", "ok") == 1
    assert fetcher.calls  # 详情确实被请求了 3 次


def test_recovered_item_on_rerun_is_removed_from_dead_letters(
    db: Database,
    raw_store: RawStore,
    mock_base_url: str,
) -> None:
    flaky = SourceConfig.model_validate(
        {
            "name": "demo_api_recovering",
            "type": "api",
            "base_url": mock_base_url,
            "retry": {"max_retries": 0},
        }
    )

    # 第一轮：持续 500，条目进死信。
    always_failing = FlakyFetcher(mock_base_url, fail_times=10**6)
    first = run_source(
        flaky, db, raw_store, fetcher=always_failing, limit=1, sleep=_no_sleep
    )
    assert first.failed == 1
    assert len(db.list_dead_letters("demo_api_recovering")) == 1

    # 第二轮：服务恢复，重跑成功后死信被删除，records 幂等写入一行。
    recovered = SourceConfig.model_validate(
        {
            "name": "demo_api_recovering",
            "type": "api",
            "base_url": mock_base_url,
            "retry": {"max_retries": 3, "base_delay_seconds": 0.01},
        }
    )
    healthy = FlakyFetcher(mock_base_url, fail_times=0)
    second = run_source(
        recovered, db, raw_store, fetcher=healthy, limit=1, sleep=_no_sleep
    )
    assert second.ok == 1
    assert db.list_dead_letters("demo_api_recovering") == []
    assert db.count_records("demo_api_recovering") == 1
    # raw_records 仍按 (source, external_id) 幂等覆盖为 ok。
    assert db.count_raw("demo_api_recovering") == 1
    assert db.count_raw("demo_api_recovering", "ok") == 1


def test_dry_run_writes_no_dead_letters(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    flaky = make_source(
        "demo_api_flaky",
        detail_path="/api/items/{id}?fail=1",
        retry={"max_retries": 0},
    )
    summary = run_source(flaky, db, raw_store, limit=2, dry_run=True, sleep=_no_sleep)

    assert summary.failed == 2
    assert db.list_dead_letters() == []
    assert db.count_raw() == 0
    assert db.count_records() == 0


def test_dead_letters_cli_lists_by_source(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [
            source_payload(
                "demo_api_flaky",
                detail_path="/api/items/{id}?fail=1",
                retry={"max_retries": 0},
            )
        ]
    )
    db_path = tmp_path / "cli.db"
    raw_dir = tmp_path / "cli-raw"
    runner = CliRunner()

    run_result = runner.invoke(
        app,
        [
            "run",
            "--source",
            "demo_api_flaky",
            "--limit",
            "2",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
        ],
    )
    assert run_result.exit_code == 0, run_result.output

    list_result = runner.invoke(
        app,
        ["dead-letters", "list", "--source", "demo_api_flaky", "--db", str(db_path)],
    )
    assert list_result.exit_code == 0, list_result.output
    assert "api-1" in list_result.output
    assert "api-2" in list_result.output
    assert "HTTP 500" in list_result.output
    assert "共 2 条死信" in list_result.output

    # 按源过滤：没有死信的源给出空提示，退出码仍为 0。
    empty_result = runner.invoke(
        app,
        ["dead-letters", "list", "--source", "demo_api", "--db", str(db_path)],
    )
    assert empty_result.exit_code == 0, empty_result.output
    assert "没有死信条目" in empty_result.output


def test_parse_failure_is_retried_and_ends_in_dead_letters(
    db: Database,
    raw_store: RawStore,
    mock_base_url: str,
) -> None:
    source = SourceConfig.model_validate(
        {
            "name": "demo_html_broken",
            "type": "html",
            "base_url": mock_base_url,
            "retry": {"max_retries": 2, "base_delay_seconds": 0.01},
        }
    )
    fetcher = BrokenDetailFetcher(mock_base_url)

    summary = run_source(
        source,
        db,
        raw_store,
        fetcher=fetcher,
        limit=1,
        sleep=_no_sleep,
    )

    # 解析失败与传输/状态码失败走同一条重试路径：共 3 次尝试。
    assert summary.failed == 1
    assert summary.results[0].attempts == 3
    assert fetcher.detail_calls == 3
    letters = db.list_dead_letters("demo_html_broken")
    assert len(letters) == 1
    assert "h1.title" in letters[0].error
    assert letters[0].attempts == 3
    assert db.count_records("demo_html_broken") == 0


def test_retry_helper_retries_until_success() -> None:
    config = RetryConfig(max_retries=3, base_delay_seconds=0.01)
    calls = {"n": 0}

    def flaky_attempt() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise DetailAttemptError("HTTP 500")
        return "ok"

    value, exhausted, attempts = run_with_retries(
        flaky_attempt,
        config,
        logger=logging.getLogger("test"),
        source="demo_api",
        external_id="api-1",
        sleep=_no_sleep,
    )

    assert value == "ok"
    assert exhausted is None
    assert attempts == 3
    assert calls["n"] == 3
