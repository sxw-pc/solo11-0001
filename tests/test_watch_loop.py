"""--watch 采集循环：轮次间热重载、失败保留与资源复用。"""

from __future__ import annotations

import logging
from typing import Any

import pytest

from crawl_flow.config_watch import ConfigWatcher
from crawl_flow.fetch.client import Fetcher
from crawl_flow.pipeline.watch import run_watch_loop
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def test_watch_loop_picks_up_new_and_modified_enabled_sources(
    write_config: Any,
    source_payload: Any,
    db: Database,
    raw_store: RawStore,
    fetcher: Fetcher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = write_config([source_payload("demo_api", page_size=5)])
    watcher = ConfigWatcher(config_path)
    watcher.load()
    rounds: list[list[str]] = []

    def fake_sleep(_interval: float) -> None:
        # 第一轮结束后新增一个 enabled HTML 源，同时修改 API 源的 page_size。
        write_config(
            [
                source_payload("demo_api", page_size=10),
                source_payload("demo_html", "html"),
            ]
        )

    monkeypatch.setattr("crawl_flow.pipeline.watch.Fetcher", _forbid_new_fetcher)
    monkeypatch.setattr("crawl_flow.pipeline.watch.Database", _forbid_new_database)

    summaries = run_watch_loop(
        watcher,
        db,
        raw_store,
        fetcher,
        name=None,
        all_sources=True,
        limit=1,
        max_rounds=2,
        sleep=fake_sleep,
        on_round=lambda _round, items: rounds.append([item.source for item in items]),
    )

    assert rounds == [["demo_api"], ["demo_api", "demo_html"]]
    assert [summary.source for summary in summaries] == [
        "demo_api",
        "demo_api",
        "demo_html",
    ]
    # 两个源各处理 1 条；重复 demo_api 仍是幂等更新。
    assert db.count_records("demo_api") == 1
    assert db.count_records("demo_html") == 1
    reloaded = {source.name: source for source in watcher.sources}
    assert reloaded["demo_api"].page_size == 10


def test_watch_loop_keeps_previous_config_when_reload_fails(
    write_config: Any,
    source_payload: Any,
    db: Database,
    raw_store: RawStore,
    fetcher: Fetcher,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    watcher = ConfigWatcher(config_path)
    watcher.load()

    def fake_sleep(_interval: float) -> None:
        config_path.write_text("sources: [\n", encoding="utf-8")

    with caplog.at_level(logging.WARNING):
        summaries = run_watch_loop(
            watcher,
            db,
            raw_store,
            fetcher,
            name=None,
            all_sources=True,
            limit=1,
            max_rounds=2,
            sleep=fake_sleep,
        )

    assert [summary.source for summary in summaries] == ["demo_api", "demo_api"]
    assert [source.name for source in watcher.sources] == ["demo_api"]
    assert "config reload failed" in caplog.text
    assert "keep_previous=true" in caplog.text


def test_watch_loop_explicit_source_ignores_newly_added_sources(
    write_config: Any,
    source_payload: Any,
    db: Database,
    raw_store: RawStore,
    fetcher: Fetcher,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    watcher = ConfigWatcher(config_path)
    watcher.load()

    def fake_sleep(_interval: float) -> None:
        write_config(
            [source_payload("demo_api"), source_payload("demo_html", "html")]
        )

    summaries = run_watch_loop(
        watcher,
        db,
        raw_store,
        fetcher,
        name="demo_api",
        all_sources=False,
        limit=1,
        max_rounds=2,
        sleep=fake_sleep,
    )
    assert [summary.source for summary in summaries] == ["demo_api", "demo_api"]


def test_watch_loop_with_one_round_does_not_sleep_or_recreate_resources(
    write_config: Any,
    source_payload: Any,
    db: Database,
    raw_store: RawStore,
    fetcher: Fetcher,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    watcher = ConfigWatcher(config_path)
    watcher.load()

    def fail_sleep(_interval: float) -> None:
        raise AssertionError("max_rounds=1 不应进入等待")

    monkeypatch.setattr("crawl_flow.pipeline.watch.Fetcher", _forbid_new_fetcher)
    monkeypatch.setattr("crawl_flow.pipeline.watch.Database", _forbid_new_database)

    summaries = run_watch_loop(
        watcher,
        db,
        raw_store,
        fetcher,
        name="demo_api",
        all_sources=False,
        limit=1,
        max_rounds=1,
        sleep=fail_sleep,
    )
    assert len(summaries) == 1
    # 传入的同一个 Fetcher 仍未被 run_source 关闭，可继续使用。
    response = fetcher.get(f"{source_payload('demo_api')['base_url']}/api/items")
    assert response.status_code == 200


def test_watch_loop_keeps_last_selection_when_all_sources_become_disabled(
    write_config: Any,
    source_payload: Any,
    db: Database,
    raw_store: RawStore,
    fetcher: Fetcher,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    watcher = ConfigWatcher(config_path)
    watcher.load()

    def fake_sleep(_interval: float) -> None:
        write_config([source_payload("demo_api", enabled=False)])

    with caplog.at_level(logging.WARNING):
        summaries = run_watch_loop(
            watcher,
            db,
            raw_store,
            fetcher,
            name=None,
            all_sources=True,
            limit=1,
            max_rounds=2,
            sleep=fake_sleep,
        )

    assert [summary.source for summary in summaries] == ["demo_api", "demo_api"]
    assert "source selector" in caplog.text
    assert "keep_previous=true" in caplog.text


def _forbid_new_fetcher(*_args: Any, **_kwargs: Any) -> Fetcher:
    raise AssertionError("watch 循环必须复用现有 HTTP 客户端，禁止新建 Fetcher")


def _forbid_new_database(*_args: Any, **_kwargs: Any) -> Database:
    raise AssertionError("watch 循环必须复用现有数据库连接，禁止新建 Database")
