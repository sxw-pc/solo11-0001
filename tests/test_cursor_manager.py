"""CursorManager 测试：首次初始化、损坏降级、规则变更、dry-run 旁路、重置。"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

from crawl_flow.cursor.manager import CursorManager
from crawl_flow.cursor.store import CursorStore


def _manager(
    store: CursorStore,
    source: str = "demo_api",
    rule: str = "completed_ids",
    *,
    dry_run: bool = False,
) -> CursorManager:
    return CursorManager(
        source_name=source,
        rule_name=rule,
        store=store,
        logger=logging.getLogger("test.cursor"),
        dry_run=dry_run,
    )


def test_first_run_initializes_without_writing_file(cursor_store: CursorStore) -> None:
    manager = _manager(cursor_store)
    manager.start()
    assert manager.active is True
    assert manager.degraded is False
    assert manager.completed_count == 0
    # 首次初始化只在内存建立游标；没有条目完成时不落文件（空列表不推进）。
    assert cursor_store.exists("demo_api") is False


def test_second_run_loads_completed_and_skips(cursor_store: CursorStore) -> None:
    first = _manager(cursor_store)
    first.start()
    first.mark_processed("api-1", 0)
    first.mark_processed("api-2", 1)
    assert cursor_store.exists("demo_api")

    second = _manager(cursor_store)
    second.start()
    assert second.active is True
    assert second.completed_count == 2
    assert second.should_skip("api-1", 0) is True
    assert second.should_skip("api-2", 1) is True
    assert second.should_skip("api-3", 2) is False


def test_corrupt_file_degrades_to_full_crawl(
    cursor_store: CursorStore, caplog: pytest.LogCaptureFixture
) -> None:
    cursor_store.save("demo_api", "completed_ids", {"completed_ids": ["api-1"]})
    cursor_store.path_for("demo_api").write_text("{not-json", encoding="utf-8")

    manager = _manager(cursor_store)
    with caplog.at_level(logging.WARNING):
        manager.start()

    assert manager.active is False
    assert manager.degraded is True
    # 降级后任何条目都不跳过，等价全量采集，且不抛异常。
    assert manager.should_skip("api-1", 0) is False
    assert any("cursor degraded" in record.getMessage() for record in caplog.records)


def test_corrupt_state_payload_degrades(cursor_store: CursorStore) -> None:
    cursor_store.path_for("demo_api").parent.mkdir(parents=True, exist_ok=True)
    cursor_store.path_for("demo_api").write_text(
        '{"version": 1, "source": "demo_api", "rule": "completed_ids", "state": {"completed_ids": [1, 2]}}',
        encoding="utf-8",
    )
    manager = _manager(cursor_store)
    manager.start()
    assert manager.degraded is True
    assert manager.active is False


def test_unsupported_version_degrades(cursor_store: CursorStore) -> None:
    cursor_store.path_for("demo_api").parent.mkdir(parents=True, exist_ok=True)
    cursor_store.path_for("demo_api").write_text(
        '{"version": 99, "source": "demo_api", "rule": "completed_ids", '
        '"state": {"completed_ids": []}}',
        encoding="utf-8",
    )
    manager = _manager(cursor_store)
    manager.start()
    assert manager.degraded is True


def test_rule_change_reinitializes_without_degraded_flag(
    cursor_store: CursorStore, caplog: pytest.LogCaptureFixture
) -> None:
    first = _manager(cursor_store, rule="completed_ids")
    first.start()
    first.mark_processed("api-1", 0)

    changed = _manager(cursor_store, rule="list_position")
    with caplog.at_level(logging.WARNING):
        changed.start()

    # 规则变更不是损坏：重新从空游标开始（本轮全量），但不打 degraded 标记。
    assert changed.degraded is False
    assert changed.active is True
    assert changed.completed_count == 0
    assert any("cursor rule changed" in record.getMessage() for record in caplog.records)


def test_dry_run_bypasses_cursor_completely(cursor_store: CursorStore) -> None:
    manager = _manager(cursor_store, dry_run=True)
    manager.start()
    assert manager.active is False
    manager.mark_processed("api-1", 0)
    assert cursor_store.exists("demo_api") is False
    assert manager.should_skip("api-1", 0) is False


def test_mark_processed_persists_each_item(cursor_store: CursorStore) -> None:
    manager = _manager(cursor_store)
    manager.start()
    manager.mark_processed("api-1", 0)
    payload = cursor_store.load("demo_api")
    assert payload["state"] == {"completed_ids": ["api-1"]}
    manager.mark_processed("api-2", 1)
    payload = cursor_store.load("demo_api")
    assert payload["state"] == {"completed_ids": ["api-1", "api-2"]}


def test_reset_clears_cursor_and_allows_full_crawl(cursor_store: CursorStore) -> None:
    manager = _manager(cursor_store)
    manager.start()
    manager.mark_processed("api-1", 0)
    assert manager.reset() is True

    restarted = _manager(cursor_store)
    restarted.start()
    assert restarted.completed_count == 0
    assert restarted.should_skip("api-1", 0) is False


def test_one_source_corruption_does_not_affect_others(
    cursor_store: CursorStore,
) -> None:
    good = _manager(cursor_store, source="demo_html")
    good.start()
    good.mark_processed("html-1", 0)

    # demo_api 的游标文件损坏。
    cursor_store.path_for("demo_api").parent.mkdir(parents=True, exist_ok=True)
    cursor_store.path_for("demo_api").write_text("broken", encoding="utf-8")
    bad = _manager(cursor_store, source="demo_api")
    bad.start()

    assert bad.degraded is True and bad.active is False
    # demo_html 完全不受影响。
    check = _manager(cursor_store, source="demo_html")
    check.start()
    assert check.degraded is False
    assert check.should_skip("html-1", 0) is True


def test_list_position_persists_and_skips(cursor_store: CursorStore) -> None:
    first = _manager(cursor_store, rule="list_position")
    first.start()
    first.mark_processed("api-1", 0)
    first.mark_processed("api-2", 1)

    second = _manager(cursor_store, rule="list_position")
    second.start()
    assert second.should_skip("api-1", 0) is True
    assert second.should_skip("api-2", 1) is True
    assert second.should_skip("api-3", 2) is False
    assert second.completed_count == 2


def test_empty_list_does_not_advance_or_create_file(
    cursor_store: CursorStore, tmp_path: Path
) -> None:
    manager = _manager(cursor_store)
    manager.start()
    # 没有任何 mark_processed 调用（对应空列表），游标文件不应出现。
    assert cursor_store.list_sources() == []
    assert manager.completed_count == 0
