"""游标规则单元测试：completed_ids / list_position 的推进与跳过语义。"""

from __future__ import annotations

import pytest

from crawl_flow.cursor.rules import (
    CompletedIdsRule,
    ItemCursorContext,
    ListPositionRule,
    build_rule,
)
from crawl_flow.errors import CursorError


def _ctx(external_id: str, position: int) -> ItemCursorContext:
    return ItemCursorContext(external_id=external_id, position=position)


def test_completed_ids_initial_state_marks_nothing() -> None:
    rule = build_rule("completed_ids")
    assert rule.completed_count() == 0
    assert rule.is_completed(_ctx("a", 0)) is False


def test_completed_ids_round_trip() -> None:
    rule = build_rule("completed_ids")
    rule.mark_completed(_ctx("api-1", 0))
    rule.mark_completed(_ctx("api-2", 1))

    payload = rule.state.to_payload()
    assert payload == {"completed_ids": ["api-1", "api-2"]}

    restored = build_rule("completed_ids", payload)
    assert restored.completed_count() == 2
    assert restored.is_completed(_ctx("api-1", 0)) is True
    assert restored.is_completed(_ctx("api-3", 2)) is False


def test_completed_ids_skip_depends_on_id_not_position() -> None:
    # 列表增删导致位置漂移时，已完成 ID 仍被识别，位置规则则不同。
    rule = build_rule("completed_ids")
    rule.mark_completed(_ctx("api-2", 1))
    assert rule.is_completed(_ctx("api-2", 7)) is True
    assert rule.is_completed(_ctx("api-9", 1)) is False


def test_list_position_initial_state_marks_nothing() -> None:
    rule = build_rule("list_position")
    assert rule.completed_count() == 0
    assert rule.is_completed(_ctx("a", 0)) is False


def test_list_position_skips_by_offset() -> None:
    rule = build_rule("list_position")
    rule.mark_completed(_ctx("api-1", 0))
    rule.mark_completed(_ctx("api-2", 1))
    assert rule.completed_count() == 2

    payload = rule.state.to_payload()
    restored = build_rule("list_position", payload)
    assert restored.is_completed(_ctx("api-1", 0)) is True
    assert restored.is_completed(_ctx("api-2", 1)) is True
    assert restored.is_completed(_ctx("api-3", 2)) is False


def test_list_position_takes_max_on_out_of_order() -> None:
    rule = build_rule("list_position")
    rule.mark_completed(_ctx("api-3", 2))
    rule.mark_completed(_ctx("api-1", 0))
    assert rule.completed_count() == 3


def test_unknown_rule_raises() -> None:
    with pytest.raises(CursorError):
        build_rule("no_such_rule")


def test_completed_ids_rejects_non_string_list() -> None:
    with pytest.raises(CursorError):
        build_rule("completed_ids", {"completed_ids": ["a", 1]})
    with pytest.raises(CursorError):
        build_rule("completed_ids", {"completed_ids": "nope"})


def test_list_position_rejects_negative_or_non_int() -> None:
    with pytest.raises(CursorError):
        build_rule("list_position", {"completed_count": -1})
    with pytest.raises(CursorError):
        build_rule("list_position", {"completed_count": True})
    with pytest.raises(CursorError):
        build_rule("list_position", {"completed_count": "3"})
