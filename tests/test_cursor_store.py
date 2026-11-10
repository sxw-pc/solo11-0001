"""CursorStore 测试：原子写入、按源隔离、重置与损坏检测。"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from crawl_flow.cursor.store import CursorStore
from crawl_flow.errors import CursorError


def test_load_missing_returns_none(tmp_path: Path) -> None:
    store = CursorStore(tmp_path / "cursors")
    assert store.load("demo_api") is None
    assert store.exists("demo_api") is False


def test_save_then_load_round_trip(tmp_path: Path) -> None:
    store = CursorStore(tmp_path / "cursors")
    store.save("demo_api", "completed_ids", {"completed_ids": ["a", "b"]})

    assert store.exists("demo_api")
    payload = store.load("demo_api")
    assert payload == {
        "version": 1,
        "source": "demo_api",
        "rule": "completed_ids",
        "state": {"completed_ids": ["a", "b"]},
    }


def test_each_source_has_independent_file(tmp_path: Path) -> None:
    store = CursorStore(tmp_path / "cursors")
    store.save("demo_api", "completed_ids", {"completed_ids": ["a"]})
    store.save("demo_html", "list_position", {"completed_count": 1})

    assert store.path_for("demo_api").name == "demo_api.json"
    assert store.path_for("demo_html").name == "demo_html.json"
    assert sorted(store.list_sources()) == ["demo_api", "demo_html"]
    # 一个源损坏不影响另一个源读取（单源隔离的存储基础）。
    store.path_for("demo_api").write_text("{broken", encoding="utf-8")
    with pytest.raises(CursorError):
        store.load("demo_api")
    assert store.load("demo_html")["rule"] == "list_position"


def test_corrupt_json_raises_cursor_error(tmp_path: Path) -> None:
    store = CursorStore(tmp_path / "cursors")
    store.path_for("demo_api").parent.mkdir(parents=True, exist_ok=True)
    store.path_for("demo_api").write_text('{"state": ', encoding="utf-8")
    with pytest.raises(CursorError):
        store.load("demo_api")


def test_non_object_top_level_raises(tmp_path: Path) -> None:
    store = CursorStore(tmp_path / "cursors")
    store.path_for("demo_api").parent.mkdir(parents=True, exist_ok=True)
    store.path_for("demo_api").write_text("[1, 2, 3]", encoding="utf-8")
    with pytest.raises(CursorError):
        store.load("demo_api")


def test_save_is_atomic_no_temp_file_left(tmp_path: Path) -> None:
    store = CursorStore(tmp_path / "cursors")
    path = store.save("demo_api", "completed_ids", {"completed_ids": []})
    # 原子替换后目录里只剩正式文件，没有 .tmp 残留；内容始终是完整 JSON。
    assert path.is_file()
    assert list(path.parent.glob("*.tmp")) == []
    json.loads(path.read_text(encoding="utf-8"))


def test_save_failure_keeps_previous_file_intact(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    store = CursorStore(tmp_path / "cursors")
    store.save("demo_api", "completed_ids", {"completed_ids": ["keep"]})
    original = store.path_for("demo_api").read_text(encoding="utf-8")

    def _boom(*args: object, **kwargs: object) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("os.replace", _boom)
    with pytest.raises(CursorError):
        store.save("demo_api", "completed_ids", {"completed_ids": ["lost"]})

    # 写入失败时旧游标完好无损，临时文件被留下但不会被当成游标读取。
    assert store.path_for("demo_api").read_text(encoding="utf-8") == original


def test_reset_removes_file_and_reports(tmp_path: Path) -> None:
    store = CursorStore(tmp_path / "cursors")
    assert store.reset("demo_api") is False
    store.save("demo_api", "completed_ids", {"completed_ids": ["a"]})
    assert store.reset("demo_api") is True
    assert store.exists("demo_api") is False
    assert store.reset("demo_api") is False
