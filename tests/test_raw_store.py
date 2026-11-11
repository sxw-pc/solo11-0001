"""RawStore 落盘：目录创建、原子覆盖、故障清理、路径安全与管道联动。"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from crawl_flow.config import SourceConfig
from crawl_flow.errors import StoreError
from crawl_flow.fetch.types import FetchResult
from crawl_flow.models import RawRecordRow
from crawl_flow.pipeline.run import process_item
from crawl_flow.sources import ListItem, SourceAdapter
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import MAX_ID_LENGTH, RawStore, safe_external_id

FETCHED_AT = datetime(2026, 11, 11, 8, 30, tzinfo=timezone.utc)
DATE_DIR = "2026-11-11"


def _payload(**extra: object) -> dict[str, Any]:
    payload: dict[str, Any] = {"source": "demo_api", "external_id": "api-1", "body": "{}"}
    payload.update(extra)
    return payload


def _save(store: RawStore, external_id: str = "api-1", **extra: object) -> Path:
    return Path(
        store.save(
            source="demo_api",
            external_id=external_id,
            payload=_payload(**extra),
            fetched_at=FETCHED_AT,
        )
    )


def test_save_creates_missing_directories(tmp_path: Path) -> None:
    """raw 根目录不存在时逐级创建，而不是直接报错。"""

    store = RawStore(tmp_path / "deep" / "nested" / "raw")
    path = _save(store)

    assert path.is_file()
    assert path.parent == store.root / "demo_api" / DATE_DIR
    assert json.loads(path.read_text(encoding="utf-8"))["external_id"] == "api-1"


def test_overwrite_same_file_is_atomic_and_complete(tmp_path: Path) -> None:
    """重复采集覆盖同名文件：内容完整更新，目录无临时文件残留。"""

    store = RawStore(tmp_path / "raw")
    first = _save(store, body="v1")
    second = _save(store, body="v2")

    assert first == second
    assert json.loads(second.read_text(encoding="utf-8"))["body"] == "v2"
    assert list(store.root.rglob("*.json")) == [second]
    assert list(store.root.rglob("*.tmp")) == []


def test_disk_full_mid_write_raises_and_removes_partial_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """写到一半磁盘满：抛 StoreError，半写入的临时文件被清理。"""

    store = RawStore(tmp_path / "raw")
    original_write_text = Path.write_text

    def disk_full_write_text(self: Path, data: str, *args: Any, **kwargs: Any) -> int:
        # 先落一半内容再报磁盘满，模拟写了一半的临时文件
        original_write_text(self, data[: len(data) // 2], *args, **kwargs)
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(Path, "write_text", disk_full_write_text)
    with pytest.raises(StoreError, match="原始响应写入失败"):
        _save(store)

    assert list(store.root.rglob("*.tmp")) == []
    assert list(store.root.rglob("*.json")) == []


def test_disk_full_does_not_corrupt_existing_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """覆盖写入中途磁盘满：已有快照保持完整可读，不会半读取。"""

    store = RawStore(tmp_path / "raw")
    good = _save(store, body="good")
    original_write_text = Path.write_text

    def disk_full_write_text(self: Path, data: str, *args: Any, **kwargs: Any) -> int:
        original_write_text(self, data[:10], *args, **kwargs)
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(Path, "write_text", disk_full_write_text)
    with pytest.raises(StoreError):
        _save(store, body="broken")

    assert json.loads(good.read_text(encoding="utf-8"))["body"] == "good"
    assert list(store.root.rglob("*.tmp")) == []


def test_permission_denied_raises_and_leaves_no_residue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """目录权限不足：抛 StoreError，不残留任何临时文件。"""

    store = RawStore(tmp_path / "raw")

    def denied_mkdir(self: Path, *args: Any, **kwargs: Any) -> None:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(Path, "mkdir", denied_mkdir)
    with pytest.raises(StoreError, match="原始响应写入失败"):
        _save(store)

    assert not store.root.exists()
    assert list(tmp_path.rglob("*.tmp")) == []


def test_replace_failure_cleans_temp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """原子替换失败：临时文件同样被清理。"""

    store = RawStore(tmp_path / "raw")

    def denied_replace(self: Path, *args: Any, **kwargs: Any) -> Path:
        raise PermissionError(13, "Permission denied")

    monkeypatch.setattr(Path, "replace", denied_replace)
    with pytest.raises(StoreError, match="原始响应写入失败"):
        _save(store)

    assert list(store.root.rglob("*.tmp")) == []
    assert list(store.root.rglob("*.json")) == []


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("api-1", "api-1"),
        ("report 2024/01/15", "report_2024_01_15"),
        ("../../etc/passwd", "etc_passwd"),
        ("..\\..\\win", "win"),
        ("a/b/c", "a_b_c"),
        (".well-known", "well-known"),
        ("数据-001", "-001"),
        ("..", "unknown"),
        (".", "unknown"),
        ("...", "unknown"),
        ("___", "unknown"),
        ("", "unknown"),
        ("a" * 500, "a" * MAX_ID_LENGTH),
    ],
)
def test_safe_external_id_values(raw: str, expected: str) -> None:
    assert safe_external_id(raw) == expected


def test_safe_external_id_never_returns_path_segments() -> None:
    """任何输入都不会产生路径分隔符、空字节或 . / .. 片段。"""

    samples = [
        "..",
        ".",
        "../..",
        "//",
        "\\\\",
        "/etc/passwd",
        "a/b",
        "a\\b",
        "\x00",
        ".." * 100,
        "🙂",
        "数据",
    ]
    for sample in samples:
        candidate = safe_external_id(sample)
        assert candidate not in {"", ".", ".."}
        assert "/" not in candidate
        assert "\\" not in candidate
        assert "\x00" not in candidate
        assert len(candidate) <= MAX_ID_LENGTH


@pytest.mark.parametrize(
    "external_id",
    [
        "../../etc/passwd",
        "..\\..\\windows\\system32",
        "/etc/passwd",
        "....//....//etc",
        "..",
        ".",
        "a/b/c",
        "\x00hidden",
        "../.." * 30,
        "x" * 500,
    ],
)
def test_external_id_cannot_escape_raw_root(tmp_path: Path, external_id: str) -> None:
    """带特殊字符的 external_id 落盘后被严格限制在 raw 根目录内。"""

    store = RawStore(tmp_path / "raw")
    path = _save(store, external_id=external_id)

    assert path.is_file()
    assert path.parent == store.root / "demo_api" / DATE_DIR
    assert path.resolve().is_relative_to(store.root.resolve())
    assert path.stem not in {".", ".."}
    assert re.fullmatch(r"[A-Za-z0-9._-]+\.json", path.name) is not None
    assert len(path.stem) <= MAX_ID_LENGTH


class _StubAdapter(SourceAdapter):
    """最小适配器替身：返回固定详情，用于把特殊 external_id 送进管道。"""

    def __init__(self, source: SourceConfig, payload: dict[str, Any]) -> None:
        super().__init__(source, fetcher=None)  # type: ignore[arg-type]
        self._payload = payload

    def list_url(self, page: int) -> str:
        return ""

    def parse_list(self, result: FetchResult) -> list[ListItem]:
        return []

    def parse_detail(self, result: FetchResult, external_id: str) -> dict[str, Any]:
        return self._payload

    def detail_url(self, external_id: str) -> str:
        return f"http://stub.local/items/{external_id}"

    def fetch_detail(self, external_id: str) -> FetchResult:
        return FetchResult(
            url=self.detail_url(external_id),
            status_code=200,
            headers={},
            body=json.dumps(self._payload, ensure_ascii=False),
            elapsed_ms=1,
        )


def test_special_char_external_id_stays_inside_root_through_pipeline(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    """管道层面：特殊 external_id 正常入库，raw 文件不越界。"""

    source = make_source("demo_api")
    external_id = "../../etc/passwd"
    adapter = _StubAdapter(
        source,
        {"id": external_id, "title": "路径安全测试", "content": "正文"},
    )
    item = ListItem(external_id=external_id, url="http://stub.local/items/x")

    outcome = process_item(
        adapter,
        item,
        db=db,
        raw_store=raw_store,
        dry_run=False,
        logger=logging.getLogger("test.raw_store"),
    )

    assert outcome.status == "ok"
    with Session(db.engine) as session:
        row = session.execute(select(RawRecordRow)).scalar_one()
    assert row.external_id == external_id  # 数据库保留原始 id
    assert row.status == "ok"
    assert row.raw_path is not None

    path = Path(row.raw_path)
    assert path.is_file()
    assert path.name == "etc_passwd.json"
    assert path.resolve().is_relative_to(raw_store.root.resolve())
