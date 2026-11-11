"""raw 检索能力：Database 元信息过滤 + RawStore 按需加载与异常文件处理。

检索只复用 raw_records 的索引/条件能力，正文关键词才按需读取 raw 文件；
dry-run 不产生任何 raw_records 行，因此天然不会进入检索范围。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from crawl_flow.config import SourceConfig
from crawl_flow.models import RawRecord
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import (
    DEFAULT_SEARCH_LIMIT,
    MAX_SEARCH_LIMIT,
    Database,
    RawSearchFilter,
)
from crawl_flow.storage.raw_store import RawStore


def _insert_raw(
    db: Database,
    raw_store: RawStore,
    *,
    source: str,
    external_id: str,
    status: str = "ok",
    error: str | None = None,
    body: str = "示例正文",
    fetched_at: datetime | None = None,
) -> RawRecord:
    fetched_at = fetched_at or datetime(2026, 1, 1, 12, 0, 0)
    payload = {
        "source": source,
        "external_id": external_id,
        "url": f"https://example.test/{external_id}",
        "status": 200 if status == "ok" else 500,
        "fetched_at": fetched_at.isoformat(),
        "headers": {},
        "body": body,
    }
    if error:
        payload["error"] = error
    raw_path = raw_store.save(
        source=source,
        external_id=external_id,
        payload=payload,
        fetched_at=fetched_at,
    )
    record = RawRecord(
        source=source,
        external_id=external_id,
        url=payload["url"],
        http_status=payload["status"],
        fetched_at=fetched_at,
        status=status,
        error=error,
        raw_path=raw_path,
    )
    db.upsert_raw(record)
    return record


def test_search_default_returns_indexed_rows_with_limit_100(
    db: Database, raw_store: RawStore
) -> None:
    for index in range(105):
        _insert_raw(
            db,
            raw_store,
            source="demo_api",
            external_id=f"api-{index:04d}",
            fetched_at=datetime(2026, 1, 1, 12, 0, 0) + timedelta(seconds=index),
        )

    page = db.search_raw()
    assert page.limit == DEFAULT_SEARCH_LIMIT
    assert page.total == 105
    assert len(page.rows) == DEFAULT_SEARCH_LIMIT
    # 按 fetched_at 倒序，最新一条排第一
    assert page.rows[0].external_id == "api-0104"

    second = db.search_raw(limit=100, offset=100)
    assert len(second.rows) == 5
    assert {row.external_id for row in second.rows} == {
        f"api-{index:04d}" for index in range(5)
    }


def test_search_limit_is_clamped_to_max(db: Database, raw_store: RawStore) -> None:
    _insert_raw(db, raw_store, source="demo_api", external_id="api-1")
    page = db.search_raw(RawSearchFilter(), limit=10**9)
    assert page.limit == MAX_SEARCH_LIMIT


def test_search_filters_by_source_status_and_external_id(
    db: Database, raw_store: RawStore
) -> None:
    _insert_raw(db, raw_store, source="demo_api", external_id="api-1")
    _insert_raw(
        db,
        raw_store,
        source="demo_api",
        external_id="api-2",
        status="failed",
        error="HTTP 500 模拟故障",
    )
    _insert_raw(db, raw_store, source="demo_html", external_id="html-1")

    assert db.search_raw(RawSearchFilter(source="demo_api")).total == 2
    assert db.search_raw(RawSearchFilter(source="demo_html")).total == 1
    failed = db.search_raw(RawSearchFilter(status="failed"))
    assert failed.total == 1
    assert failed.rows[0].external_id == "api-2"
    one = db.search_raw(RawSearchFilter(source="demo_api", external_id="api-1"))
    assert one.total == 1
    assert one.rows[0].raw_path is not None


def test_search_filters_by_error_keyword_without_touching_files(
    db: Database, raw_store: RawStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    _insert_raw(
        db,
        raw_store,
        source="demo_api",
        external_id="api-1",
        status="failed",
        error="HTTP 500 内部错误",
    )
    _insert_raw(
        db,
        raw_store,
        source="demo_api",
        external_id="api-2",
        status="failed",
        error="连接超时 timeout",
    )

    def _forbidden_read(self: Any, path: str | Path) -> Any:
        raise AssertionError("错误关键词检索不应读取 raw 文件")

    monkeypatch.setattr(RawStore, "read_raw", _forbidden_read)
    page = db.search_raw(RawSearchFilter(error_keyword="HTTP 500"))
    assert [row.external_id for row in page.rows] == ["api-1"]
    assert db.search_raw(RawSearchFilter(error_keyword="不存在的错误")).total == 0


def test_search_filters_by_time_range_inclusive(
    db: Database, raw_store: RawStore
) -> None:
    base = datetime(2026, 3, 10, 8, 0, 0)
    for index, day in enumerate((8, 9, 10, 11, 12)):
        _insert_raw(
            db,
            raw_store,
            source="demo_api",
            external_id=f"api-{index}",
            fetched_at=base.replace(day=day),
        )

    page = db.search_raw(
        RawSearchFilter(
            start=datetime(2026, 3, 9),
            end=datetime(2026, 3, 11, 23, 59, 59),
        )
    )
    assert {row.external_id for row in page.rows} == {"api-1", "api-2", "api-3"}

    # 带时区的边界同样可比较（UTC 转换后落在范围内）
    aware = db.search_raw(
        RawSearchFilter(
            start=datetime(2026, 3, 10, 0, 0, tzinfo=timezone.utc),
            end=datetime(2026, 3, 10, 23, 59, tzinfo=timezone.utc),
        )
    )
    assert {row.external_id for row in aware.rows} == {"api-2"}


def test_mock_site_raw_data_is_fully_searchable(
    api_source: SourceConfig,
    html_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
) -> None:
    api_summary = run_source(api_source, db, raw_store, limit=15)
    html_summary = run_source(html_source, db, raw_store, limit=15)
    assert (api_summary.ok, api_summary.failed) == (15, 0)
    assert (html_summary.ok, html_summary.failed) == (15, 0)

    assert db.search_raw(RawSearchFilter(source="demo_api")).total == 15
    assert db.search_raw(RawSearchFilter(source="demo_html")).total == 15
    assert db.search_raw(RawSearchFilter(status="ok")).total == 30

    # 正文按需加载：mock fixtures 的 api-2 正文含「数据库」
    page = db.search_raw(RawSearchFilter(source="demo_api"))
    result = raw_store.filter_by_body(page.rows, "数据库")
    assert [row.external_id for row in result.matched] == ["api-2"]
    assert result.scanned == 15
    assert result.read_errors == []


def test_body_search_loads_only_candidate_files(
    db: Database, raw_store: RawStore
) -> None:
    _insert_raw(
        db, raw_store, source="demo_api", external_id="api-1", body="alpha 内容"
    )
    _insert_raw(
        db, raw_store, source="demo_api", external_id="api-2", body="beta 内容"
    )
    _insert_raw(
        db, raw_store, source="demo_html", external_id="html-1", body="alpha 内容"
    )

    candidates = db.search_raw(RawSearchFilter(source="demo_api")).rows
    result = raw_store.filter_by_body(candidates, "alpha")
    assert [row.external_id for row in result.matched] == ["api-1"]
    assert result.scanned == 2  # 只加载 demo_api 的两个文件，不扫 demo_html


def test_read_raw_handles_corrupt_permission_empty_and_missing(
    db: Database, raw_store: RawStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    record = _insert_raw(
        db, raw_store, source="demo_api", external_id="api-1", body="正文"
    )
    path = Path(record.raw_path)

    # 正常文件
    ok = raw_store.read_raw(path)
    assert ok.ok and ok.body == "正文" and ok.error is None

    # JSON 损坏
    path.write_text("{ broken", encoding="utf-8")
    corrupt = raw_store.read_raw(path)
    assert not corrupt.ok and "JSON 损坏" in (corrupt.error or "")

    # 空文件（空响应落盘场景）
    path.write_text("", encoding="utf-8")
    empty = raw_store.read_raw(path)
    assert not empty.ok and "为空" in (empty.error or "")

    # 顶层不是 JSON 对象
    path.write_text('["not", "object"]', encoding="utf-8")
    not_object = raw_store.read_raw(path)
    assert not not_object.ok and "JSON 对象" in (not_object.error or "")

    # 权限不足：模拟 PermissionError，保证 root 环境下测试也稳定
    def _deny(*args: Any, **kwargs: Any) -> Any:
        raise PermissionError("denied")

    monkeypatch.setattr(Path, "read_text", _deny)
    denied = raw_store.read_raw(path)
    assert not denied.ok and "权限不足" in (denied.error or "")
    monkeypatch.undo()

    # 文件缺失
    path.unlink()
    missing = raw_store.read_raw(path)
    assert not missing.ok and "不存在" in (missing.error or "")

    # 未登记 raw_path
    no_path = raw_store.read_for_record(
        RawRecord(
            source="demo_api",
            external_id="api-x",
            url="u",
            fetched_at=datetime(2026, 1, 1),
            status="ok",
            raw_path=None,
        )
    )
    assert not no_path.ok and "raw_path" in (no_path.error or "")


def test_body_search_skips_bad_files_without_aborting(
    db: Database, raw_store: RawStore
) -> None:
    good = _insert_raw(
        db, raw_store, source="demo_api", external_id="api-1", body="命中关键词"
    )
    bad = _insert_raw(
        db, raw_store, source="demo_api", external_id="api-2", body="其他"
    )
    Path(bad.raw_path).write_text("{损坏", encoding="utf-8")

    rows = db.search_raw(RawSearchFilter(source="demo_api")).rows
    result = raw_store.filter_by_body(rows, "命中关键词")
    assert [row.external_id for row in result.matched] == [good.external_id]
    assert len(result.read_errors) == 1
    assert "JSON 损坏" in (result.read_errors[0].error or "")


def test_empty_body_is_valid_payload_not_an_error(
    db: Database, raw_store: RawStore
) -> None:
    record = _insert_raw(
        db, raw_store, source="demo_api", external_id="api-1", body=""
    )
    content = raw_store.read_raw(record.raw_path)
    assert content.ok
    assert content.body == ""

    result = raw_store.filter_by_body([record], "任意词")
    assert result.matched == []
    assert result.read_errors == []


def test_dry_run_creates_no_searchable_rows(
    api_source: SourceConfig, db: Database, raw_store: RawStore
) -> None:
    summary = run_source(api_source, db, raw_store, limit=5, dry_run=True)
    assert summary.ok == 5
    assert db.search_raw().total == 0
    assert not raw_store.root.exists()
