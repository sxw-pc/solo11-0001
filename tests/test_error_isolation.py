"""错误隔离：详情 500 不中断整轮，raw_records 留 failed 行，CLI 退出码 0。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.errors import StoreError
from crawl_flow.models import RawRecordRow
from crawl_flow.pipeline.run import MAX_ERROR_LENGTH, run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def test_detail_500_does_not_abort_round(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    flaky = make_source("demo_api_flaky", detail_path="/api/items/{id}?fail=1")
    summary = run_source(flaky, db, raw_store, limit=3)

    assert (summary.ok, summary.failed, summary.skipped) == (0, 3, 12)
    assert db.count_records() == 0
    assert db.count_raw("demo_api_flaky", "failed") == 3

    with Session(db.engine) as session:
        rows = list(
            session.execute(
                select(RawRecordRow).order_by(RawRecordRow.external_id)
            ).scalars()
        )

    assert [row.external_id for row in rows] == ["api-1", "api-2", "api-3"]
    for row in rows:
        assert row.status == "failed"
        assert row.http_status == 500
        assert row.error is not None and "HTTP 500" in row.error
        assert row.raw_path is not None
        payload = json.loads(Path(row.raw_path).read_text(encoding="utf-8"))
        assert payload["status"] == 500
        assert payload["error"] == row.error


def test_cli_returns_zero_when_details_fail(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [source_payload("demo_api_flaky", detail_path="/api/items/{id}?fail=1")]
    )
    db_path = tmp_path / "cli.db"
    raw_dir = tmp_path / "cli-raw"

    result = CliRunner().invoke(
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

    assert result.exit_code == 0, result.output
    assert "failed=2" in result.output

    database = Database(db_path)
    database.init()
    try:
        assert database.count_raw("demo_api_flaky", "failed") == 2
        assert database.count_records() == 0
    finally:
        database.close()


def test_raw_save_failure_marks_single_item_failed(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """单条 raw 落盘失败只隔离该条，其余条目照常落盘入库。"""

    source_config = make_source("demo_api")
    original_save = RawStore.save

    def flaky_save(self: RawStore, *, source: str, external_id: str, **kwargs: Any) -> str:
        if external_id == "api-2":
            raise StoreError(
                "原始响应写入失败 path=/raw/api-2.json error=No space left on device"
            )
        return original_save(self, source=source, external_id=external_id, **kwargs)

    monkeypatch.setattr(RawStore, "save", flaky_save)
    summary = run_source(source_config, db, raw_store, limit=3)

    assert (summary.ok, summary.failed) == (2, 1)
    assert db.count_records("demo_api") == 2
    assert db.count_raw("demo_api", "ok") == 2
    assert db.count_raw("demo_api", "failed") == 1

    with Session(db.engine) as session:
        rows = {row.external_id: row for row in session.execute(select(RawRecordRow)).scalars()}

    failed = rows["api-2"]
    assert failed.status == "failed"
    assert failed.error is not None and "原始响应写入失败" in failed.error
    assert failed.raw_path is None
    for external_id in ("api-1", "api-3"):
        row = rows[external_id]
        assert row.status == "ok"
        assert row.raw_path is not None
        assert Path(row.raw_path).is_file()
    assert list(raw_store.root.rglob("*.tmp")) == []


def test_raw_store_outage_does_not_abort_round(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """raw 目录整体不可写（权限不足/磁盘满）时整轮仍跑完，全部记 failed。"""

    def always_fail(self: RawStore, **kwargs: Any) -> str:
        raise StoreError("原始响应写入失败 path=/raw/x.json error=Permission denied")

    monkeypatch.setattr(RawStore, "save", always_fail)
    summary = run_source(make_source("demo_api"), db, raw_store, limit=3)

    assert (summary.ok, summary.failed) == (0, 3)
    assert db.count_records() == 0
    assert db.count_raw("demo_api", "failed") == 3

    with Session(db.engine) as session:
        rows = list(session.execute(select(RawRecordRow)).scalars())
    for row in rows:
        assert row.status == "failed"
        assert row.raw_path is None
        assert row.error is not None and "Permission denied" in row.error


def test_raw_save_error_is_truncated_in_raw_records(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """落盘失败写进 raw_records 的错误信息截断到 500 字符。"""

    def always_fail(self: RawStore, **kwargs: Any) -> str:
        raise StoreError("原始响应写入失败 " + "磁盘满 " * 400)

    monkeypatch.setattr(RawStore, "save", always_fail)
    summary = run_source(make_source("demo_api"), db, raw_store, limit=1)

    assert summary.failed == 1
    with Session(db.engine) as session:
        row = session.execute(select(RawRecordRow)).scalar_one()
    assert row.status == "failed"
    assert row.error is not None
    assert len(row.error) == MAX_ERROR_LENGTH


def test_failed_item_snapshot_save_failure_still_records(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """详情 500 且故障快照也写不进去：raw_records 仍留 failed 行，raw_path 为空。"""

    def always_fail(self: RawStore, **kwargs: Any) -> str:
        raise StoreError("原始响应写入失败 path=/raw/x.json error=No space left on device")

    monkeypatch.setattr(RawStore, "save", always_fail)
    flaky = make_source("demo_api_flaky", detail_path="/api/items/{id}?fail=1")
    summary = run_source(flaky, db, raw_store, limit=2)

    assert (summary.ok, summary.failed) == (0, 2)
    assert db.count_raw("demo_api_flaky", "failed") == 2
    with Session(db.engine) as session:
        rows = list(session.execute(select(RawRecordRow)).scalars())
    for row in rows:
        assert row.status == "failed"
        assert row.http_status == 500
        assert row.error is not None and "HTTP 500" in row.error
        assert row.raw_path is None


def test_other_source_unaffected_when_raw_store_fails(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """一个源落盘全部失败时，其他源的记录照常入库。"""

    original_save = RawStore.save

    def flaky_save(self: RawStore, *, source: str, **kwargs: Any) -> str:
        if source == "demo_api":
            raise StoreError("原始响应写入失败 path=/raw/x.json error=Permission denied")
        return original_save(self, source=source, **kwargs)

    monkeypatch.setattr(RawStore, "save", flaky_save)
    api_summary = run_source(make_source("demo_api"), db, raw_store, limit=2)
    html_summary = run_source(make_source("demo_html", "html"), db, raw_store, limit=2)

    assert (api_summary.ok, api_summary.failed) == (0, 2)
    assert (html_summary.ok, html_summary.failed) == (2, 0)
    assert db.count_records("demo_api") == 0
    assert db.count_records("demo_html") == 2
    assert db.count_raw("demo_api", "failed") == 2
    assert db.count_raw("demo_html", "ok") == 2


def test_cli_returns_zero_when_raw_store_fails(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """raw 落盘全部失败时 CLI 仍正常退出（退出码 0）并打印 failed 汇总。"""

    def always_fail(self: RawStore, **kwargs: Any) -> str:
        raise StoreError("原始响应写入失败 path=/raw/x.json error=No space left on device")

    monkeypatch.setattr(RawStore, "save", always_fail)
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "cli_raw_fail.db"

    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "2",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(tmp_path / "cli-raw"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "failed=2" in result.output

    database = Database(db_path)
    database.init()
    try:
        assert database.count_raw("demo_api", "failed") == 2
        assert database.count_records() == 0
    finally:
        database.close()
