"""版本历史：内容变化追加版本、原样重采不追加、旧库迁移与 CLI 查询。"""

from __future__ import annotations

import json
import shutil
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner
from werkzeug.serving import make_server

from crawl_flow.cli import app
from crawl_flow.config import SourceConfig
from crawl_flow.models import Record, RecordRow, RecordVersionRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database, compute_content_hash
from crawl_flow.storage.raw_store import RawStore

REPO_ROOT = Path(__file__).resolve().parents[1]
runner = CliRunner()


@contextmanager
def _serve(fixtures_dir: Path) -> Iterator[str]:
    """按指定 fixtures 目录启动一个临时 mock 站点。"""

    from mock_site.app import create_app

    server = make_server("127.0.0.1", 0, create_app(fixtures_dir))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _api_source(base_url: str) -> SourceConfig:
    return SourceConfig(name="demo_api", type="api", base_url=base_url, page_size=5)


def _versions(db: Database, external_id: str = "api-1") -> list[RecordVersionRow]:
    with Session(db.engine) as session:
        return list(
            session.execute(
                select(RecordVersionRow)
                .where(RecordVersionRow.external_id == external_id)
                .order_by(RecordVersionRow.version)
            ).scalars()
        )


def _copy_api_fixtures(tmp_path: Path) -> Path:
    fixtures = tmp_path / "fixtures"
    shutil.copytree(REPO_ROOT / "mock_site" / "fixtures" / "api", fixtures / "api")
    return fixtures


def test_changed_content_adds_one_version_without_new_record_row(
    tmp_path: Path,
) -> None:
    fixtures = _copy_api_fixtures(tmp_path)
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    try:
        with _serve(fixtures) as base_url:
            first = run_source(_api_source(base_url), db, raw_store, limit=1)
            assert first.changed == 0
            assert db.count_records("demo_api") == 1
            assert db.count_versions("demo_api", "api-1") == 1

            # 原样重采：内容未变，不新增版本。
            second = run_source(_api_source(base_url), db, raw_store, limit=1)
            assert second.changed == 0
            assert db.count_records("demo_api") == 1
            assert db.count_versions("demo_api", "api-1") == 1

        # 修改 mock 正文后重新启动 mock 站点并重采。
        page_path = fixtures / "api" / "page_1.json"
        payload = json.loads(page_path.read_text(encoding="utf-8"))
        payload["items"][0]["content"] = "<p>修改后的正文，用于验证版本历史。</p>"
        page_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        with _serve(fixtures) as base_url:
            third = run_source(_api_source(base_url), db, raw_store, limit=1)

        assert third.changed == 1
        assert third.ok == 1
        assert db.count_records("demo_api") == 1
        assert db.count_versions("demo_api", "api-1") == 2

        with Session(db.engine) as session:
            row = session.execute(
                select(RecordRow).where(RecordRow.external_id == "api-1")
            ).scalar_one()
        assert "修改后的正文" in row.content
        assert row.content_hash == compute_content_hash(row.title, row.content)

        versions = _versions(db)
        assert [item.version for item in versions] == [1, 2]
        assert versions[0].content_hash != versions[1].content_hash
        assert versions[1].content_hash == row.content_hash
    finally:
        db.close()


def test_old_database_is_migrated_to_version_one(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    fetched_at = "2024-03-01 09:30:00"
    with sqlite3.connect(db_path) as connection:
        connection.execute(
            """
            CREATE TABLE records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source VARCHAR(128) NOT NULL,
                external_id VARCHAR(256) NOT NULL,
                title VARCHAR(512) NOT NULL,
                category VARCHAR(128),
                tags JSON NOT NULL,
                published_at DATETIME,
                content TEXT NOT NULL,
                url VARCHAR(2048) NOT NULL,
                fetched_at DATETIME NOT NULL,
                updated_at DATETIME NOT NULL,
                CONSTRAINT uq_records_source_external UNIQUE (source, external_id)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO records
                (source, external_id, title, category, tags, published_at,
                 content, url, fetched_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "demo_api",
                "api-1",
                "旧库标题",
                "环境",
                '["公园"]',
                None,
                "旧库正文",
                "http://example.test/api-1",
                fetched_at,
                fetched_at,
            ),
        )

    db = Database(db_path)
    try:
        db.init()
        # 迁移可重复执行，不会重复写入 version 1。
        db.init()

        with Session(db.engine) as session:
            row = session.execute(select(RecordRow)).scalar_one()
        assert row.content_hash == compute_content_hash("旧库标题", "旧库正文")
        versions = _versions(db)
        assert [item.version for item in versions] == [1]
        assert versions[0].content_hash == row.content_hash

        same = Record(
            source="demo_api",
            external_id="api-1",
            title="旧库标题",
            content="旧库正文",
            fetched_at=datetime.now(timezone.utc),
        )
        assert db.upsert_record(same) is False
        assert db.count_versions() == 1

        changed = Record(
            source="demo_api",
            external_id="api-1",
            title="旧库标题",
            content="旧库正文（已更新）",
            fetched_at=datetime.now(timezone.utc),
        )
        assert db.upsert_record(changed) is True
        assert [item.version for item in _versions(db)] == [1, 2]
    finally:
        db.close()


def test_dry_run_writes_no_versions(
    db: Database,
    raw_store: RawStore,
    api_source: SourceConfig,
) -> None:
    summary = run_source(api_source, db, raw_store, limit=2, dry_run=True)

    assert summary.ok == 2
    assert summary.changed == 0
    assert db.count_records() == 0
    assert db.count_versions() == 0


def test_cli_history_prints_version_timeline(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "history.db"
    raw_dir = tmp_path / "history-raw"

    run_result = runner.invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "1",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
        ],
    )
    assert run_result.exit_code == 0, run_result.output
    assert "changed=0" in run_result.output

    history_result = runner.invoke(
        app, ["db", "history", "--db", str(db_path), "--source", "demo_api"]
    )
    assert history_result.exit_code == 0, history_result.output
    assert "api-1" in history_result.output
    assert "content_hash" in history_result.output
    assert "1" in history_result.output


def test_cli_history_missing_database_exits_one(tmp_path: Path) -> None:
    result = runner.invoke(app, ["db", "history", "--db", str(tmp_path / "missing.db")])
    assert result.exit_code == 1
    assert "数据库不存在" in result.output
