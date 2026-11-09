"""错误隔离：详情 500 不中断整轮，raw_records 留 failed 行，CLI 退出码 0。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.models import RawRecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def test_detail_500_does_not_abort_round(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    flaky = make_source(
        "demo_api_flaky",
        detail_path="/api/items/{id}?fail=1",
        retry={"max_retries": 0},
    )
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
