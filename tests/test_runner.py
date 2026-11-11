"""execute_and_record：手动与定时共用的「执行 + 记指标」入口。"""

from __future__ import annotations

from pathlib import Path

import pytest

from crawl_flow.config import SourceConfig
from crawl_flow.errors import FetchError, StoreError
from crawl_flow.scheduler.runner import execute_and_record
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def test_successful_run_recorded(
    db: Database, raw_store: RawStore, api_source: SourceConfig
) -> None:
    summary = execute_and_record(api_source, db, raw_store, trigger="manual", limit=3)
    assert summary.ok == 3

    runs = db.recent_job_runs()
    assert len(runs) == 1
    row = runs[0]
    assert row["source"] == "demo_api"
    assert row["trigger"] == "manual"
    assert row["status"] == "succeeded"
    assert row["ok_count"] == 3
    assert row["failed_count"] == 0
    assert row["elapsed_ms"] >= 0
    assert row["started_at"] is not None and row["finished_at"] is not None


def test_failed_run_recorded_and_reraised(
    db: Database, raw_store: RawStore, make_source
) -> None:
    broken = make_source("broken", list_path="/api/items?fail=1")
    with pytest.raises(FetchError):
        execute_and_record(broken, db, raw_store, trigger="scheduled")

    runs = db.recent_job_runs()
    assert len(runs) == 1
    assert runs[0]["status"] == "failed"
    assert runs[0]["trigger"] == "scheduled"
    assert runs[0]["error"]


def test_dry_run_records_nothing(
    db: Database, raw_store: RawStore, api_source: SourceConfig
) -> None:
    summary = execute_and_record(api_source, db, raw_store, trigger="manual", limit=2, dry_run=True)
    assert summary.ok == 2
    assert db.recent_job_runs() == []
    assert db.count_records() == 0


def test_metrics_failure_does_not_break_run(
    db: Database, raw_store: RawStore, api_source: SourceConfig, monkeypatch
) -> None:
    def boom(run, **kwargs):
        raise StoreError("disk full")

    monkeypatch.setattr(db, "insert_job_run", boom)
    summary = execute_and_record(api_source, db, raw_store, trigger="manual", limit=2)
    assert summary.ok == 2
    assert db.count_records("demo_api") == 2  # 采集本身不受影响


def test_manual_and_scheduled_accumulate(
    db: Database, raw_store: RawStore, api_source: SourceConfig
) -> None:
    """同一源手动与定时各跑一次，指标在同一份存储中累加。"""

    execute_and_record(api_source, db, raw_store, trigger="manual", limit=2)
    execute_and_record(api_source, db, raw_store, trigger="scheduled", limit=3)

    aggregates = db.job_run_aggregates()
    runs = {
        (row["trigger"], row["status"]): row["count"]
        for row in aggregates["runs"]
        if row["source"] == "demo_api"
    }
    assert runs[("manual", "succeeded")] == 1
    assert runs[("scheduled", "succeeded")] == 1
    items = next(row for row in aggregates["items"] if row["source"] == "demo_api")
    assert items["ok"] == 5
