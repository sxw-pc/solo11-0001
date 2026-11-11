"""指标：job_runs 持久化、聚合查询、Prometheus 文本渲染。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crawl_flow.models import JobRun
from crawl_flow.observability.metrics import render_metrics, summarize_sources
from crawl_flow.storage.db import Database, JOB_RUNS_KEEP_PER_SOURCE


def _run(
    source: str,
    *,
    trigger: str = "manual",
    status: str = "succeeded",
    ok: int = 0,
    failed: int = 0,
    skipped: int = 0,
    elapsed_ms: int = 100,
    at: datetime | None = None,
    error: str | None = None,
) -> JobRun:
    moment = at or datetime(2026, 11, 12, 10, 0, tzinfo=timezone.utc)
    return JobRun(
        source=source,
        trigger=trigger,  # type: ignore[arg-type]
        status=status,  # type: ignore[arg-type]
        ok_count=ok,
        failed_count=failed,
        skipped_count=skipped,
        elapsed_ms=elapsed_ms,
        started_at=moment,
        finished_at=moment + timedelta(milliseconds=elapsed_ms),
        error=error,
    )


@pytest.fixture
def db_with_runs(db: Database) -> Database:
    db.insert_job_run(_run("demo_api", trigger="manual", ok=5, elapsed_ms=200))
    db.insert_job_run(_run("demo_api", trigger="scheduled", ok=4, failed=1, elapsed_ms=400))
    db.insert_job_run(_run("demo_api", trigger="scheduled", status="skipped", elapsed_ms=0))
    db.insert_job_run(
        _run("demo_html", trigger="scheduled", status="failed", elapsed_ms=50, error="HTTP 503")
    )
    return db


def test_aggregates_counts(db_with_runs: Database) -> None:
    aggregates = db_with_runs.job_run_aggregates()

    runs = {
        (row["source"], row["trigger"], row["status"]): row["count"]
        for row in aggregates["runs"]
    }
    assert runs[("demo_api", "manual", "succeeded")] == 1
    assert runs[("demo_api", "scheduled", "succeeded")] == 1
    assert runs[("demo_api", "scheduled", "skipped")] == 1
    assert runs[("demo_html", "scheduled", "failed")] == 1

    items = {row["source"]: row for row in aggregates["items"]}
    assert items["demo_api"]["ok"] == 9
    assert items["demo_api"]["failed"] == 1

    durations = {row["source"]: row for row in aggregates["durations"]}
    # skipped 不计入平均耗时：(200 + 400) / 2
    assert durations["demo_api"]["avg_ms"] == pytest.approx(300.0)
    assert durations["demo_api"]["runs"] == 2


def test_recent_job_runs_order_and_filter(db_with_runs: Database) -> None:
    runs = db_with_runs.recent_job_runs()
    assert len(runs) == 4
    assert [row["id"] for row in runs] == sorted(row["id"] for row in runs)[::-1]

    only_api = db_with_runs.recent_job_runs(source="demo_api")
    assert len(only_api) == 3
    assert all(row["source"] == "demo_api" for row in only_api)


def test_prune_keeps_last_n_per_source(db: Database) -> None:
    for index in range(30):
        db.insert_job_run(_run("demo_api", at=datetime(2026, 11, 12, tzinfo=timezone.utc) + timedelta(minutes=index)), keep_per_source=10)
    db.insert_job_run(_run("demo_html"), keep_per_source=10)

    assert len(db.recent_job_runs(source="demo_api", limit=100)) == 10
    assert len(db.recent_job_runs(source="demo_html", limit=100)) == 1
    # 保留的是最新的 10 条
    kept = db.recent_job_runs(source="demo_api", limit=100)
    assert max(row["id"] for row in kept) == 30


def test_render_prometheus_format(db_with_runs: Database) -> None:
    text = render_metrics(db_with_runs.job_run_aggregates())

    assert "# HELP crawl_flow_runs_total" in text
    assert "# TYPE crawl_flow_runs_total counter" in text
    assert 'crawl_flow_runs_total{source="demo_api",trigger="manual",status="succeeded"} 1' in text
    assert 'crawl_flow_runs_total{source="demo_api",trigger="scheduled",status="skipped"} 1' in text
    assert 'crawl_flow_items_total{source="demo_api",status="ok"} 9' in text
    assert 'crawl_flow_items_total{source="demo_api",status="failed"} 1' in text
    assert 'crawl_flow_run_duration_seconds_avg{source="demo_api"} 0.3' in text
    assert 'crawl_flow_run_last_success{source="demo_api"} 1' in text
    assert 'crawl_flow_run_last_success{source="demo_html"} 0' in text
    assert "crawl_flow_run_last_timestamp_seconds" in text
    assert text.endswith("\n")


def test_render_includes_scheduler_state(db_with_runs: Database) -> None:
    state = [
        {"source": "demo_api", "running": False, "next_fire": datetime(2026, 11, 12, 10, 5, tzinfo=timezone.utc)},
        {"source": "demo_html", "running": True, "next_fire": None},
    ]
    text = render_metrics(db_with_runs.job_run_aggregates(), scheduler_state=state)
    assert "crawl_flow_scheduler_jobs 2" in text
    assert 'crawl_flow_scheduler_job_running{source="demo_api"} 0' in text
    assert 'crawl_flow_scheduler_job_running{source="demo_html"} 1' in text
    assert 'crawl_flow_scheduler_next_fire_timestamp_seconds{source="demo_api"}' in text
    # next_fire 为 None 的任务不输出该指标
    assert 'crawl_flow_scheduler_next_fire_timestamp_seconds{source="demo_html"}' not in text


def test_render_contains_no_sensitive_config(db_with_runs: Database) -> None:
    text = render_metrics(
        db_with_runs.job_run_aggregates(),
        scheduler_state=[{"source": "demo_api", "running": False, "next_fire": None}],
    )
    for forbidden in (
        "http://",
        "https://",
        "base_url",
        "field_map",
        "list_path",
        "detail_path",
        "data/",
        ".db",
        "HTTP 503",  # 错误详情不进指标
    ):
        assert forbidden not in text


def test_label_value_escaping(db: Database) -> None:
    db.insert_job_run(_run('we"ird\\source'))
    text = render_metrics(db.job_run_aggregates())
    assert 'source="we\\"ird\\\\source"' in text


def test_summarize_sources(db_with_runs: Database) -> None:
    rows = {row["source"]: row for row in summarize_sources(db_with_runs.job_run_aggregates())}

    api = rows["demo_api"]
    assert api["runs"] == 3
    assert api["succeeded"] == 2
    assert api["skipped"] == 1
    # 成功率只统计真正执行的轮次：2/2 = 100%
    assert api["success_rate"] == pytest.approx(1.0)
    assert api["avg_ms"] == pytest.approx(300.0)
    assert api["items_ok"] == 9

    html = rows["demo_html"]
    assert html["success_rate"] == pytest.approx(0.0)
    assert html["last_status"] == "failed"


def test_empty_database_renders_valid_text(db: Database) -> None:
    text = render_metrics(db.job_run_aggregates())
    assert "# TYPE crawl_flow_runs_total counter" in text
    assert "crawl_flow_runs_total{" not in text
    assert summarize_sources(db.job_run_aggregates()) == []


def test_keep_constant_reasonable() -> None:
    assert 100 <= JOB_RUNS_KEEP_PER_SOURCE <= 100000
