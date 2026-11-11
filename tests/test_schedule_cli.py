"""CLI：schedule list / history / start 与 metrics 命令、手动触发计入指标。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.storage.db import Database

runner = CliRunner()


def _args(config: Path, db_path: Path, raw_dir: Path, *extra: str) -> list[str]:
    return [
        "--config",
        str(config),
        "--db",
        str(db_path),
        "--raw-dir",
        str(raw_dir),
        *extra,
    ]


def test_run_records_manual_metrics(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "metrics.db"

    result = runner.invoke(
        app,
        ["run", *_args(config_path, db_path, tmp_path / "raw"), "--source", "demo_api", "--limit", "2"],
    )
    assert result.exit_code == 0, result.output

    database = Database(db_path)
    database.init()
    try:
        runs = database.recent_job_runs()
        assert len(runs) == 1
        assert runs[0]["trigger"] == "manual"
        assert runs[0]["status"] == "succeeded"
        assert runs[0]["ok_count"] == 2
    finally:
        database.close()


def test_schedule_list_shows_cron_and_next_fire(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config(
        [
            source_payload("demo_api", schedule="*/5 * * * *"),
            source_payload("demo_html", source_type="html"),
        ]
    )
    result = runner.invoke(app, ["schedule", "list", "--config", str(config_path)])
    assert result.exit_code == 0, result.output
    assert "demo_api" in result.output
    assert "*/5 * * * *" in result.output
    assert "demo_html" in result.output  # 无计划的源也列出，schedule 为 -


def test_schedule_list_rejects_invalid_cron(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config([source_payload("demo_api", schedule="not a cron")])
    result = runner.invoke(app, ["schedule", "list", "--config", str(config_path)])
    assert result.exit_code == 2
    assert "配置错误" in result.output


def test_schedule_history_and_metrics_commands(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config([source_payload("demo_api", schedule="*/5 * * * *")])
    db_path = tmp_path / "history.db"
    raw_dir = tmp_path / "raw"

    run_result = runner.invoke(
        app,
        ["run", *_args(config_path, db_path, raw_dir), "--source", "demo_api", "--limit", "2"],
    )
    assert run_result.exit_code == 0, run_result.output

    history = runner.invoke(app, ["schedule", "history", "--db", str(db_path)])
    assert history.exit_code == 0, history.output
    assert "按源汇总" in history.output
    assert "demo_api" in history.output
    assert "100.0" in history.output  # 成功率
    assert "manual" in history.output

    metrics = runner.invoke(app, ["metrics", "--db", str(db_path)])
    assert metrics.exit_code == 0, metrics.output
    assert "# TYPE crawl_flow_runs_total counter" in metrics.output
    assert 'crawl_flow_runs_total{source="demo_api",trigger="manual",status="succeeded"} 1' in metrics.output
    assert 'crawl_flow_items_total{source="demo_api",status="ok"} 2' in metrics.output


def test_schedule_history_missing_db_exits_1(tmp_path: Path) -> None:
    result = runner.invoke(app, ["schedule", "history", "--db", str(tmp_path / "none.db")])
    assert result.exit_code == 1
    assert "数据库不存在" in result.output


def test_metrics_missing_db_exits_1(tmp_path: Path) -> None:
    result = runner.invoke(app, ["metrics", "--db", str(tmp_path / "none.db")])
    assert result.exit_code == 1


def test_schedule_start_without_any_schedule_exits_1(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = runner.invoke(
        app,
        [
            "schedule",
            "start",
            *_args(config_path, tmp_path / "s.db", tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 1
    assert "没有任何 enabled 源配置 schedule" in result.output


def test_disabled_source_with_schedule_not_scheduled(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config([source_payload("demo_api", enabled=False, schedule="*/5 * * * *")])
    result = runner.invoke(
        app,
        ["schedule", "start", *_args(config_path, tmp_path / "s.db", tmp_path / "raw")],
    )
    assert result.exit_code == 1  # disabled 源不参与调度 → 没有任务
