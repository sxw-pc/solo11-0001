"""CLI 冒烟：run / --all / --dry-run / db stats / 配置与整轮失败退出码。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.storage.db import Database

runner = CliRunner()


def _run_args(config: Path, db_path: Path, raw_dir: Path, *extra: str) -> list[str]:
    return [
        "run",
        "--config",
        str(config),
        "--db",
        str(db_path),
        "--raw-dir",
        str(raw_dir),
        *extra,
    ]


def test_run_limit_two_and_db_stats(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [source_payload("demo_api"), source_payload("demo_html", "html")]
    )
    db_path = tmp_path / "smoke.db"

    result = runner.invoke(
        app,
        _run_args(config_path, db_path, tmp_path / "raw", "--source", "demo_api", "--limit", "2"),
    )
    assert result.exit_code == 0, result.output
    assert "source=demo_api ok=2" in result.output

    database = Database(db_path)
    database.init()
    try:
        assert database.count_records("demo_api") == 2
        assert database.count_raw("demo_api") == 2
    finally:
        database.close()

    stats = runner.invoke(app, ["db", "stats", "--db", str(db_path)])
    assert stats.exit_code == 0, stats.output
    assert "demo_api" in stats.output
    assert "TOTAL" in stats.output


def test_all_runs_every_enabled_source(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [
            source_payload("demo_api"),
            source_payload("demo_html", "html"),
            source_payload("demo_disabled", enabled=False),
        ]
    )
    db_path = tmp_path / "all.db"

    result = runner.invoke(
        app,
        _run_args(config_path, db_path, tmp_path / "raw", "--all", "--limit", "1"),
    )
    assert result.exit_code == 0, result.output
    assert "source=demo_api ok=1" in result.output
    assert "source=demo_html ok=1" in result.output
    assert "demo_disabled" not in result.output


def test_dry_run_writes_no_rows_or_raw_files(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "dry.db"
    raw_dir = tmp_path / "dry-raw"

    result = runner.invoke(
        app,
        _run_args(
            config_path,
            db_path,
            raw_dir,
            "--source",
            "demo_api",
            "--limit",
            "2",
            "--dry-run",
        ),
    )
    assert result.exit_code == 0, result.output
    assert "dry-run" in result.output

    database = Database(db_path)
    database.init()
    try:
        assert database.count_records() == 0
        assert database.count_raw() == 0
    finally:
        database.close()
    assert not raw_dir.exists()


def test_unknown_source_type_exits_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("bad", source_type="ftp")])
    result = runner.invoke(
        app,
        _run_args(config_path, tmp_path / "bad.db", tmp_path / "raw", "--all"),
    )
    assert result.exit_code == 2
    assert "配置错误" in result.output


def test_unknown_source_name_exits_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = runner.invoke(
        app,
        _run_args(config_path, tmp_path / "x.db", tmp_path / "raw", "--source", "nope"),
    )
    assert result.exit_code == 2
    assert "未找到源" in result.output


def test_list_page_5xx_aborts_with_exit_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [source_payload("demo_api_broken", list_path="/api/items?fail=1")]
    )
    result = runner.invoke(
        app,
        _run_args(
            config_path,
            tmp_path / "broken.db",
            tmp_path / "raw",
            "--source",
            "demo_api_broken",
            "--limit",
            "1",
        ),
    )
    assert result.exit_code == 2
    assert "整轮中止" in result.output
