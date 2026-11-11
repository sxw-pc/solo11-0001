"""预算相关 CLI：熔断退出码、当日状态子命令、损坏拒绝启动、dry-run 估算输出。"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.budget import BudgetLedger

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


def test_cli_breaks_with_exit_code_3_and_keeps_completed_items(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    """验收：8 次/天上限，第 9 次请求前熔断，退出码 3，已完成条目不丢。"""

    config_path = write_config(
        [source_payload("demo_api", daily_request_limit=8)]
    )
    db_path = tmp_path / "crawl.db"
    raw_dir = tmp_path / "raw"

    result = runner.invoke(
        app,
        _run_args(config_path, db_path, raw_dir, "--source", "demo_api"),
    )
    assert result.exit_code == 3, result.output
    assert "预算熔断" in result.output
    assert "请求次数上限" in result.output
    assert "requests=8" in result.output
    # 已完成的 6 条详情（5 + 1）正常入库。
    ledger = BudgetLedger(db_path)
    try:
        assert ledger.get_counters("demo_api").request_count == 8
    finally:
        ledger.close()

    from crawl_flow.storage.db import Database

    database = Database(db_path)
    try:
        assert database.count_records("demo_api") == 6
    finally:
        database.close()


def test_cli_second_run_same_day_rejects_immediately(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    """验收：当天第二轮立即拒绝，退出码 3，不新增请求与条目。"""

    config_path = write_config(
        [source_payload("demo_api", daily_request_limit=8)]
    )
    db_path = tmp_path / "crawl.db"
    raw_dir = tmp_path / "raw"
    args = _run_args(config_path, db_path, raw_dir, "--source", "demo_api")

    first = runner.invoke(app, args)
    assert first.exit_code == 3, first.output

    second = runner.invoke(app, args)
    assert second.exit_code == 3, second.output
    assert "预算熔断" in second.output

    ledger = BudgetLedger(db_path)
    try:
        assert ledger.get_counters("demo_api").request_count == 8
    finally:
        ledger.close()

    from crawl_flow.storage.db import Database

    database = Database(db_path)
    try:
        assert database.count_records("demo_api") == 6  # 没有新增
    finally:
        database.close()


def test_cli_unlimited_source_exits_zero(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    """不配预算的源行为不变：正常结束，退出码 0，但账本照样计数。"""

    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "crawl.db"

    result = runner.invoke(
        app,
        _run_args(
            config_path, db_path, tmp_path / "raw",
            "--source", "demo_api", "--limit", "2",
        ),
    )
    assert result.exit_code == 0, result.output
    ledger = BudgetLedger(db_path)
    try:
        assert ledger.get_counters("demo_api").request_count > 0
    finally:
        ledger.close()


def test_budget_status_shows_used_and_remaining(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [
            source_payload("demo_api", daily_request_limit=8, daily_bytes_limit=10_485_760),
            source_payload("demo_html", "html"),
        ]
    )
    db_path = tmp_path / "crawl.db"

    # 先跑一轮产生账本数据（limit=1，只发少量请求）。
    run_result = runner.invoke(
        app,
        _run_args(
            config_path, db_path, tmp_path / "raw",
            "--source", "demo_api", "--limit", "1",
        ),
    )
    assert run_result.exit_code == 0, run_result.output

    status = runner.invoke(
        app,
        ["budget", "status", "--config", str(config_path), "--db", str(db_path)],
    )
    assert status.exit_code == 0, status.output
    assert "UTC 自然日" in status.output
    assert "demo_api" in status.output
    assert "demo_html" in status.output
    # 表格里要能看到上限 8、剩余 = 8 - 已用，以及不限字样。
    assert "不限" in status.output
    lines = [line for line in status.output.splitlines() if line.startswith("demo_api")]
    assert lines
    cells = lines[0].split()
    # source req_used req_limit req_left bytes_used bytes_limit bytes_left
    assert cells[2] == "8"
    assert int(cells[1]) + int(cells[3]) == 8
    assert cells[5] == "10485760"


def test_budget_status_missing_db_exits_1(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = runner.invoke(
        app,
        [
            "budget", "status",
            "--config", str(config_path),
            "--db", str(tmp_path / "missing.db"),
        ],
    )
    assert result.exit_code == 1
    assert "数据库不存在" in result.output


def test_corrupt_ledger_makes_run_refuse_to_start(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "corrupt.db"
    # 先建一个合法库，再把文件头写坏，模拟磁盘损坏。
    db_path.write_bytes(b"")
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE t (x INTEGER)")
    conn.commit()
    conn.close()
    db_path.write_bytes(b"X" * 64 + db_path.read_bytes()[64:])

    result = runner.invoke(
        app,
        _run_args(config_path, db_path, tmp_path / "raw", "--source", "demo_api"),
    )
    assert result.exit_code == 2
    assert "预算账本损坏" in result.output


def test_corrupt_ledger_makes_status_refuse(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "corrupt-status.db"
    db_path.write_bytes(b"not a database" * 30)

    result = runner.invoke(
        app,
        ["budget", "status", "--config", str(config_path), "--db", str(db_path)],
    )
    assert result.exit_code == 2
    assert "预算账本损坏" in result.output


def test_dry_run_estimates_budget_with_exit_zero(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [source_payload("demo_api", daily_request_limit=2)]
    )
    db_path = tmp_path / "crawl.db"

    result = runner.invoke(
        app,
        _run_args(
            config_path, db_path, tmp_path / "raw",
            "--source", "demo_api", "--dry-run",
        ),
    )
    assert result.exit_code == 0, result.output
    assert "预算估算" in result.output
    assert "演练未记账" in result.output
    # dry-run 不创建账本表：库里没有 budget_usage。
    conn = sqlite3.connect(db_path)
    try:
        names = [
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        ]
    finally:
        conn.close()
    assert "budget_usage" not in names


def test_byte_limit_break_exit_code_3(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [source_payload("demo_api", daily_bytes_limit=1)]
    )
    db_path = tmp_path / "crawl.db"

    result = runner.invoke(
        app,
        _run_args(config_path, db_path, tmp_path / "raw", "--source", "demo_api"),
    )
    assert result.exit_code == 3, result.output
    assert "响应字节上限" in result.output
