"""CLI 互斥：第二个 run 默认退出码 3、--wait-for-lock 排队、--dry-run 不参与锁。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from crawl_flow.cli import EXIT_LOCK_BUSY, app
from crawl_flow.storage.locking import RunLock

runner = CliRunner()


def _args(config: Path, db_path: Path, raw_dir: Path, *extra: str) -> list[str]:
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


def test_second_run_rejected_with_exit_code_3(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "busy.db"
    raw_dir = tmp_path / "raw"

    holder = RunLock(db_path).acquire()
    try:
        result = runner.invoke(
            app,
            _args(
                config_path,
                db_path,
                raw_dir,
                "--source",
                "demo_api",
                "--limit",
                "1",
            ),
        )
    finally:
        holder.release()

    assert result.exit_code == EXIT_LOCK_BUSY
    assert result.exit_code != 2  # 与配置/采集/存储错误区分
    assert "运行被拒绝" in result.output
    assert "--wait-for-lock" in result.output


def test_wait_for_lock_timeout_exits_3(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "busy2.db"

    holder = RunLock(db_path).acquire()
    try:
        result = runner.invoke(
            app,
            _args(
                config_path,
                db_path,
                tmp_path / "raw",
                "--source",
                "demo_api",
                "--wait-for-lock",
                "--lock-timeout",
                "0.2",
            ),
        )
    finally:
        holder.release()

    assert result.exit_code == EXIT_LOCK_BUSY
    assert "排队等待锁" in result.output


def test_wait_for_lock_runs_after_holder_releases(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    import threading

    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "queue.db"
    holder = RunLock(db_path).acquire()

    def release_soon() -> None:
        import time

        time.sleep(0.3)
        holder.release()

    threading.Thread(target=release_soon, daemon=True).start()
    result = runner.invoke(
        app,
        _args(
            config_path,
            db_path,
            tmp_path / "raw",
            "--source",
            "demo_api",
            "--limit",
            "1",
            "--wait-for-lock",
        ),
    )
    assert result.exit_code == 0, result.output
    assert "source=demo_api ok=1" in result.output


def test_dry_run_ignores_held_lock(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "locked.db"
    lock_file = db_path.with_name("locked.db.run.lock")

    holder = RunLock(db_path).acquire()
    assert lock_file.exists()  # 持有者已留下锁文件
    try:
        result = runner.invoke(
            app,
            _args(
                config_path,
                db_path,
                tmp_path / "raw",
                "--source",
                "demo_api",
                "--limit",
                "1",
                "--dry-run",
            ),
        )
    finally:
        holder.release()

    assert result.exit_code == 0, result.output
    assert "dry-run" in result.output
    # dry-run 不创建数据库/raw 目录。
    assert not db_path.exists()
    assert not (tmp_path / "raw").exists()


def test_dry_run_creates_no_lock_file(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "clean" / "crawl.db"

    result = runner.invoke(
        app,
        _args(
            config_path,
            db_path,
            tmp_path / "raw",
            "--source",
            "demo_api",
            "--limit",
            "1",
            "--dry-run",
        ),
    )

    assert result.exit_code == 0, result.output
    assert not db_path.exists()
    assert not db_path.parent.exists()
    assert not (tmp_path / "raw").exists()


def test_dry_run_does_not_block_other_runs(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "dry-first.db"

    dry = runner.invoke(
        app,
        _args(
            config_path,
            db_path,
            tmp_path / "raw",
            "--source",
            "demo_api",
            "--limit",
            "1",
            "--dry-run",
        ),
    )
    assert dry.exit_code == 0, dry.output

    real = runner.invoke(
        app,
        _args(
            config_path,
            db_path,
            tmp_path / "raw",
            "--source",
            "demo_api",
            "--limit",
            "1",
        ),
    )
    assert real.exit_code == 0, real.output
    assert "source=demo_api ok=1" in real.output


def test_lock_conflict_distinct_from_config_error(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "codes.db"

    bad_config = runner.invoke(
        app,
        _args(
            config_path,
            tmp_path / "other.db",
            tmp_path / "raw",
            "--source",
            "nope",
        ),
    )
    assert bad_config.exit_code == 2

    holder = RunLock(db_path).acquire()
    try:
        busy = runner.invoke(
            app,
            _args(
                config_path,
                db_path,
                tmp_path / "raw",
                "--source",
                "demo_api",
            ),
        )
    finally:
        holder.release()
    assert busy.exit_code == 3


def test_db_stats_readonly_while_lock_held(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    """持锁状态下 db stats 不需要锁，直接只读出表格。"""

    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "stats.db"
    seeded = runner.invoke(
        app,
        _args(
            config_path,
            db_path,
            tmp_path / "raw",
            "--source",
            "demo_api",
            "--limit",
            "2",
        ),
    )
    assert seeded.exit_code == 0, seeded.output

    holder = RunLock(db_path).acquire()
    try:
        stats = runner.invoke(app, ["db", "stats", "--db", str(db_path)])
    finally:
        holder.release()

    assert stats.exit_code == 0, stats.output
    assert "demo_api" in stats.output
    assert "TOTAL" in stats.output
