"""游标 CLI 与真实进程级断点续采测试。

- ``cursor list`` / ``cursor reset`` 子命令的行为与退出码。
- 在独立子进程中跑采集，用 SIGTERM 强制终止，重启后验证从断点继续、
  已完成条目不重复请求（验收点 2 的端到端验证）。
"""

from __future__ import annotations

import json
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner
from werkzeug.serving import make_server

from crawl_flow.cli import app
from crawl_flow.cursor.store import CursorStore

runner = CliRunner()


@pytest.fixture(scope="module")
def mock_server() -> Any:
    from mock_site.app import create_app

    repo_root = Path(__file__).resolve().parents[1]
    server = make_server(
        "127.0.0.1", 0, create_app(repo_root / "mock_site" / "fixtures")
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


def _write_cursor_config(path: Path, base_url: str, name: str = "demo_api") -> None:
    import yaml

    path.write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "name": name,
                        "type": "api",
                        "base_url": base_url,
                        "cursor": {"enabled": True, "rule": "completed_ids"},
                    }
                ]
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def test_cursor_list_and_reset_cli(
    mock_server: str, tmp_path: Path, write_config: Any, source_payload: Any
) -> None:
    config_path = write_config(
        [
            source_payload(
                "demo_api",
                cursor={"enabled": True, "rule": "completed_ids"},
            )
        ]
    )
    db_path = tmp_path / "cli.db"
    raw_dir = tmp_path / "raw"
    cursor_dir = tmp_path / "cursors"

    run = runner.invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "4",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
            "--cursor-dir",
            str(cursor_dir),
        ],
    )
    assert run.exit_code == 0, run.output
    assert "cursor_skipped=0" in run.output

    listing = runner.invoke(app, ["cursor", "list", "--cursor-dir", str(cursor_dir)])
    assert listing.exit_code == 0, listing.output
    assert "demo_api" in listing.output
    assert "completed_ids" in listing.output
    assert "4" in listing.output

    reset = runner.invoke(
        app,
        ["cursor", "reset", "--source", "demo_api", "--config", str(config_path),
         "--cursor-dir", str(cursor_dir)],
    )
    assert reset.exit_code == 0, reset.output
    assert "已重置游标" in reset.output
    assert CursorStore(cursor_dir).exists("demo_api") is False

    # 重置后再次运行回到全量：cursor_skipped 为 0。
    rerun = runner.invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "4",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
            "--cursor-dir",
            str(cursor_dir),
        ],
    )
    assert rerun.exit_code == 0, rerun.output
    assert "cursor_skipped=0" in rerun.output


def test_corrupt_cursor_warns_and_run_succeeds_cli(
    mock_server: str,
    tmp_path: Path,
    write_config: Any,
    source_payload: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config_path = write_config(
        [source_payload("demo_api", cursor={"enabled": True})]
    )
    cursor_dir = tmp_path / "cursors"
    cursor_dir.mkdir(parents=True)
    (cursor_dir / "demo_api.json").write_text("CORRUPT{json", encoding="utf-8")

    with caplog.at_level("WARNING", logger="crawl_flow.cursor"):
        result = runner.invoke(
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
                str(tmp_path / "c.db"),
                "--raw-dir",
                str(tmp_path / "raw"),
                "--cursor-dir",
                str(cursor_dir),
            ],
        )
    assert result.exit_code == 0, result.output
    # 损坏自动降级为全量采集：任务不崩、前 2 条正常处理，并输出告警日志。
    assert any("cursor degraded" in r.getMessage() for r in caplog.records)
    assert "ok=2" in result.output
    assert "cursor_skipped=0" in result.output


def test_sigterm_resume_from_last_position(
    mock_server: str, tmp_path: Path
) -> None:
    """子进程跑到第 3 条时被 SIGTERM 杀掉，重启后从断点继续且不重复请求。"""

    repo_root = Path(__file__).resolve().parents[1]
    config_path = tmp_path / "sources.yaml"
    _write_cursor_config(config_path, mock_server)
    db_path = tmp_path / "sigterm.db"
    raw_dir = tmp_path / "raw"
    cursor_dir = tmp_path / "cursors"
    env = {
        "PYTHONPATH": f"{repo_root / 'src'}{':'}{repo_root}",
        "PATH": __import__("os").environ.get("PATH", ""),
    }

    def _cmd(limit: str | None = None) -> list[str]:
        cmd = [
            sys.executable,
            "-m",
            "crawl_flow.cli",
            "run",
            "--source",
            "demo_api",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
            "--cursor-dir",
            str(cursor_dir),
        ]
        if limit:
            cmd += ["--limit", limit]
        return cmd

    # 第一轮：只跑 5 条后正常结束，建立游标。
    first = subprocess.run(
        _cmd(limit="5"), cwd=repo_root, env=env, capture_output=True, text=True
    )
    assert first.returncode == 0, first.stderr
    cursor_file = cursor_dir / "demo_api.json"
    assert cursor_file.is_file()
    first_ids = set(json.loads(cursor_file.read_text(encoding="utf-8"))["state"]["completed_ids"])
    assert first_ids == {f"api-{i}" for i in range(1, 6)}

    # 第二轮：全量跑（无 limit），在进行中发 SIGTERM 强制终止。
    proc = subprocess.Popen(
        _cmd(), cwd=repo_root, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    # 等足够多的条目落盘后强杀；mock 本地很快，轮询游标增长即可。
    deadline = time.time() + 20
    killed = False
    while time.time() < deadline:
        if cursor_file.is_file():
            count = len(
                json.loads(cursor_file.read_text(encoding="utf-8"))["state"]["completed_ids"]
            )
            if count >= 8:
                proc.send_signal(signal.SIGTERM)
                killed = True
                break
        time.sleep(0.05)
    assert killed, "子进程未能在超时内推进到预期断点"
    proc.wait(timeout=10)
    assert proc.returncode != 0

    interrupted_ids = set(
        json.loads(cursor_file.read_text(encoding="utf-8"))["state"]["completed_ids"]
    )
    assert first_ids.issubset(interrupted_ids)
    assert len(interrupted_ids) >= 8

    # 第三轮：重启续跑，正常结束；已完成 ID 数量最终覆盖全部 15 条。
    resumed = subprocess.run(
        _cmd(), cwd=repo_root, env=env, capture_output=True, text=True
    )
    assert resumed.returncode == 0, resumed.stderr
    final_ids = set(
        json.loads(cursor_file.read_text(encoding="utf-8"))["state"]["completed_ids"]
    )
    assert final_ids == {f"api-{i}" for i in range(1, 16)}
    # 续跑日志中跳过数等于中断时已完成数。
    assert f"cursor_skipped={len(interrupted_ids)}" in resumed.stdout
