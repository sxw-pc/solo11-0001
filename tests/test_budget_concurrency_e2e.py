"""端到端验收：两个独立采集进程并发跑同一预算账本，计数不重不漏不超额。

与纯线程测试不同，这里用 subprocess 启动两个真实的 ``crawl-flow`` 进程，
各自打开独立的 sqlite 连接、争抢同一账本文件，完整覆盖 WAL +
BEGIN IMMEDIATE 的跨进程串行化路径。
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import yaml

from crawl_flow.budget import BudgetLedger

REPO_ROOT = Path(__file__).resolve().parents[1]


def _write_capped_source(config_path: Path, base_url: str, request_limit: int) -> None:
    config_path.write_text(
        yaml.safe_dump(
            {
                "sources": [
                    {
                        "name": "demo_api",
                        "type": "api",
                        "base_url": base_url,
                        "daily_request_limit": request_limit,
                    }
                ]
            },
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def test_two_concurrent_processes_never_overshoot(
    mock_base_url: str,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "sources.yaml"
    db_path = tmp_path / "crawl.db"
    raw_dir = tmp_path / "raw"
    # 6 次额度，两个进程同时开跑：总共只可能成功预留 6 次。
    request_limit = 6
    _write_capped_source(config_path, mock_base_url, request_limit)

    env = dict(os.environ)
    env["PYTHONPATH"] = (
        f"{REPO_ROOT / 'src'}{os.pathsep}{REPO_ROOT}"
        + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    )
    base_cmd = [
        sys.executable,
        "-m",
        "crawl_flow.cli",
        "run",
        "--config",
        str(config_path),
        "--db",
        str(db_path),
        "--raw-dir",
        str(raw_dir),
        "--source",
        "demo_api",
    ]

    processes = [subprocess.Popen(base_cmd, env=env) for _ in range(2)]
    exit_codes = [proc.wait(timeout=120) for proc in processes]

    # 退出码只能是 0（没抢到额度前跑完）或 3（熔断）；不能出现 2（账本错误）。
    assert all(code in (0, 3) for code in exit_codes), exit_codes
    assert 3 in exit_codes, f"至少一个进程应被熔断，实际: {exit_codes}"

    ledger = BudgetLedger(db_path)
    try:
        counters = ledger.get_counters("demo_api")
        # 核心验收：两个进程加起来恰好 6 次，不重不漏不超额。
        assert counters.request_count == request_limit
        assert counters.response_bytes > 0
        # 账本自身完整性校验通过（重新打开即触发）。
    finally:
        ledger.close()

    # 第三个进程再来，当天必须立即被拒。
    follow_up = subprocess.run(
        base_cmd, env=env, capture_output=True, text=True, timeout=120
    )
    assert follow_up.returncode == 3, follow_up.stdout + follow_up.stderr
    assert "预算熔断" in follow_up.stderr or "预算熔断" in follow_up.stdout
    ledger = BudgetLedger(db_path)
    try:
        assert ledger.get_counters("demo_api").request_count == request_limit
    finally:
        ledger.close()
