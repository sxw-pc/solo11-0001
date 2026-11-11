"""调度器内存占用：单进程运行调度循环 + 指标端点，RSS 不得超过 100MB。

在子进程中测量，避免 pytest 自身的内存基线干扰。
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MAX_RSS_KB = 100 * 1024  # 100MB

_SCRIPT = """
import resource, time
from crawl_flow.config import SourceConfig
from crawl_flow.scheduler.service import SchedulerService
from crawl_flow.observability.metrics import render_metrics
from crawl_flow.observability.metrics_server import MetricsServer

sources = [
    SourceConfig(
        name=f"src_{i}", type="api", base_url="http://127.0.0.1:1", schedule="* * * * *"
    )
    for i in range(10)
]
service = SchedulerService(
    sources,
    db_path=DB_PATH,
    raw_dir=RAW_DIR,
    runner=lambda source: time.sleep(0.01),
)
server = MetricsServer(
    "127.0.0.1", 0,
    lambda: render_metrics(service._db.job_run_aggregates(), scheduler_state=service.jobs_snapshot()),
)
service.start()
server.start()
time.sleep(2.0)
print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
server.stop()
service.stop()
"""


def test_scheduler_memory_footprint(tmp_path: Path) -> None:
    script = _SCRIPT.replace("DB_PATH", repr(str(tmp_path / "mem.db"))).replace(
        "RAW_DIR", repr(str(tmp_path / "raw"))
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(REPO_ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    result = subprocess.run(
        [sys.executable, "-I", "-c", script],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=REPO_ROOT,
    )
    assert result.returncode == 0, result.stderr
    rss_kb = int(result.stdout.strip().splitlines()[-1])
    assert rss_kb < MAX_RSS_KB, f"调度进程 RSS {rss_kb / 1024:.1f}MB 超过 100MB 上限"
