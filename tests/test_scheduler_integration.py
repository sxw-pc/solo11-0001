"""调度模式与采集管道的集成测试（对接本地 mock 站点）。"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.fetch.client import Fetcher
from crawl_flow.pipeline.concurrent import (
    ThreadSafeDatabase,
    ThreadSafeRawStore,
    run_source_concurrent,
)
from crawl_flow.scheduler import (
    SchedulerConfig,
    RequestScheduler,
    load_scheduler_yaml,
)
from crawl_flow.errors import ConfigError
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def _scheduler(**kwargs: Any) -> SchedulerConfig:
    defaults: dict[str, Any] = {"max_concurrency": 4}
    defaults.update(kwargs)
    return SchedulerConfig(**defaults)


def test_concurrent_run_matches_serial_results(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    source = make_source("demo_api", page_size=5)
    with Fetcher(timeout=5.0) as fetcher:
        scheduler = RequestScheduler(
            _scheduler(max_concurrency=4, default_source_concurrency=4),
            transport=fetcher,
        )
        try:
            summary = run_source_concurrent(
                source,
                db,
                raw_store,
                scheduler,
                fetcher=fetcher,
                limit=8,
            )
        finally:
            scheduler.close()

    # mock 站点共 15 条；limit=8 → 8 成功、其余 skipped。
    assert (summary.ok, summary.failed, summary.skipped) == (8, 0, 7)
    assert db.count_records("demo_api") == 8
    assert db.count_raw("demo_api", "ok") == 8


def test_concurrent_error_isolation_marks_failures(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    flaky = make_source("demo_api_flaky", detail_path="/api/items/{id}?fail=1")
    with Fetcher(timeout=5.0) as fetcher:
        scheduler = RequestScheduler(
            _scheduler(max_concurrency=4), transport=fetcher
        )
        try:
            summary = run_source_concurrent(
                flaky,
                db,
                raw_store,
                scheduler,
                fetcher=fetcher,
                limit=5,
            )
        finally:
            scheduler.close()

    # 与串行管道完全一致的错误隔离语义。
    assert (summary.ok, summary.failed, summary.skipped) == (0, 5, 10)
    assert db.count_records() == 0
    assert db.count_raw("demo_api_flaky", "failed") == 5


def test_two_sources_run_concurrently_in_one_pool(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    api_source = make_source("demo_api")
    html_source = make_source("demo_html", source_type="html")
    with Fetcher(timeout=5.0) as fetcher:
        scheduler = RequestScheduler(
            _scheduler(
                max_concurrency=6,
                sources=[
                    {"name": "demo_api", "max_concurrency": 3, "weight": 2},
                    {"name": "demo_html", "max_concurrency": 3, "weight": 1},
                ],
            ),
            transport=fetcher,
        )
        try:
            t1 = threading.Thread(
                target=run_source_concurrent,
                args=(api_source, db, raw_store, scheduler),
                kwargs={"fetcher": fetcher, "limit": 6},
            )
            t2 = threading.Thread(
                target=run_source_concurrent,
                args=(html_source, db, raw_store, scheduler),
                kwargs={"fetcher": fetcher, "limit": 6},
            )
            t1.start()
            t2.start()
            t1.join(timeout=30)
            t2.join(timeout=30)
            assert not t1.is_alive() and not t2.is_alive()
            stats = scheduler.stats()
        finally:
            scheduler.close()

    assert not scheduler.degraded
    assert db.count_records("demo_api") == 6
    assert db.count_records("demo_html") == 6
    source_stats = {item["source"]: item for item in stats["sources"]}
    assert source_stats["demo_api"]["dispatched"] > 0
    assert source_stats["demo_html"]["dispatched"] > 0


def test_degraded_scheduler_still_completes_full_run(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    source = make_source("demo_api")
    with Fetcher(timeout=5.0) as fetcher:
        scheduler = RequestScheduler(
            _scheduler(max_concurrency=2), transport=fetcher
        )
        # 还没提交任何请求就降级：全部走直连串行，任务不中断。
        scheduler.enter_degradation("集成测试：预先降级")
        try:
            summary = run_source_concurrent(
                source,
                db,
                raw_store,
                scheduler,
                fetcher=fetcher,
                limit=4,
            )
        finally:
            scheduler.close()

    assert (summary.ok, summary.failed) == (4, 0)
    assert db.count_records("demo_api") == 4


def test_thread_safe_wrappers_under_contention(
    db: Database,
    raw_store: RawStore,
) -> None:
    safe_db = ThreadSafeDatabase(db)
    safe_raw = ThreadSafeRawStore(raw_store)

    def _touch(_: int) -> None:
        safe_db.init()
        safe_db.count_records()

    threads = [
        threading.Thread(target=_touch, args=(i,)) for i in range(20)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)
    assert all(not thread.is_alive() for thread in threads)
    # 重复包装复用同一个底层实例与同一把锁，不会再引入第二把锁。
    wrapped_again = ThreadSafeDatabase(safe_db)
    assert wrapped_again._db is db
    assert wrapped_again._lock is safe_db._lock
    raw_wrapped = ThreadSafeRawStore(ThreadSafeRawStore(raw_store))
    assert raw_wrapped._lock is raw_store.lock


# ---------------------------------------------------------------- CLI


def test_cli_scheduled_flag_runs(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "sched.db"
    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "3",
            "--scheduled",
            "--max-concurrency",
            "3",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "source=demo_api ok=3" in result.output
    # 并发模式标记走 stdout，不依赖日志 handler 的 stderr 绑定。
    assert "并发调度模式" in result.output

    database = Database(db_path)
    database.init()
    try:
        assert database.count_records("demo_api") == 3
    finally:
        database.close()


def test_cli_default_remains_serial(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = CliRunner().invoke(
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
            str(tmp_path / "serial.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "并发调度模式" not in result.output
    assert "本轮汇总" in result.output


def test_cli_scheduled_with_qps(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "3",
            "--scheduled",
            "--qps",
            "2",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "qps.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "ok=3" in result.output


def test_cli_scheduled_bad_config_exits_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    source_config = write_config([source_payload("demo_api")])
    scheduler_path = tmp_path / "scheduler.yaml"
    scheduler_path.write_text(
        "scheduler:\n  max_concurrency: 0\n", encoding="utf-8"
    )
    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--scheduled",
            "--config",
            str(source_config),
            "--scheduler-config",
            str(scheduler_path),
            "--db",
            str(tmp_path / "bad.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 2
    assert "配置错误" in result.output


def test_load_scheduler_yaml_missing_file_uses_defaults(tmp_path: Path) -> None:
    config = load_scheduler_yaml(tmp_path / "absent.yaml")
    assert config.max_concurrency == 8
    assert config.qps is None


def test_load_scheduler_yaml_full_document(tmp_path: Path) -> None:
    path = tmp_path / "scheduler.yaml"
    path.write_text(
        """
scheduler:
  max_concurrency: 5
  qps: 12.5
  default_source_concurrency: 3
  sources:
    - name: demo_api
      weight: 4
      max_concurrency: 2
      qps: 6
  domain_rules:
    - domain: example.com
      match: suffix
      qps: 8
""",
        encoding="utf-8",
    )
    config = load_scheduler_yaml(path)
    assert config.max_concurrency == 5
    assert config.qps == 12.5
    assert config.effective_source_concurrency("demo_api") == 2
    assert config.effective_source_qps("demo_api") == 6
    assert config.domain_rules[0].domain == "example.com"


def test_source_concurrency_cannot_exceed_global() -> None:
    with pytest.raises(Exception):
        SchedulerConfig(
            max_concurrency=2,
            sources=[{"name": "x", "max_concurrency": 5}],
        )


def test_duplicate_source_and_domain_rules_rejected(tmp_path: Path) -> None:
    path = tmp_path / "scheduler.yaml"
    path.write_text(
        """
scheduler:
  sources:
    - name: x
    - name: x
  domain_rules:
    - domain: a.test
      qps: 1
    - domain: a.test
      qps: 2
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError):
        load_scheduler_yaml(path)


def test_domain_rule_validates_hostname() -> None:
    with pytest.raises(Exception):
        SchedulerConfig(
            domain_rules=[{"domain": "https://bad.example/path", "qps": 1}]
        )


def test_dry_run_scheduled_writes_nothing(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    raw_dir = tmp_path / "raw"
    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "3",
            "--scheduled",
            "--dry-run",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "dry.db"),
            "--raw-dir",
            str(raw_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert not raw_dir.exists()
