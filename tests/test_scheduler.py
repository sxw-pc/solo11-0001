"""调度器：到点触发、防重入跳过、重启恢复、执行不阻塞循环。"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from crawl_flow.config import SourceConfig
from crawl_flow.scheduler.service import SchedulerService


def _source(name: str = "demo", schedule: str | None = "* * * * *", enabled: bool = True) -> SourceConfig:
    return SourceConfig(
        name=name,
        type="api",
        base_url="http://127.0.0.1:1",
        enabled=enabled,
        schedule=schedule,
    )


def _make_service(
    tmp_path: Path,
    sources: list[SourceConfig],
    *,
    runner=None,
    sync: bool = True,
) -> SchedulerService:
    return SchedulerService(
        sources,
        db_path=tmp_path / "sched.db",
        raw_dir=tmp_path / "raw",
        runner=runner or (lambda source: None),
        sync=sync,
    )


def test_jobs_built_only_from_enabled_sources_with_schedule(tmp_path: Path) -> None:
    service = _make_service(
        tmp_path,
        [
            _source("with_schedule"),
            _source("no_schedule", schedule=None),
            _source("disabled", schedule="*/5 * * * *", enabled=False),
        ],
    )
    try:
        assert service.job_count == 1
        snapshot = service.jobs_snapshot()
        assert snapshot[0]["source"] == "with_schedule"
        assert snapshot[0]["schedule"] == "* * * * *"
        assert snapshot[0]["running"] is False
        assert snapshot[0]["next_fire"] is not None
    finally:
        service.stop()


def test_tick_fires_due_job_and_advances_next_fire(tmp_path: Path) -> None:
    fired: list[str] = []
    service = _make_service(tmp_path, [_source()], runner=lambda source: fired.append(source.name))
    try:
        past = datetime.now().astimezone() - timedelta(minutes=1)
        service._jobs[0].next_fire = past  # 直接摆到到点状态
        service.tick()
        assert fired == ["demo"]
        # 触发后 next_fire 已推进到未来
        assert service._jobs[0].next_fire > datetime.now().astimezone()
    finally:
        service.stop()


def test_tick_skips_job_not_due(tmp_path: Path) -> None:
    fired: list[str] = []
    service = _make_service(tmp_path, [_source()], runner=lambda source: fired.append(source.name))
    try:
        service.tick()  # next_fire 在未来，不应触发
        assert fired == []
    finally:
        service.stop()


def test_overlap_skip_recorded(tmp_path: Path) -> None:
    """上一次执行未完成时，下一次调度自动跳过并记录到 job_runs。"""

    entered = threading.Event()
    release = threading.Event()

    def blocking_runner(source) -> None:
        entered.set()
        release.wait(timeout=10)

    service = _make_service(tmp_path, [_source()], runner=blocking_runner, sync=False)
    try:
        due = datetime.now().astimezone() - timedelta(seconds=1)
        service._jobs[0].next_fire = due
        service.tick()
        assert entered.wait(timeout=5), "第一次调度应开始执行"

        # 任务仍在执行：再次到点触发 → 跳过
        service._jobs[0].next_fire = datetime.now().astimezone() - timedelta(seconds=1)
        service.tick()

        runs = service._db.recent_job_runs(source="demo")
        assert len(runs) == 1
        assert runs[0]["status"] == "skipped"
        assert runs[0]["trigger"] == "scheduled"
    finally:
        release.set()
        service.stop()


def test_runner_exception_does_not_kill_scheduler(tmp_path: Path) -> None:
    calls: list[str] = []

    def flaky_runner(source) -> None:
        calls.append(source.name)
        raise RuntimeError("boom")

    service = _make_service(tmp_path, [_source()], runner=flaky_runner)
    try:
        service._jobs[0].next_fire = datetime.now().astimezone() - timedelta(seconds=1)
        service.tick()  # 异常被吞掉，不向上抛
        assert calls == ["demo"]
        # 调度器仍可继续触发
        service._jobs[0].next_fire = datetime.now().astimezone() - timedelta(seconds=1)
        service.tick()
        assert calls == ["demo", "demo"]
    finally:
        service.stop()


def test_restart_recovers_schedule_and_history(tmp_path: Path) -> None:
    """进程重启（重建 Service）后定时计划自动恢复，历史记录保留。"""

    sources = [_source("demo", "*/5 * * * *")]
    first = _make_service(tmp_path, sources)
    first._db.insert_job_run(
        _job_run("demo")
    )
    first.stop()

    second = _make_service(tmp_path, sources)
    try:
        assert second.job_count == 1
        assert second.jobs_snapshot()[0]["schedule"] == "*/5 * * * *"
        runs = second._db.recent_job_runs()
        assert len(runs) == 1 and runs[0]["source"] == "demo"
    finally:
        second.stop()


def test_background_loop_fires_within_one_second(tmp_path: Path) -> None:
    """端到端计时：后台循环在到点后 1 秒内触发任务。"""

    fired = threading.Event()
    service = _make_service(
        tmp_path, [_source()], runner=lambda source: fired.set(), sync=False
    )
    try:
        service._jobs[0].next_fire = datetime.now().astimezone() + timedelta(milliseconds=300)
        started = time.monotonic()
        service.start()
        assert fired.wait(timeout=2), "后台调度循环没有在 2 秒内触发到点任务"
        delay = time.monotonic() - started
        assert delay < 1.3, f"触发延迟 {delay:.2f}s 超出 1 秒误差要求（含 0.3s 预设等待）"
    finally:
        service.stop()


def test_next_sleep_capped(tmp_path: Path) -> None:
    service = _make_service(tmp_path, [_source("a"), _source("b", "0 3 * * *")])
    try:
        sleep = service._next_sleep()
        assert 0 < sleep <= 0.5
    finally:
        service.stop()


def test_scheduled_run_end_to_end(tmp_path: Path, mock_base_url: str) -> None:
    """默认 runner 走真实采集：tick 后记录入库、指标可查。"""

    source = SourceConfig(
        name="demo_api",
        type="api",
        base_url=mock_base_url,
        page_size=5,
        schedule="* * * * *",
    )
    service = SchedulerService(
        [source],
        db_path=tmp_path / "e2e.db",
        raw_dir=tmp_path / "raw",
        limit=3,
        sync=True,
    )
    try:
        service._jobs[0].next_fire = datetime.now().astimezone() - timedelta(seconds=1)
        service.tick()

        assert service._db.count_records("demo_api") == 3
        runs = service._db.recent_job_runs()
        assert len(runs) == 1
        assert runs[0]["trigger"] == "scheduled"
        assert runs[0]["status"] == "succeeded"
        assert runs[0]["ok_count"] == 3
    finally:
        service.stop()


def _job_run(source: str):
    from crawl_flow.models import JobRun

    now = datetime.now()
    return JobRun(
        source=source,
        trigger="scheduled",
        status="succeeded",
        ok_count=3,
        failed_count=0,
        skipped_count=0,
        elapsed_ms=120,
        started_at=now,
        finished_at=now,
    )
