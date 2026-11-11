"""单进程轻量调度引擎。

不引入任何第三方调度框架，核心就是一个 200ms 间隔的 tick 循环 + 每任务
守护线程：

- tick 按各源 cron 的下一次触发时刻到点分发，最大固有延迟为一个 tick
  （默认 200ms），满足 1 秒执行误差要求；
- 触发前抢 SQLite 源级租约，抢不到说明上一轮还没结束，本轮自动跳过并
  记录 skipped 指标，杜绝同一来源并发执行；
- 执行在独立守护线程中进行，不阻塞后续 tick，也不影响手动 CLI 采集；
- 全部状态在 SQLite 中，进程重启后 :meth:`SchedulerStore.recover_after_restart`
  自动恢复计划，无需重新配置。
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

from crawl_flow.config import SourceConfig
from crawl_flow.observability.metrics import (
    METRIC_RUNNING_GAUGE,
    METRIC_TRIGGERS_TOTAL,
    METRIC_UP_GAUGE,
    MetricsRegistry,
)
from crawl_flow.scheduler.cron import CronSpec, parse_cron
from crawl_flow.scheduler.models import TRIGGER_MANUAL, TRIGGER_SCHEDULED
from crawl_flow.scheduler.runner import RunRecorder, RunnerFunc
from crawl_flow.scheduler.store import SchedulerStore, utcnow
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

LOGGER_NAME = "crawl_flow.scheduler"
DEFAULT_TICK_SECONDS = 0.2
DEFAULT_LEASE_TTL_SECONDS = 3600
HEARTBEAT_MULTIPLIER = 25  # 每 25 个 tick 续期一次租约（约 5 秒）


@dataclass(frozen=True)
class JobPlan:
    """调度引擎内部使用的单源计划。"""

    source: SourceConfig
    cron: CronSpec


class Scheduler:
    """定时调度引擎，配合 :class:`MetricsServer` 在同一进程内运行。"""

    def __init__(
        self,
        plans: list[JobPlan],
        store: SchedulerStore,
        registry: MetricsRegistry,
        db_path: str,
        raw_dir: str,
        *,
        limit: int | None = None,
        tick_seconds: float = DEFAULT_TICK_SECONDS,
        clock: Callable[[], datetime] = utcnow,
        runner: RunnerFunc | None = None,
        lease_ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
        logger: logging.Logger | None = None,
    ) -> None:
        self._plans = {plan.source.name: plan for plan in plans}
        self._store = store
        self._registry = registry
        self._db_path = db_path
        self._raw_dir = raw_dir
        self._limit = limit
        self._tick_seconds = tick_seconds
        self._clock = clock
        self._runner = runner
        self._lease_ttl = lease_ttl_seconds
        self._log = logger or logging.getLogger(LOGGER_NAME)

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._active_lock = threading.Lock()
        self._active: set[str] = set()
        self._next_fire: dict[str, datetime] = {
            name: plan.cron.next_after(clock()) for name, plan in self._plans.items()
        }

    # ---- 生命周期 ----
    def start(self, *, run_immediately: bool = False) -> None:
        """启动调度线程（非守护线程由调用方进程管理，内部任务线程为守护）。"""

        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("调度器已在运行")
        self._stop_event.clear()
        self._registry.set_gauge(METRIC_UP_GAUGE, 1)
        self._thread = threading.Thread(
            target=self._run_loop,
            name="crawl-flow-scheduler",
            daemon=True,
            kwargs={"run_immediately": run_immediately},
        )
        self._thread.start()
        self._log.info(
            "scheduler started jobs=%d tick_seconds=%.2f run_immediately=%s",
            len(self._plans),
            self._tick_seconds,
            run_immediately,
        )

    def stop(self, timeout: float | None = 10.0) -> None:
        """请求停止并等待 tick 线程退出；正在执行的采集线程自行跑完。"""

        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
        self._registry.set_gauge(METRIC_UP_GAUGE, 0)
        self._log.info("scheduler stopped")

    def is_running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def next_fire_times(self) -> dict[str, datetime]:
        """返回每个源的下一次计划触发时刻（只读快照）。"""

        return dict(self._next_fire)

    def active_sources(self) -> set[str]:
        with self._active_lock:
            return set(self._active)

    def trigger_now(self, source_name: str) -> bool:
        """立即触发一次指定源（不等 cron 时刻），重叠时同样跳过。

        主要用于手动验证与测试；返回 False 表示因上一轮未完成而跳过。
        """

        plan = self._plans.get(source_name)
        if plan is None:
            raise KeyError(f"未知调度源: {source_name}")
        return self._dispatch(plan, scheduled_at=self._clock(), manual_immediate=True)

    # ---- 主循环 ----
    def _run_loop(self, *, run_immediately: bool) -> None:
        if run_immediately:
            for plan in self._plans.values():
                if self._stop_event.is_set():
                    break
                self._dispatch(plan, scheduled_at=self._clock(), manual_immediate=True)

        tick_count = 0
        while not self._stop_event.is_set():
            try:
                self._tick()
            except Exception as exc:  # tick 本身异常不能杀死调度线程
                self._log.error("scheduler tick error: %s", exc, exc_info=True)
            tick_count += 1
            if tick_count % HEARTBEAT_MULTIPLIER == 0:
                try:
                    self._store.renew_leases(ttl_seconds=self._lease_ttl)
                except Exception as exc:
                    self._log.warning("lease renew failed: %s", exc)
            self._stop_event.wait(self._tick_seconds)

    def _tick(self) -> None:
        now = self._clock()
        for name, plan in self._plans.items():
            fire_at = self._next_fire.get(name)
            if fire_at is None or now < fire_at:
                continue
            # 先推进下一次触发时刻，保证即使本轮执行很慢也不会补打历史时刻
            self._next_fire[name] = plan.cron.next_after(now)
            self._dispatch(plan, scheduled_at=fire_at, manual_immediate=False)

    # ---- 分发与执行 ----
    def _dispatch(
        self,
        plan: JobPlan,
        *,
        scheduled_at: datetime,
        manual_immediate: bool,
    ) -> bool:
        source_name = plan.source.name
        with self._active_lock:
            if source_name in self._active:
                self._record_skip(plan, scheduled_at, "上一轮执行尚未结束（进程内）")
                return False

        run_id = self._store.insert_run(
            source_name, TRIGGER_SCHEDULED, scheduled_at=scheduled_at
        )
        if not self._store.acquire_lease(
            source_name, run_id, ttl_seconds=self._lease_ttl
        ):
            # 抢租约失败：登记的 running 行直接转为 skipped，不占用执行槽
            self._store.finish_run(
                run_id,
                status="skipped",
                elapsed_ms=0,
                items_ok=0,
                items_failed=0,
                items_skipped=0,
                error="上一轮执行尚未结束，本轮调度自动跳过",
            )
            self._registry.inc_counter(
                METRIC_TRIGGERS_TOTAL, {"source": source_name, "result": "skipped"}
            )
            self._log.info(
                "scheduled run skipped source=%s scheduled_at=%s reason=lease_busy",
                source_name,
                scheduled_at.isoformat(),
            )
            return False

        with self._active_lock:
            self._active.add(source_name)
        self._registry.set_gauge(
            METRIC_RUNNING_GAUGE, float(len(self._active)), {"source": source_name}
        )
        self._registry.inc_counter(
            METRIC_TRIGGERS_TOTAL, {"source": source_name, "result": "fired"}
        )
        if not manual_immediate:
            self._store.update_last_fired(source_name, scheduled_at)
        self._log.info(
            "scheduled run fired source=%s run_id=%d scheduled_at=%s",
            source_name,
            run_id,
            scheduled_at.isoformat(),
        )

        thread = threading.Thread(
            target=self._run_worker,
            name=f"crawl-flow-job-{source_name}",
            args=(plan, run_id, scheduled_at),
            daemon=True,
        )
        thread.start()
        return True

    def _run_worker(
        self,
        plan: JobPlan,
        run_id: int,
        scheduled_at: datetime,
    ) -> None:
        source_name = plan.source.name
        # 每个执行线程使用独立的 Database / RawStore / 连接，避免跨线程共享
        database = Database(self._db_path)
        raw_store = RawStore(self._raw_dir)
        try:
            database.init()
            recorder = RunRecorder(
                self._store,
                self._registry,
                plan.source,
                scheduled_at=scheduled_at,
                limit=self._limit,
                logger=self._log,
                clock=self._clock,
            )
            recorder.run_id = run_id  # 复用 _dispatch 已登记的历史行与租约
            recorder.complete(database, raw_store, runner=self._runner)
        except Exception as exc:  # 守护线程兜底，绝不让异常静默杀死线程
            self._log.error(
                "scheduled worker crashed source=%s run_id=%s error=%s",
                source_name,
                run_id,
                exc,
                exc_info=True,
            )
        finally:
            database.close()
            self._store.release_lease(source_name)
            with self._active_lock:
                self._active.discard(source_name)
                remaining = len(self._active)
            self._registry.set_gauge(
                METRIC_RUNNING_GAUGE, float(remaining), {"source": source_name}
            )

    def _record_skip(
        self, plan: JobPlan, scheduled_at: datetime, reason: str
    ) -> None:
        self._store.insert_skipped_run(
            plan.source.name, scheduled_at=scheduled_at, reason=reason
        )
        self._registry.inc_counter(
            METRIC_TRIGGERS_TOTAL, {"source": plan.source.name, "result": "skipped"}
        )
        self._log.info(
            "scheduled run skipped source=%s reason=%s",
            plan.source.name,
            reason,
        )


def build_job_plans(
    sources: list[SourceConfig],
) -> list[JobPlan]:
    """从带 ``schedule`` 配置的源构造调度计划，enabled=false 的源不参与。"""

    plans: list[JobPlan] = []
    for source in sources:
        if not source.enabled or source.schedule is None:
            continue
        plans.append(JobPlan(source=source, cron=parse_cron(source.schedule.cron)))
    return plans


def real_sleep(seconds: float) -> None:
    """测试可替换的 sleep 入口。"""

    time.sleep(seconds)
