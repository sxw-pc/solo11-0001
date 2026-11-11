"""单进程定时调度器：按 cron 到点触发采集，同源防重入，重叠自动跳过。

设计要点：

- 调度循环完全在后台线程内运行，不依赖任何外部调度服务或重型框架；
- 每轮循环的休眠上限为 ``_MAX_SLEEP_SECONDS``（0.5 秒），到点触发误差
  远小于 1 秒；
- 每个源一把非阻塞锁：上一次执行未结束时，本次调度直接跳过并记录到
  ``job_runs``（``status=skipped``），不会产生并发执行；
- 定时计划来自 ``sources.yaml``，进程重启后重新加载配置即自动恢复，
  无需手动重新配置；停机期间错过的轮次不补跑（标准 cron 语义）；
- 执行通过线程池异步进行，长任务不会阻塞调度循环本身。
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from crawl_flow.config import SourceConfig
from crawl_flow.errors import StoreError
from crawl_flow.models import JobRun
from crawl_flow.scheduler.cron import CronSchedule
from crawl_flow.scheduler.runner import execute_and_record
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

# 调度循环单次休眠上限：保证到点后最多晚 0.5 秒触发（要求误差 < 1 秒）。
_MAX_SLEEP_SECONDS = 0.5
_MIN_SLEEP_SECONDS = 0.05
_IDLE_SLEEP_SECONDS = 1.0

RunCallable = Callable[[SourceConfig], None]


@dataclass
class ScheduledJob:
    """一个源的定时计划及其下次触发时间。"""

    source: SourceConfig
    cron: CronSchedule
    next_fire: datetime | None


class SchedulerService:
    """常驻调度器：管理所有源的 cron 计划并在到点时触发采集。

    ``runner`` 为单源执行函数，默认是「采集 + 记录指标」；测试可注入假实现。
    ``sync=True`` 时 :meth:`tick` 同步执行任务（用于确定性测试）。
    """

    def __init__(
        self,
        sources: list[SourceConfig],
        *,
        db_path: str | Path,
        raw_dir: str | Path,
        limit: int | None = None,
        runner: RunCallable | None = None,
        sync: bool = False,
        max_workers: int = 4,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger("crawl_flow.scheduler")
        self._limit = limit
        self._sync = sync
        self._db = Database(db_path)
        self._db.init()
        self._raw_store = RawStore(raw_dir)
        self._runner: RunCallable = runner or self._default_runner

        self._jobs: list[ScheduledJob] = []
        self._locks: dict[str, threading.Lock] = {}
        self._running: dict[str, bool] = {}
        now = datetime.now().astimezone()
        for source in sources:
            if source.schedule is None:
                continue
            if not source.enabled:
                self._log.warning(
                    "source=%s 配置了 schedule 但 enabled=false，调度器将忽略该源",
                    source.name,
                )
                continue
            cron = CronSchedule(source.schedule)
            self._jobs.append(ScheduledJob(source=source, cron=cron, next_fire=cron.next_after(now)))
            self._locks[source.name] = threading.Lock()
            self._running[source.name] = False

        self._executor = ThreadPoolExecutor(
            max_workers=max(1, max_workers), thread_name_prefix="crawl-job"
        )
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------ 查询

    def jobs_snapshot(self) -> list[dict[str, object]]:
        """当前任务快照：源、计划、下次触发时间、是否在执行。"""

        return [
            {
                "source": job.source.name,
                "schedule": job.cron.expression,
                "next_fire": job.next_fire,
                "running": self._running.get(job.source.name, False),
            }
            for job in self._jobs
        ]

    @property
    def job_count(self) -> int:
        return len(self._jobs)

    # -------------------------------------------------------------- 生命周期

    def start(self) -> None:
        """启动后台调度线程（幂等）。"""

        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="crawl-scheduler", daemon=True
        )
        self._thread.start()
        self._log.info("scheduler started jobs=%d", len(self._jobs))

    def stop(self) -> None:
        """停止调度线程并等待在途任务结束。"""

        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
            self._thread = None
        self._executor.shutdown(wait=True)
        self._db.close()
        self._log.info("scheduler stopped")

    def __enter__(self) -> "SchedulerService":
        self.start()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.stop()

    # ---------------------------------------------------------------- 调度

    def tick(self, now: datetime | None = None) -> None:
        """检查所有任务，触发到点的任务。

        从循环线程与测试中都会调用；``now`` 可注入以便确定性测试。
        """

        moment = now or datetime.now().astimezone()
        for job in list(self._jobs):
            if job.next_fire is None or moment < job.next_fire:
                continue
            # 先推进下次触发时间（基于当前时刻，停机期间不补跑），再触发。
            job.next_fire = job.cron.next_after(moment)
            if job.next_fire is None:
                self._log.warning(
                    "source=%s 的 cron 表达式未来不再有触发时间，任务停止调度",
                    job.source.name,
                )
            self._trigger(job, moment)

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception:  # 调度循环不允许崩溃
                self._log.exception("scheduler tick failed")
            self._stop_event.wait(self._next_sleep())

    def _next_sleep(self) -> float:
        """距最近触发点的秒数，封顶 0.5 秒以保证触发误差 < 1 秒。"""

        now = datetime.now().astimezone()
        delays = [
            (job.next_fire - now).total_seconds()
            for job in self._jobs
            if job.next_fire is not None
        ]
        if not delays:
            return _IDLE_SLEEP_SECONDS
        return max(_MIN_SLEEP_SECONDS, min(min(delays), _MAX_SLEEP_SECONDS))

    def _trigger(self, job: ScheduledJob, moment: datetime) -> None:
        name = job.source.name
        lock = self._locks[name]
        if not lock.acquire(blocking=False):
            self._log.info("source=%s 上一次执行未完成，本次调度跳过", name)
            self._record_skip(name)
            return
        if self._sync:
            try:
                self._run(job)
            finally:
                lock.release()
        else:
            self._executor.submit(self._run_and_release, job, lock)

    def _run_and_release(self, job: ScheduledJob, lock: threading.Lock) -> None:
        try:
            self._run(job)
        finally:
            lock.release()

    def _run(self, job: ScheduledJob) -> None:
        name = job.source.name
        self._running[name] = True
        self._log.info("scheduled run start source=%s", name)
        try:
            self._runner(job.source)
        except Exception:
            # 默认 runner 已把失败写入 job_runs；这里只保证调度器不崩。
            self._log.exception("scheduled run failed source=%s", name)
        finally:
            self._running[name] = False

    def _default_runner(self, source: SourceConfig) -> None:
        execute_and_record(
            source,
            self._db,
            self._raw_store,
            trigger="scheduled",
            limit=self._limit,
            logger=self._log,
        )

    def _record_skip(self, source_name: str) -> None:
        now_utc = datetime.now(timezone.utc)
        try:
            self._db.insert_job_run(
                JobRun(
                    source=source_name,
                    trigger="scheduled",
                    status="skipped",
                    elapsed_ms=0,
                    started_at=now_utc,
                    finished_at=now_utc,
                    error="上一次执行未完成，本次调度自动跳过",
                )
            )
        except StoreError as exc:
            self._log.warning("job_runs 记录跳过失败 source=%s error=%s", source_name, exc)
