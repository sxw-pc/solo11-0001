"""采集执行包装：把 ``pipeline.run_source`` 与指标记录、执行历史打通。

手动 CLI 与定时引擎共用本模块的执行入口，保证两条路径写入同一套指标，
互不冲突（计数器在 SQLite 中原子自增）。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from crawl_flow.config import SourceConfig
from crawl_flow.observability.metrics import (
    METRIC_DURATION_MS,
    METRIC_ITEMS_TOTAL,
    METRIC_RUNS_TOTAL,
    MetricsRegistry,
)
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.scheduler.models import (
    STATUS_FAILED,
    STATUS_OK,
    TRIGGER_MANUAL,
    TRIGGER_SCHEDULED,
)
from crawl_flow.scheduler.store import SchedulerStore, utcnow
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

LOGGER_NAME = "crawl_flow.scheduler.runner"


@dataclass
class CollectionResult:
    """一次采集的完整结果，供调度引擎更新历史与释放租约。"""

    run_id: int | None
    status: str
    elapsed_ms: int
    summary: RunSummary | None
    error: str | None = None


# 可注入的执行函数签名：引擎用它隔离测试里的真实网络采集
RunnerFunc = Callable[..., RunSummary]


def run_collection(
    source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    registry: MetricsRegistry,
    *,
    trigger_type: str = TRIGGER_MANUAL,
    limit: int | None = None,
    dry_run: bool = False,
    record_metrics: bool = True,
    logger: logging.Logger | None = None,
    runner: RunnerFunc | None = None,
) -> RunSummary:
    """执行一轮采集并记录指标。

    成功返回 :class:`RunSummary`；管道抛出的异常（FetchError / StoreError 等）
    在指标落库后原样向上抛，由 CLI 决定退出码。``dry_run`` 只控制是否写
    raw 与数据库；``record_metrics=False`` 只跳过指标累加，不影响采集本身。
    """

    log = logger or logging.getLogger(LOGGER_NAME)
    active_runner = runner or run_source
    labels = {"source": source.name, "trigger": trigger_type}
    started = time.perf_counter()
    try:
        summary = active_runner(
            source,
            db,
            raw_store,
            limit=limit,
            dry_run=dry_run,
            logger=log,
        )
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        if record_metrics and not dry_run:
            registry.inc_counter(
                METRIC_RUNS_TOTAL, {**labels, "status": STATUS_FAILED}
            )
            registry.observe_histogram(METRIC_DURATION_MS, elapsed_ms, labels)
        raise
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    if record_metrics and not dry_run:
        registry.inc_counter(METRIC_RUNS_TOTAL, {**labels, "status": STATUS_OK})
        registry.inc_counter(
            METRIC_ITEMS_TOTAL, {**labels, "status": STATUS_OK}, summary.ok
        )
        registry.inc_counter(
            METRIC_ITEMS_TOTAL, {**labels, "status": STATUS_FAILED}, summary.failed
        )
        registry.observe_histogram(METRIC_DURATION_MS, elapsed_ms, labels)
    return summary


class RunRecorder:
    """一次定时执行的生命周期管理：登记历史 → 采集 → 收尾写回。"""

    def __init__(
        self,
        store: SchedulerStore,
        registry: MetricsRegistry,
        source: SourceConfig,
        *,
        scheduled_at: datetime,
        limit: int | None = None,
        logger: logging.Logger | None = None,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._store = store
        self._registry = registry
        self._source = source
        self._scheduled_at = scheduled_at
        self._limit = limit
        self._log = logger or logging.getLogger(LOGGER_NAME)
        self._clock = clock
        self.run_id: int | None = None
        self._started = time.perf_counter()

    def start(self) -> int:
        self.run_id = self._store.insert_run(
            self._source.name,
            TRIGGER_SCHEDULED,
            scheduled_at=self._scheduled_at,
        )
        return self.run_id

    def complete(
        self,
        db: Database,
        raw_store: RawStore,
        *,
        runner: RunnerFunc | None = None,
    ) -> CollectionResult:
        """执行采集并把结果写回历史；任何异常都转成 failed 结果不外抛。"""

        elapsed_ms = int((time.perf_counter() - self._started) * 1000)
        try:
            summary = run_collection(
                self._source,
                db,
                raw_store,
                self._registry,
                trigger_type=TRIGGER_SCHEDULED,
                limit=self._limit,
                logger=self._log,
                runner=runner,
            )
        except Exception as exc:  # 调度线程不能因单个任务异常退出
            elapsed_ms = int((time.perf_counter() - self._started) * 1000)
            error = f"{exc.__class__.__name__}: {exc}"[:500]
            self._store.finish_run(
                self._run_id(),
                status=STATUS_FAILED,
                elapsed_ms=elapsed_ms,
                items_ok=0,
                items_failed=0,
                items_skipped=0,
                error=error,
            )
            self._log.warning(
                "scheduled run failed source=%s run_id=%s error=%s",
                self._source.name,
                self.run_id,
                error,
            )
            return CollectionResult(
                run_id=self.run_id,
                status=STATUS_FAILED,
                elapsed_ms=elapsed_ms,
                summary=None,
                error=error,
            )
        elapsed_ms = int((time.perf_counter() - self._started) * 1000)
        self._store.finish_run(
            self._run_id(),
            status=STATUS_OK,
            elapsed_ms=elapsed_ms,
            items_ok=summary.ok,
            items_failed=summary.failed,
            items_skipped=summary.skipped,
        )
        return CollectionResult(
            run_id=self.run_id,
            status=STATUS_OK,
            elapsed_ms=elapsed_ms,
            summary=summary,
        )

    def _run_id(self) -> int:
        if self.run_id is None:
            raise RuntimeError("RunRecorder.start() 未调用")
        return self.run_id
