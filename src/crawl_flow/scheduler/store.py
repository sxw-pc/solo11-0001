"""调度状态存储：定时计划、执行租约、运行历史与统计查询。

与采集库共用同一个 SQLite 文件（WAL 模式），调度器长驻进程与手动 CLI
进程可以并发访问。所有写操作都是短事务，异常统一转 :class:`StoreError`。
"""

from __future__ import annotations

import os
import socket
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from crawl_flow.errors import StoreError
from crawl_flow.scheduler.models import (
    STATUS_INTERRUPTED,
    STATUS_RUNNING,
    STATUS_SKIPPED,
    JobLeaseRow,
    JobRunRow,
    MetricCounterRow,
    MetricHistogramBucketRow,
    MetricHistogramSumRow,
    ScheduledJobRow,
)
from crawl_flow.storage.db import Database

DEFAULT_LEASE_TTL_SECONDS = 3600
HISTORY_LIMIT_DEFAULT = 20
MAX_ERROR_LENGTH = 500


def utcnow() -> datetime:
    """统一的 naive UTC 时间入口，测试可注入假时钟替换。"""

    return datetime.now(timezone.utc).replace(tzinfo=None)


def current_holder() -> str:
    """当前调度器实例标识：主机名 + 进程号。"""

    return f"{socket.gethostname()}:{os.getpid()}"


@dataclass(frozen=True)
class ScheduledJob:
    """一条定时计划的只读视图。"""

    source: str
    cron_expr: str
    enabled: bool
    last_fired_at: datetime | None


@dataclass(frozen=True)
class JobRun:
    """一条执行历史的只读视图。"""

    id: int
    source: str
    trigger_type: str
    status: str
    scheduled_at: datetime | None
    started_at: datetime
    finished_at: datetime | None
    elapsed_ms: int | None
    items_ok: int
    items_failed: int
    items_skipped: int
    error: str | None


@dataclass(frozen=True)
class JobStats:
    """单个源的执行统计。"""

    source: str
    total_runs: int
    ok_runs: int
    failed_runs: int
    skipped_runs: int
    interrupted_runs: int
    success_rate: float | None
    avg_elapsed_ms: int | None
    last_run_at: datetime | None


class SchedulerStore:
    """调度与指标相关表的持久化封装。"""

    def __init__(
        self,
        database: Database,
        *,
        clock: Callable[[], datetime] = utcnow,
        holder: str | None = None,
    ) -> None:
        self.database = database
        self._clock = clock
        self.holder = holder or current_holder()

    def init(self) -> None:
        """创建调度 / 指标相关表（幂等）。"""

        try:
            from crawl_flow.models import Base

            Base.metadata.create_all(self.database.engine)
        except SQLAlchemyError as exc:
            raise StoreError(f"调度模块建表失败: {exc}") from exc

    # ---- 定时计划 ----
    def sync_jobs(self, plans: dict[str, str]) -> None:
        """按 YAML 配置全量同步定时计划。

        ``plans`` 为 ``{source: cron 表达式}``，仅包含仍配置了 schedule 的
        源；配置里被移除的源保留历史行但标记 disabled，执行历史不删除。
        """

        now = self._clock()
        try:
            with self.database.session() as session:
                existing = {
                    row.source: row
                    for row in session.execute(select(ScheduledJobRow)).scalars()
                }
                for source, cron_expr in plans.items():
                    if source in existing:
                        row = existing[source]
                        row.cron_expr = cron_expr
                        row.enabled = True
                        row.updated_at = now
                    else:
                        session.add(
                            ScheduledJobRow(
                                source=source,
                                cron_expr=cron_expr,
                                enabled=True,
                                last_fired_at=None,
                                updated_at=now,
                            )
                        )
                configured = set(plans)
                for source, row in existing.items():
                    if source not in configured:
                        row.enabled = False
                        row.updated_at = now
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"定时计划同步失败: {exc}") from exc

    def list_jobs(self) -> list[ScheduledJob]:
        with self.database.session() as session:
            rows = session.execute(
                select(ScheduledJobRow).order_by(ScheduledJobRow.source)
            ).scalars()
            return [
                ScheduledJob(
                    source=row.source,
                    cron_expr=row.cron_expr,
                    enabled=bool(row.enabled),
                    last_fired_at=row.last_fired_at,
                )
                for row in rows
            ]

    def update_last_fired(self, source: str, moment: datetime) -> None:
        try:
            with self.database.session() as session:
                session.execute(
                    update(ScheduledJobRow)
                    .where(ScheduledJobRow.source == source)
                    .values(last_fired_at=moment, updated_at=self._clock())
                )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"last_fired 更新失败 source={source}: {exc}") from exc

    # ---- 执行租约 ----
    def acquire_lease(
        self,
        source: str,
        run_id: int,
        *,
        ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS,
    ) -> bool:
        """尝试插入源级租约；已存在（同源自执行未完成）时返回 False。"""

        now = self._clock()
        try:
            with self.database.session() as session:
                session.add(
                    JobLeaseRow(
                        source=source,
                        run_id=run_id,
                        holder=self.holder,
                        acquired_at=now,
                        expires_at=now + timedelta(seconds=ttl_seconds),
                    )
                )
                session.commit()
            return True
        except SQLAlchemyError:
            # 主键冲突说明租约已被占用；其他锁问题同样按“未抢到”处理
            return False

    def renew_leases(self, *, ttl_seconds: int = DEFAULT_LEASE_TTL_SECONDS) -> None:
        """续期本进程持有的全部租约，防止长任务超过 TTL 被误判。"""

        now = self._clock()
        try:
            with self.database.session() as session:
                session.execute(
                    update(JobLeaseRow)
                    .where(JobLeaseRow.holder == self.holder)
                    .values(expires_at=now + timedelta(seconds=ttl_seconds))
                )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"租约续期失败: {exc}") from exc

    def release_lease(self, source: str) -> None:
        try:
            with self.database.session() as session:
                session.execute(
                    delete(JobLeaseRow).where(
                        JobLeaseRow.source == source,
                        JobLeaseRow.holder == self.holder,
                    )
                )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"租约释放失败 source={source}: {exc}") from exc

    def list_leased_sources(self) -> set[str]:
        with self.database.session() as session:
            return {
                row.source
                for row in session.execute(select(JobLeaseRow.source)).scalars()
            }

    # ---- 执行历史 ----
    def insert_run(
        self,
        source: str,
        trigger_type: str,
        *,
        scheduled_at: datetime | None = None,
    ) -> int:
        """登记一条 running 历史，返回自增 run_id。"""

        try:
            with self.database.session() as session:
                row = JobRunRow(
                    source=source,
                    trigger_type=trigger_type,
                    status=STATUS_RUNNING,
                    scheduled_at=scheduled_at,
                    started_at=self._clock(),
                    finished_at=None,
                    elapsed_ms=None,
                    items_ok=0,
                    items_failed=0,
                    items_skipped=0,
                    error=None,
                )
                session.add(row)
                session.commit()
                return int(row.id)
        except SQLAlchemyError as exc:
            raise StoreError(f"执行历史写入失败 source={source}: {exc}") from exc

    def insert_skipped_run(
        self,
        source: str,
        *,
        scheduled_at: datetime,
        reason: str,
    ) -> int:
        """登记一条因重叠而跳过的调度记录（瞬时完成）。"""

        now = self._clock()
        try:
            with self.database.session() as session:
                row = JobRunRow(
                    source=source,
                    trigger_type="scheduled",
                    status=STATUS_SKIPPED,
                    scheduled_at=scheduled_at,
                    started_at=now,
                    finished_at=now,
                    elapsed_ms=0,
                    error=reason[:MAX_ERROR_LENGTH],
                )
                session.add(row)
                session.commit()
                return int(row.id)
        except SQLAlchemyError as exc:
            raise StoreError(f"跳过记录写入失败 source={source}: {exc}") from exc

    def finish_run(
        self,
        run_id: int,
        *,
        status: str,
        elapsed_ms: int,
        items_ok: int,
        items_failed: int,
        items_skipped: int,
        error: str | None = None,
    ) -> None:
        try:
            with self.database.session() as session:
                session.execute(
                    update(JobRunRow)
                    .where(JobRunRow.id == run_id)
                    .values(
                        status=status,
                        finished_at=self._clock(),
                        elapsed_ms=elapsed_ms,
                        items_ok=items_ok,
                        items_failed=items_failed,
                        items_skipped=items_skipped,
                        error=(error or None) and error.strip()[:MAX_ERROR_LENGTH],
                    )
                )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"执行历史更新失败 run_id={run_id}: {exc}") from exc

    def list_runs(
        self,
        *,
        source: str | None = None,
        limit: int = HISTORY_LIMIT_DEFAULT,
        trigger_type: str | None = None,
    ) -> list[JobRun]:
        statement = select(JobRunRow).order_by(JobRunRow.id.desc()).limit(max(1, limit))
        if source is not None:
            statement = statement.where(JobRunRow.source == source)
        if trigger_type is not None:
            statement = statement.where(JobRunRow.trigger_type == trigger_type)
        with self.database.session() as session:
            rows = session.execute(statement).scalars()
            return [
                JobRun(
                    id=row.id,
                    source=row.source,
                    trigger_type=row.trigger_type,
                    status=row.status,
                    scheduled_at=row.scheduled_at,
                    started_at=row.started_at,
                    finished_at=row.finished_at,
                    elapsed_ms=row.elapsed_ms,
                    items_ok=row.items_ok,
                    items_failed=row.items_failed,
                    items_skipped=row.items_skipped,
                    error=row.error,
                )
                for row in rows
            ]

    def stats(self) -> list[JobStats]:
        """按源汇总执行次数、成功率与平均耗时。"""

        with self.database.session() as session:
            grouped = session.execute(
                select(
                    JobRunRow.source,
                    JobRunRow.status,
                    func.count(),
                    func.avg(JobRunRow.elapsed_ms),
                    func.max(JobRunRow.started_at),
                ).group_by(JobRunRow.source, JobRunRow.status)
            ).all()

        aggregated: dict[str, dict[str, Any]] = {}

        def bucket(source: str) -> dict[str, Any]:
            return aggregated.setdefault(
                source,
                {
                    "source": source,
                    "ok": 0,
                    "failed": 0,
                    "skipped": 0,
                    "interrupted": 0,
                    "elapsed_values": [],
                    "last_run_at": None,
                },
            )

        for source, status, count, avg_elapsed, last_run in grouped:
            entry = bucket(source)
            entry[status if status in ("ok", "failed", "skipped", "interrupted") else "failed"] = (
                entry.get(status, 0) + int(count)
            )
            if status in ("ok", "failed") and avg_elapsed is not None:
                # 组内平均值按组内条数加权还原总耗时
                entry["elapsed_values"].append((float(avg_elapsed), int(count)))
            if last_run is not None and (
                entry["last_run_at"] is None or last_run > entry["last_run_at"]
            ):
                entry["last_run_at"] = last_run

        result: list[JobStats] = []
        for source in sorted(aggregated):
            entry = aggregated[source]
            completed = entry["ok"] + entry["failed"]
            success_rate = round(entry["ok"] / completed, 4) if completed else None
            if entry["elapsed_values"]:
                total_weight = sum(weight for _, weight in entry["elapsed_values"])
                avg_ms = int(
                    sum(value * weight for value, weight in entry["elapsed_values"])
                    / total_weight
                )
            else:
                avg_ms = None
            total_runs = (
                entry["ok"]
                + entry["failed"]
                + entry["skipped"]
                + entry["interrupted"]
            )
            result.append(
                JobStats(
                    source=source,
                    total_runs=total_runs,
                    ok_runs=entry["ok"],
                    failed_runs=entry["failed"],
                    skipped_runs=entry["skipped"],
                    interrupted_runs=entry["interrupted"],
                    success_rate=success_rate,
                    avg_elapsed_ms=avg_ms,
                    last_run_at=entry["last_run_at"],
                )
            )
        return result

    # ---- 异常恢复 ----
    def recover_after_restart(self) -> int:
        """调度器启动时调用：清理残留租约并把 running 标记为 interrupted。

        返回被判定为中断的执行数量。last_fired_at 从最近一条定时历史重建，
        保证重启后不会把重启前已触发的分钟再触发一次。
        """

        now = self._clock()
        try:
            with self.database.session() as session:
                session.execute(delete(JobLeaseRow))
                interrupted = session.execute(
                    update(JobRunRow)
                    .where(JobRunRow.status == STATUS_RUNNING)
                    .values(
                        status=STATUS_INTERRUPTED,
                        finished_at=now,
                        error="调度器重启，上一次执行未正常结束",
                    )
                ).rowcount
                latest_rows = session.execute(
                    select(
                        JobRunRow.source,
                        func.max(JobRunRow.scheduled_at),
                    )
                    .where(JobRunRow.trigger_type == "scheduled")
                    .group_by(JobRunRow.source)
                ).all()
                for source, last_scheduled in latest_rows:
                    if last_scheduled is not None:
                        session.execute(
                            update(ScheduledJobRow)
                            .where(ScheduledJobRow.source == source)
                            .values(last_fired_at=last_scheduled)
                        )
                session.commit()
                return int(interrupted or 0)
        except SQLAlchemyError as exc:
            raise StoreError(f"调度器恢复失败: {exc}") from exc

    # ---- 指标表清理（测试辅助）----
    def reset_metrics(self) -> None:
        with self.database.session() as session:
            session.execute(delete(MetricCounterRow))
            session.execute(delete(MetricHistogramBucketRow))
            session.execute(delete(MetricHistogramSumRow))
            session.commit()
