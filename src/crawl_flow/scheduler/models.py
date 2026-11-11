"""调度与指标模块的 SQLAlchemy 表定义。

所有状态都落在与采集库同一个 SQLite 文件中，进程异常重启后调度计划与
历史指标自动恢复，不依赖任何外部服务。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Index, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from crawl_flow.models import Base

# 触发方式与运行状态使用短字符串常量，避免散落魔法值
TRIGGER_SCHEDULED = "scheduled"
TRIGGER_MANUAL = "manual"

STATUS_RUNNING = "running"
STATUS_OK = "ok"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"
STATUS_INTERRUPTED = "interrupted"

TERMINAL_STATUSES = (STATUS_OK, STATUS_FAILED, STATUS_SKIPPED, STATUS_INTERRUPTED)


class ScheduledJobRow(Base):
    """``scheduled_jobs``：每个采集源的定时计划（从 YAML 同步）。"""

    __tablename__ = "scheduled_jobs"

    source: Mapped[str] = mapped_column(String(128), primary_key=True)
    cron_expr: Mapped[str] = mapped_column(String(128), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    # 上次触发的分钟（naive UTC），防止同一分钟重复触发；重启后从历史重建
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class JobRunRow(Base):
    """``job_runs``：每次执行（定时 / 手动）的一条历史记录。"""

    __tablename__ = "job_runs"
    __table_args__ = (
        Index("ix_job_runs_source_started", "source", "started_at"),
        Index("ix_job_runs_trigger", "trigger_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    trigger_type: Mapped[str] = mapped_column(String(16), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    elapsed_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    items_ok: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    items_skipped: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)


class JobLeaseRow(Base):
    """``job_leases``：源级执行租约，保证同一来源同时只有一个执行。

    租约由调度器在触发前插入（主键冲突即跳过本轮），执行结束删除；
    调度器启动时统一清理残留租约，崩溃不会造成永久死锁。
    """

    __tablename__ = "job_leases"

    source: Mapped[str] = mapped_column(String(128), primary_key=True)
    run_id: Mapped[int] = mapped_column(Integer, nullable=False)
    holder: Mapped[str] = mapped_column(String(64), nullable=False)
    acquired_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class MetricCounterRow(Base):
    """``metric_counters``：单调递增计数器（采集总量、失败量、调度次数等）。"""

    __tablename__ = "metric_counters"
    __table_args__ = (UniqueConstraint("metric", "labels", name="uq_metric_counter_series"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    metric: Mapped[str] = mapped_column(String(128), nullable=False)
    # 标签按 key 排序后 JSON 序列化，保证同一条序列的键稳定
    labels: Mapped[str] = mapped_column(String(1024), nullable=False, default="{}")
    value: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    help_text: Mapped[str] = mapped_column(String(512), nullable=False, default="")


class MetricHistogramBucketRow(Base):
    """``metric_histogram_buckets``：直方图分桶累计（执行耗时分布）。"""

    __tablename__ = "metric_histogram_buckets"
    __table_args__ = (
        UniqueConstraint(
            "metric", "labels", "bucket", name="uq_metric_histogram_bucket_series"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    metric: Mapped[str] = mapped_column(String(128), nullable=False)
    labels: Mapped[str] = mapped_column(String(1024), nullable=False, default="{}")
    # 桶上界（毫秒）；+Inf 桶用 NULL 表示
    bucket: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cumulative_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class MetricHistogramSumRow(Base):
    """``metric_histogram_sums``：直方图观测总数与总和（毫秒）。"""

    __tablename__ = "metric_histogram_sums"
    __table_args__ = (UniqueConstraint("metric", "labels", name="uq_metric_histogram_sum_series"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    metric: Mapped[str] = mapped_column(String(128), nullable=False)
    labels: Mapped[str] = mapped_column(String(1024), nullable=False, default="{}")
    observation_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sum_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    help_text: Mapped[str] = mapped_column(String(512), nullable=False, default="")
