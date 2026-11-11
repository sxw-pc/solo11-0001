"""执行一轮采集并记录运行指标：手动 CLI 与定时调度共用的入口。

指标记录失败（如数据库暂时不可用）只记日志、不影响采集本身；
``dry_run`` 模式不写任何运行记录。
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from crawl_flow.config import SourceConfig
from crawl_flow.errors import StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.models import JobRun, JobRunTrigger
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

MAX_ERROR_LENGTH = 500


def execute_and_record(
    source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    *,
    trigger: JobRunTrigger,
    limit: int | None = None,
    dry_run: bool = False,
    fetcher: Fetcher | None = None,
    logger: logging.Logger | None = None,
) -> RunSummary:
    """执行 :func:`run_source` 并把结果写入 ``job_runs``。

    整轮异常（列表页失败等）记录为 ``status=failed`` 后原样向上抛出；
    正常完成记录为 ``succeeded`` 并带上 ok/failed/skipped 计数。
    """

    if dry_run:
        return run_source(
            source, db, raw_store, fetcher=fetcher, limit=limit, dry_run=True, logger=logger
        )

    started_at = datetime.now(timezone.utc)
    try:
        summary = run_source(source, db, raw_store, fetcher=fetcher, limit=limit, logger=logger)
    except Exception as exc:
        finished_at = datetime.now(timezone.utc)
        _safe_record(
            db,
            JobRun(
                source=source.name,
                trigger=trigger,
                status="failed",
                elapsed_ms=_elapsed_ms(started_at, finished_at),
                started_at=started_at,
                finished_at=finished_at,
                error=str(exc).strip().replace("\n", " ")[:MAX_ERROR_LENGTH] or None,
            ),
            logger,
        )
        raise

    finished_at = datetime.now(timezone.utc)
    _safe_record(
        db,
        JobRun(
            source=source.name,
            trigger=trigger,
            status="succeeded",
            ok_count=summary.ok,
            failed_count=summary.failed,
            skipped_count=summary.skipped,
            elapsed_ms=summary.elapsed_ms,
            started_at=started_at,
            finished_at=finished_at,
        ),
        logger,
    )
    return summary


def _safe_record(db: Database, run: JobRun, logger: logging.Logger | None) -> None:
    """写运行记录；失败只告警不抛出，避免指标问题影响采集主流程。"""

    try:
        db.insert_job_run(run)
    except StoreError as exc:
        (logger or logging.getLogger("crawl_flow.runner")).warning(
            "job_runs 记录失败 source=%s error=%s", run.source, exc
        )


def _elapsed_ms(started: datetime, finished: datetime) -> int:
    return max(0, int((finished - started).total_seconds() * 1000))
