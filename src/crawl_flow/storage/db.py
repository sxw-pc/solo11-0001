"""SQLite 持久化：建表、幂等 upsert、统计查询、采集运行记录。"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, delete, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from crawl_flow.errors import StoreError
from crawl_flow.models import Base, JobRun, JobRunRow, RawRecord, RawRecordRow, Record, RecordRow

RECORD_UPDATE_FIELDS = (
    "title",
    "category",
    "tags",
    "published_at",
    "content",
    "url",
    "fetched_at",
    "updated_at",
)

RAW_UPDATE_FIELDS = ("url", "http_status", "fetched_at", "status", "error", "raw_path")

# 每个源最多保留的运行历史条数，防止 job_runs 无限增长。
JOB_RUNS_KEEP_PER_SOURCE = 1000


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Database:
    """单文件 SQLite 数据库封装，所有 SQLAlchemy 异常统一转 :class:`StoreError`。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine: Engine = _create_engine(self.path)
        self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

    def init(self) -> None:
        """创建全部表及索引（幂等）。"""

        try:
            Base.metadata.create_all(self.engine)
        except SQLAlchemyError as exc:
            raise StoreError(f"建表失败 db={self.path} error={exc}") from exc

    def upsert_record(self, record: Record) -> None:
        """按 (source, external_id) 幂等写入；重复采集只更新内容与时间戳。"""

        fetched_at = record.fetched_at or _utcnow()
        values = {
            "source": record.source,
            "external_id": record.external_id,
            "title": record.title,
            "category": record.category,
            "tags": list(record.tags),
            "published_at": record.published_at,
            "content": record.content,
            "url": record.url,
            "fetched_at": fetched_at,
            "updated_at": _utcnow(),
        }
        try:
            with self._session_factory() as session:
                statement = sqlite_insert(RecordRow).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=["source", "external_id"],
                    set_={field: statement.excluded[field] for field in RECORD_UPDATE_FIELDS},
                )
                session.execute(statement)
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(
                f"records 写入失败 source={record.source} "
                f"external_id={record.external_id} error={exc}"
            ) from exc

    def upsert_raw(self, raw: RawRecord) -> None:
        """按 (source, external_id) 幂等写入原始响应元信息。"""

        values = {
            "source": raw.source,
            "external_id": raw.external_id,
            "url": raw.url,
            "http_status": raw.http_status,
            "fetched_at": raw.fetched_at,
            "status": raw.status,
            "error": raw.error,
            "raw_path": raw.raw_path,
        }
        try:
            with self._session_factory() as session:
                statement = sqlite_insert(RawRecordRow).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=["source", "external_id"],
                    set_={field: statement.excluded[field] for field in RAW_UPDATE_FIELDS},
                )
                session.execute(statement)
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(
                f"raw_records 写入失败 source={raw.source} "
                f"external_id={raw.external_id} error={exc}"
            ) from exc

    def count_records(self, source: str | None = None) -> int:
        with Session(self.engine) as session:
            statement = select(func.count()).select_from(RecordRow)
            if source is not None:
                statement = statement.where(RecordRow.source == source)
            return int(session.execute(statement).scalar_one())

    def count_raw(self, source: str | None = None, status: str | None = None) -> int:
        with Session(self.engine) as session:
            statement = select(func.count()).select_from(RawRecordRow)
            if source is not None:
                statement = statement.where(RawRecordRow.source == source)
            if status is not None:
                statement = statement.where(RawRecordRow.status == status)
            return int(session.execute(statement).scalar_one())

    def stats(self) -> dict[str, Any]:
        """按源汇总 records 数量、raw 成功/失败数量与最近抓取时间。"""

        with Session(self.engine) as session:
            record_counts = dict(
                session.execute(
                    select(RecordRow.source, func.count()).group_by(RecordRow.source)
                ).all()
            )
            raw_counts = session.execute(
                select(RawRecordRow.source, RawRecordRow.status, func.count()).group_by(
                    RawRecordRow.source, RawRecordRow.status
                )
            ).all()
            last_seen = dict(
                session.execute(
                    select(RawRecordRow.source, func.max(RawRecordRow.fetched_at)).group_by(
                        RawRecordRow.source
                    )
                ).all()
            )

        aggregated: dict[str, dict[str, Any]] = {}

        def bucket(name: str) -> dict[str, Any]:
            return aggregated.setdefault(
                name,
                {
                    "source": name,
                    "records": int(record_counts.get(name, 0)),
                    "raw_ok": 0,
                    "raw_failed": 0,
                    "last_fetched_at": last_seen.get(name),
                },
            )

        for name in record_counts:
            bucket(name)
        for name, status, count in raw_counts:
            entry = bucket(name)
            if status == "failed":
                entry["raw_failed"] = int(count)
            else:
                entry["raw_ok"] = int(count)

        rows = sorted(aggregated.values(), key=lambda item: item["source"])
        totals = {
            "records": sum(row["records"] for row in rows),
            "raw_ok": sum(row["raw_ok"] for row in rows),
            "raw_failed": sum(row["raw_failed"] for row in rows),
        }
        return {"sources": rows, "totals": totals}

    def insert_job_run(self, run: JobRun, *, keep_per_source: int = JOB_RUNS_KEEP_PER_SOURCE) -> None:
        """追加一条运行记录，并裁剪该源超出保留上限的旧记录。"""

        values = {
            "source": run.source,
            "trigger": run.trigger,
            "status": run.status,
            "ok_count": run.ok_count,
            "failed_count": run.failed_count,
            "skipped_count": run.skipped_count,
            "elapsed_ms": run.elapsed_ms,
            "started_at": run.started_at,
            "finished_at": run.finished_at,
            "error": run.error,
        }
        try:
            with self._session_factory() as session:
                session.execute(sqlite_insert(JobRunRow).values(**values))
                if keep_per_source > 0:
                    ranked = (
                        select(
                            JobRunRow.id,
                            func.row_number()
                            .over(partition_by=JobRunRow.source, order_by=JobRunRow.id.desc())
                            .label("rn"),
                        )
                        .subquery()
                    )
                    session.execute(
                        delete(JobRunRow).where(
                            JobRunRow.id.in_(
                                select(ranked.c.id).where(ranked.c.rn > keep_per_source)
                            )
                        )
                    )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"job_runs 写入失败 source={run.source} error={exc}") from exc

    def recent_job_runs(self, *, source: str | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """按开始时间倒序返回最近的运行记录。"""

        statement = select(JobRunRow).order_by(JobRunRow.id.desc()).limit(limit)
        if source is not None:
            statement = statement.where(JobRunRow.source == source)
        with Session(self.engine) as session:
            rows = session.execute(statement).scalars().all()
        return [
            {
                "id": row.id,
                "source": row.source,
                "trigger": row.trigger,
                "status": row.status,
                "ok_count": row.ok_count,
                "failed_count": row.failed_count,
                "skipped_count": row.skipped_count,
                "elapsed_ms": row.elapsed_ms,
                "started_at": row.started_at,
                "finished_at": row.finished_at,
                "error": row.error,
            }
            for row in rows
        ]

    def job_run_aggregates(self) -> dict[str, Any]:
        """汇总运行记录，供指标渲染与 history 展示使用。"""

        with Session(self.engine) as session:
            run_rows = session.execute(
                select(
                    JobRunRow.source,
                    JobRunRow.trigger,
                    JobRunRow.status,
                    func.count(),
                ).group_by(JobRunRow.source, JobRunRow.trigger, JobRunRow.status)
            ).all()
            item_rows = session.execute(
                select(
                    JobRunRow.source,
                    func.coalesce(func.sum(JobRunRow.ok_count), 0),
                    func.coalesce(func.sum(JobRunRow.failed_count), 0),
                    func.coalesce(func.sum(JobRunRow.skipped_count), 0),
                ).group_by(JobRunRow.source)
            ).all()
            duration_rows = session.execute(
                select(
                    JobRunRow.source,
                    func.avg(JobRunRow.elapsed_ms),
                    func.count(),
                )
                .where(JobRunRow.status != "skipped")
                .group_by(JobRunRow.source)
            ).all()
            latest_ids = (
                select(JobRunRow.source, func.max(JobRunRow.id).label("max_id"))
                .group_by(JobRunRow.source)
                .subquery()
            )
            last_rows = session.execute(
                select(JobRunRow).where(JobRunRow.id.in_(select(latest_ids.c.max_id)))
            ).scalars().all()
            executed_ids = (
                select(JobRunRow.source, func.max(JobRunRow.id).label("max_id"))
                .where(JobRunRow.status != "skipped")
                .group_by(JobRunRow.source)
                .subquery()
            )
            last_executed_rows = session.execute(
                select(JobRunRow).where(JobRunRow.id.in_(select(executed_ids.c.max_id)))
            ).scalars().all()

        def _row_dict(row: JobRunRow) -> dict[str, Any]:
            return {
                "source": row.source,
                "status": row.status,
                "trigger": row.trigger,
                "started_at": row.started_at,
                "finished_at": row.finished_at,
                "elapsed_ms": row.elapsed_ms,
            }

        return {
            "runs": [
                {"source": source, "trigger": trigger, "status": status, "count": int(count)}
                for source, trigger, status, count in run_rows
            ],
            "items": [
                {
                    "source": source,
                    "ok": int(ok),
                    "failed": int(failed),
                    "skipped": int(skipped),
                }
                for source, ok, failed, skipped in item_rows
            ],
            "durations": [
                {"source": source, "avg_ms": float(avg_ms), "runs": int(runs)}
                for source, avg_ms, runs in duration_rows
            ],
            "last": [_row_dict(row) for row in last_rows],
            "last_executed": [_row_dict(row) for row in last_executed_rows],
        }

    def close(self) -> None:
        self.engine.dispose()


def _create_engine(path: Path) -> Engine:
    from sqlalchemy import create_engine

    if str(path) == ":memory:":
        return create_engine("sqlite://", future=True)
    # busy timeout：调度线程与手动 CLI 进程可能同时写同一个库文件，
    # 遇到写锁时最多等待 15 秒而不是立即报 "database is locked"。
    return create_engine(
        f"sqlite:///{path}", future=True, connect_args={"timeout": 15}
    )
