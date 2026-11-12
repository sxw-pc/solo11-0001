"""SQLite 持久化：建表、幂等 upsert、统计查询。"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from crawl_flow.errors import StoreError
from crawl_flow.models import (
    Base,
    ProbeRecord,
    ProbeRecordRow,
    RawRecord,
    RawRecordRow,
    Record,
    RecordRow,
    SourceHealthStats,
)

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

RAW_UPDATE_FIELDS = (
    "url",
    "http_status",
    "fetched_at",
    "status",
    "error",
    "raw_path",
    "elapsed_ms",
)

# 兼容旧库：``create_all`` 不会给已存在的表补列，这里显式补齐新增列。
_RAW_RECORD_COLUMNS: dict[str, str] = {
    "elapsed_ms": "INTEGER NOT NULL DEFAULT 0",
}


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
        """创建所有表及索引（幂等），并为旧库补齐新增列。"""

        try:
            Base.metadata.create_all(self.engine)
            self._ensure_raw_record_columns()
        except SQLAlchemyError as exc:
            raise StoreError(f"建表失败 db={self.path} error={exc}") from exc

    def _ensure_raw_record_columns(self) -> None:
        from sqlalchemy import inspect, text

        inspector = inspect(self.engine)
        if "raw_records" not in inspector.get_table_names():
            return
        existing = {column["name"] for column in inspector.get_columns("raw_records")}
        missing = [
            (name, ddl)
            for name, ddl in _RAW_RECORD_COLUMNS.items()
            if name not in existing
        ]
        if not missing:
            return
        with self.engine.begin() as connection:
            for name, ddl in missing:
                connection.execute(text(f"ALTER TABLE raw_records ADD COLUMN {name} {ddl}"))

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
            "elapsed_ms": raw.elapsed_ms,
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

    def health_stats(self, source: str) -> SourceHealthStats:
        """按源汇总 ``raw_records`` 的历史错误率与平均响应时间。

        没有历史数据时返回空统计（``failure_rate`` / ``avg_response_ms`` 均为
        ``None``），评级不会被历史信号收紧。
        """

        with Session(self.engine) as session:
            total = session.execute(
                select(func.count())
                .select_from(RawRecordRow)
                .where(RawRecordRow.source == source)
            ).scalar_one()
            failed = session.execute(
                select(func.count())
                .select_from(RawRecordRow)
                .where(RawRecordRow.source == source, RawRecordRow.status == "failed")
            ).scalar_one()
            avg_elapsed = session.execute(
                select(func.avg(RawRecordRow.elapsed_ms))
                .select_from(RawRecordRow)
                .where(RawRecordRow.source == source, RawRecordRow.elapsed_ms > 0)
            ).scalar_one()

        total_int = int(total or 0)
        failed_int = int(failed or 0)
        return SourceHealthStats(
            total=total_int,
            failed=failed_int,
            failure_rate=(failed_int / total_int) if total_int else None,
            avg_response_ms=float(avg_elapsed) if avg_elapsed is not None else None,
        )

    def insert_probe(self, probe: ProbeRecord) -> None:
        """追加一条健康探测记录（``probe_records`` 只增不改）。"""

        values = {
            "source": probe.source,
            "probed_at": probe.probed_at,
            "outcome": probe.outcome,
            "rating": probe.rating,
            "http_status": probe.http_status,
            "elapsed_ms": probe.elapsed_ms,
            "history_failure_rate": probe.history_failure_rate,
            "avg_response_ms": probe.avg_response_ms,
            "error": probe.error,
        }
        try:
            with self._session_factory() as session:
                session.add(ProbeRecordRow(**values))
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(
                f"probe_records 写入失败 source={probe.source} error={exc}"
            ) from exc

    def probe_history(self, source: str | None = None) -> list[dict[str, Any]]:
        """读取探测历史（按探测时间升序），供 ``db health`` 展示或测试断言。"""

        with Session(self.engine) as session:
            statement = select(ProbeRecordRow)
            if source is not None:
                statement = statement.where(ProbeRecordRow.source == source)
            statement = statement.order_by(ProbeRecordRow.probed_at, ProbeRecordRow.id)
            rows = list(session.execute(statement).scalars())
        return [
            {
                "source": row.source,
                "probed_at": row.probed_at,
                "outcome": row.outcome,
                "rating": row.rating,
                "http_status": row.http_status,
                "elapsed_ms": row.elapsed_ms,
                "history_failure_rate": row.history_failure_rate,
                "avg_response_ms": row.avg_response_ms,
                "error": row.error,
            }
            for row in rows
        ]

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

    def close(self) -> None:
        self.engine.dispose()


def _create_engine(path: Path) -> Engine:
    from sqlalchemy import create_engine

    if str(path) == ":memory:":
        return create_engine("sqlite://", future=True)
    return create_engine(f"sqlite:///{path}", future=True)
