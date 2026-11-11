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
    CorrelationEventRow,
    RawRecord,
    RawRecordRow,
    Record,
    RecordRow,
    WideRecordRow,
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

RAW_UPDATE_FIELDS = ("url", "http_status", "fetched_at", "status", "error", "raw_path")

WIDE_UPDATE_FIELDS = (
    "status",
    "primary_source",
    "sources",
    "fields",
    "conflict_detail",
    "fetched_at",
    "updated_at",
)

EVENT_UPDATE_FIELDS = ("detail", "updated_at")


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
        """创建两张表及索引（幂等）。"""

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

    def load_records(self, source: str) -> list[Record]:
        """一次性加载某源的全部归一化记录，用于预加载关联索引。

        避免关联时逐行查询：每个关联源一轮采集只产生一次 SQL。
        """

        with Session(self.engine) as session:
            rows = (
                session.execute(
                    select(RecordRow)
                    .where(RecordRow.source == source)
                    .order_by(RecordRow.external_id)
                )
                .scalars()
                .all()
            )
            return [
                Record(
                    source=row.source,
                    external_id=row.external_id,
                    title=row.title,
                    category=row.category,
                    tags=list(row.tags or []),
                    published_at=row.published_at,
                    content=row.content,
                    url=row.url,
                    fetched_at=row.fetched_at,
                )
                for row in rows
            ]

    def upsert_wide_record(
        self,
        *,
        table_name: str,
        entity_key: str,
        status: str,
        primary_source: str,
        sources: list[str],
        fields: dict[str, Any],
        conflict_detail: dict[str, Any] | None,
        fetched_at: datetime,
    ) -> None:
        """按 (table_name, entity_key) 幂等写入宽表，重复采集只更新不增行。"""

        values = {
            "table_name": table_name,
            "entity_key": entity_key,
            "status": status,
            "primary_source": primary_source,
            "sources": list(sources),
            "fields": dict(fields),
            "conflict_detail": conflict_detail,
            "fetched_at": fetched_at,
            "updated_at": _utcnow(),
        }
        try:
            with self._session_factory() as session:
                statement = sqlite_insert(WideRecordRow).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=["table_name", "entity_key"],
                    set_={field: statement.excluded[field] for field in WIDE_UPDATE_FIELDS},
                )
                session.execute(statement)
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(
                f"wide_records 写入失败 table={table_name} "
                f"entity_key={entity_key} error={exc}"
            ) from exc

    def upsert_correlation_event(
        self,
        *,
        table_name: str,
        local_source: str,
        local_external_id: str,
        remote_source: str,
        event_type: str,
        detail: dict[str, Any],
    ) -> None:
        """按 (local_source, local_external_id, event_type) 幂等写入关联事件。"""

        now = _utcnow()
        values = {
            "table_name": table_name,
            "local_source": local_source,
            "local_external_id": local_external_id,
            "remote_source": remote_source,
            "event_type": event_type,
            "detail": dict(detail),
            "created_at": now,
            "updated_at": now,
        }
        try:
            with self._session_factory() as session:
                statement = sqlite_insert(CorrelationEventRow).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=["local_source", "local_external_id", "event_type"],
                    set_={field: statement.excluded[field] for field in EVENT_UPDATE_FIELDS},
                )
                session.execute(statement)
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(
                f"correlation_events 写入失败 source={local_source} "
                f"external_id={local_external_id} event={event_type} error={exc}"
            ) from exc

    def count_wide_records(self, table_name: str | None = None) -> int:
        with Session(self.engine) as session:
            statement = select(func.count()).select_from(WideRecordRow)
            if table_name is not None:
                statement = statement.where(WideRecordRow.table_name == table_name)
            return int(session.execute(statement).scalar_one())

    def count_correlation_events(
        self, event_type: str | None = None, table_name: str | None = None
    ) -> int:
        with Session(self.engine) as session:
            statement = select(func.count()).select_from(CorrelationEventRow)
            if event_type is not None:
                statement = statement.where(CorrelationEventRow.event_type == event_type)
            if table_name is not None:
                statement = statement.where(CorrelationEventRow.table_name == table_name)
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

    def close(self) -> None:
        self.engine.dispose()


def _create_engine(path: Path) -> Engine:
    from sqlalchemy import create_engine

    if str(path) == ":memory:":
        return create_engine("sqlite://", future=True)
    return create_engine(f"sqlite:///{path}", future=True)
