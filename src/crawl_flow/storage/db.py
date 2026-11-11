"""SQLite 持久化：建表、幂等 upsert、统计查询、raw 元信息检索。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from crawl_flow.errors import StoreError
from crawl_flow.models import Base, RawRecord, RawRecordRow, Record, RecordRow

DEFAULT_SEARCH_LIMIT = 100
MAX_SEARCH_LIMIT = 10_000

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


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class RawSearchFilter:
    """raw_records 检索条件，全部可选；所有维度都走数据库索引/条件过滤。"""

    source: str | None = None
    external_id: str | None = None
    status: str | None = None
    error_keyword: str | None = None
    start: datetime | None = None
    end: datetime | None = None


@dataclass(frozen=True)
class RawSearchPage:
    """一页检索结果：当前页行、满足条件的总行数（与分页无关）、实际使用的 limit。"""

    rows: list[RawRecord]
    total: int
    limit: int
    offset: int


def normalize_naive_utc(value: datetime) -> datetime:
    """把带时区的时间转成 UTC naive，与 SQLite 里存储的 fetched_at 口径一致。"""

    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


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

    def search_raw(
        self,
        search_filter: RawSearchFilter | None = None,
        *,
        limit: int = DEFAULT_SEARCH_LIMIT,
        offset: int = 0,
    ) -> RawSearchPage:
        """按元信息维度检索 raw_records，不触碰 raw 文件。

        过滤与排序全部在 SQLite 内完成（source/status 等可命中既有索引），
        只有正文关键词检索才由调用方在结果之上按需加载 raw 文件。
        ``limit`` 默认 100，防止一次性把整张表读进内存。
        """

        search_filter = search_filter or RawSearchFilter()
        limit = min(max(limit, 1), MAX_SEARCH_LIMIT)
        offset = max(offset, 0)
        conditions = []
        if search_filter.source is not None:
            conditions.append(RawRecordRow.source == search_filter.source)
        if search_filter.external_id is not None:
            conditions.append(RawRecordRow.external_id == search_filter.external_id)
        if search_filter.status is not None:
            conditions.append(RawRecordRow.status == search_filter.status)
        if search_filter.error_keyword:
            escaped = (
                search_filter.error_keyword.replace("\\", "\\\\")
                .replace("%", "\\%")
                .replace("_", "\\_")
            )
            conditions.append(
                RawRecordRow.error.like(f"%{escaped}%", escape="\\")
            )
        if search_filter.start is not None:
            conditions.append(
                RawRecordRow.fetched_at >= normalize_naive_utc(search_filter.start)
            )
        if search_filter.end is not None:
            conditions.append(
                RawRecordRow.fetched_at <= normalize_naive_utc(search_filter.end)
            )

        try:
            with Session(self.engine) as session:
                total = int(
                    session.execute(
                        select(func.count())
                        .select_from(RawRecordRow)
                        .where(*conditions)
                    ).scalar_one()
                )
                result = session.execute(
                    select(RawRecordRow)
                    .where(*conditions)
                    .order_by(RawRecordRow.fetched_at.desc(), RawRecordRow.id.desc())
                    .limit(limit)
                    .offset(offset)
                ).scalars().all()
                rows = [
                    RawRecord(
                        source=row.source,
                        external_id=row.external_id,
                        url=row.url,
                        http_status=row.http_status,
                        fetched_at=row.fetched_at,
                        status=row.status,
                        error=row.error,
                        raw_path=row.raw_path,
                    )
                    for row in result
                ]
        except SQLAlchemyError as exc:
            raise StoreError(f"raw_records 检索失败 error={exc}") from exc
        return RawSearchPage(rows=rows, total=total, limit=limit, offset=offset)

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
