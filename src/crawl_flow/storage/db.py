"""SQLite 持久化：建表、幂等 upsert、统计查询。"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import (
    Engine,
    and_,
    case,
    func,
    literal_column,
    select,
    true,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from crawl_flow.errors import StoreError
from crawl_flow.models import Base, RawRecord, RawRecordRow, Record, RecordRow

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

STATS_DIMENSIONS = ("category", "tag")
"""``dimension_stats`` 支持的统计维度。"""

UNCATEGORIZED_LABEL = "(未分类)"
"""category 为 NULL 时在统计结果中使用的占位值。"""


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

    def dimension_stats(
        self,
        dimension: str,
        *,
        sources: list[str] | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> dict[str, Any]:
        """按分类（category）或标签（tag）维度汇总 records。

        每条结果包含：维度值、记录总数、最近更新时间、关联 raw 总数与
        失败 raw 占比。``category`` 为 NULL 的记录归入
        :data:`UNCATEGORIZED_LABEL`；标签为空数组的记录不进入任何标签
        桶。``sources`` / ``start`` / ``end`` 均基于 records 表过滤，
        关联失败数通过 ``(source, external_id)`` 连接 raw_records 统计。
        """

        if dimension not in STATS_DIMENSIONS:
            raise ValueError(
                f"不支持的统计维度: {dimension}，可选值: {', '.join(STATS_DIMENSIONS)}"
            )
        if start is not None and end is not None and start > end:
            raise ValueError("时间区间起点不能晚于终点")

        record_filters = self._record_filters(sources, start, end)
        raw_join = and_(
            RawRecordRow.source == RecordRow.source,
            RawRecordRow.external_id == RecordRow.external_id,
        )
        failed_flag = case((RawRecordRow.status == "failed", 1), else_=0)

        try:
            with Session(self.engine) as session:
                if dimension == "category":
                    value_expr = func.coalesce(RecordRow.category, UNCATEGORIZED_LABEL)
                    aggregated_rows = session.execute(
                        select(
                            value_expr.label("dimension_value"),
                            func.count().label("record_count"),
                            func.max(RecordRow.updated_at).label("last_updated_at"),
                            func.count(RawRecordRow.id).label("raw_total"),
                            func.coalesce(func.sum(failed_flag), 0).label("raw_failed"),
                        )
                        .select_from(RecordRow)
                        .outerjoin(RawRecordRow, raw_join)
                        .where(*record_filters)
                        .group_by(value_expr)
                    ).all()
                    overall_raw_total: int | None = None
                    overall_raw_failed: int | None = None
                else:
                    tag_table = func.json_each(RecordRow.tags).table_valued(
                        literal_column("value")
                    )
                    tag_value = tag_table.c.value
                    aggregated_rows = session.execute(
                        select(
                            tag_value.label("dimension_value"),
                            func.count().label("record_count"),
                            func.max(RecordRow.updated_at).label("last_updated_at"),
                            func.count(RawRecordRow.id).label("raw_total"),
                            func.coalesce(func.sum(failed_flag), 0).label("raw_failed"),
                        )
                        .select_from(RecordRow)
                        .join(tag_table, true())
                        .outerjoin(RawRecordRow, raw_join)
                        .where(*record_filters)
                        .group_by(tag_value)
                    ).all()
                    # 标签会展开多份，整体口径按 distinct raw 行统计，避免重复计数
                    overall = session.execute(
                        select(
                            func.count(func.distinct(RawRecordRow.id)),
                            func.count(
                                func.distinct(case((RawRecordRow.status == "failed", RawRecordRow.id)))
                            ),
                        )
                        .select_from(RecordRow)
                        .outerjoin(RawRecordRow, raw_join)
                        .where(*record_filters)
                    ).one()
                    overall_raw_total = int(overall[0] or 0)
                    overall_raw_failed = int(overall[1] or 0)
        except SQLAlchemyError as exc:
            raise StoreError(f"维度统计失败 dimension={dimension} error={exc}") from exc

        rows: list[dict[str, Any]] = []
        total_records = 0
        total_failed_hits = 0
        total_raw_hits = 0
        for value, count, last_updated, raw_total, raw_failed in aggregated_rows:
            count = int(count)
            raw_total = int(raw_total or 0)
            raw_failed = int(raw_failed or 0)
            total_records += count
            total_raw_hits += raw_total
            total_failed_hits += raw_failed
            rows.append(
                {
                    "dimension": dimension,
                    "dimension_value": value,
                    "record_count": count,
                    "last_updated_at": last_updated,
                    "raw_total": raw_total,
                    "raw_failed": raw_failed,
                    "raw_failed_ratio": (raw_failed / raw_total) if raw_total else 0.0,
                }
            )
        rows.sort(key=lambda item: item["dimension_value"])

        if dimension == "category":
            # 一条记录只属于一个分类，桶内 raw 求和即整体 distinct 口径
            overall_raw_total = total_raw_hits
            overall_raw_failed = total_failed_hits
        totals = {
            "records": total_records,
            "raw_total": int(overall_raw_total or 0),
            "raw_failed": int(overall_raw_failed or 0),
            "raw_failed_ratio": (
                (overall_raw_failed / overall_raw_total)
                if overall_raw_total
                else 0.0
            ),
        }
        return {"dimension": dimension, "rows": rows, "totals": totals}

    @staticmethod
    def _record_filters(
        sources: list[str] | None,
        start: datetime | None,
        end: datetime | None,
    ) -> list[Any]:
        filters: list[Any] = []
        if sources:
            filters.append(RecordRow.source.in_(list(sources)))
        if start is not None:
            filters.append(RecordRow.fetched_at >= start)
        if end is not None:
            filters.append(RecordRow.fetched_at <= end)
        return filters

    def close(self) -> None:
        self.engine.dispose()


def _create_engine(path: Path) -> Engine:
    from sqlalchemy import create_engine

    if str(path) == ":memory:":
        return create_engine("sqlite://", future=True)
    return create_engine(f"sqlite:///{path}", future=True)
