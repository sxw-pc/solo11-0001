"""SQLite 持久化：建表、幂等 upsert、统计查询、只读差异快照。"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from sqlalchemy import Engine, func, select, text
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

# dry-run 差异对比允许查询的标准字段（与 config.ALLOWED_DIFF_FIELDS 保持一致）。
DIFF_QUERY_FIELDS = ("title", "category", "tags", "published_at", "content", "url")
DIFF_CHUNK_SIZE = 500


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

    def fetch_source_record_ids(self, source: str) -> list[str]:
        """只读查询某源的全部 ``external_id``，用于发现存量缺失。

        单条短查询，不做写事务，也不加锁；调用方应立即关闭结果。
        """

        try:
            with Session(self.engine) as session:
                rows = session.execute(
                    select(RecordRow.external_id).where(RecordRow.source == source)
                )
                return [str(row) for row in rows.scalars()]
        except SQLAlchemyError as exc:
            raise StoreError(f"存量 ID 查询失败 source={source} error={exc}") from exc

    def fetch_diff_snapshot(
        self,
        source: str,
        external_ids: Sequence[str],
        fields: Sequence[str],
    ) -> dict[str, dict[str, Any]]:
        """批量读取存量记录用于差异对比。

        - ``external_ids`` 按 :data:`DIFF_CHUNK_SIZE` 分块，避免超长 SQL。
        - 只 SELECT 需要的对比字段，不读取 ``id`` / 时间戳等无关列。
        - 每次查询独立短会话并在 ``finally`` 中立即关闭，不持有长事务。

        返回 ``{external_id: {field: value}}``；不存在的 ID 不会出现在结果里。
        """

        if not external_ids or not fields:
            return {}
        unknown = [name for name in fields if name not in DIFF_QUERY_FIELDS]
        if unknown:
            raise StoreError(f"不支持的对比字段: {', '.join(unknown)}")

        columns = [RecordRow.external_id] + [getattr(RecordRow, name) for name in fields]
        snapshot: dict[str, dict[str, Any]] = {}
        ids = [str(item) for item in external_ids]
        try:
            for start in range(0, len(ids), DIFF_CHUNK_SIZE):
                chunk = ids[start : start + DIFF_CHUNK_SIZE]
                session = Session(self.engine)
                try:
                    rows = session.execute(
                        select(*columns).where(
                            RecordRow.source == source,
                            RecordRow.external_id.in_(chunk),
                        )
                    ).all()
                    for row in rows:
                        mapping = row._mapping
                        external_id = str(mapping[RecordRow.external_id])
                        snapshot[external_id] = {
                            name: mapping[getattr(RecordRow, name)] for name in fields
                        }
                finally:
                    session.close()
        except SQLAlchemyError as exc:
            raise StoreError(
                f"存量记录批量查询失败 source={source} error={exc}"
            ) from exc
        return snapshot

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

    def close(self) -> None:
        self.engine.dispose()


def _create_engine(path: Path) -> Engine:
    from sqlalchemy import create_engine

    if str(path) == ":memory:":
        return create_engine("sqlite://", future=True)
    return create_engine(f"sqlite:///{path}", future=True)
