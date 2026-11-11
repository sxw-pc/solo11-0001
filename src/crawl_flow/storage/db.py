"""SQLite 持久化：建表、旧库迁移、幂等 upsert、版本历史与统计查询。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, func, inspect, select, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from crawl_flow.errors import StoreError
from crawl_flow.models import (
    Base,
    RawRecord,
    RawRecordRow,
    Record,
    RecordRow,
    RecordVersionRow,
)

RAW_UPDATE_FIELDS = ("url", "http_status", "fetched_at", "status", "error", "raw_path")


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def compute_content_hash(title: str, content: str) -> str:
    """对归一化后的 title + content 计算稳定 SHA-256 指纹。

    使用 JSON 数组编码避免简单拼接造成的边界歧义；``ensure_ascii=False``
    保证中文正文按 UTF-8 原样参与哈希。
    """

    payload = json.dumps([title, content], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class Database:
    """单文件 SQLite 数据库封装，所有 SQLAlchemy 异常统一转 :class:`StoreError`。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine: Engine = _create_engine(self.path)
        self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

    def init(self) -> None:
        """创建表与索引，并自动迁移旧库（幂等）。

        旧库的 ``records`` 表可能没有 ``content_hash`` 列或 ``record_versions``
        表。这里通过 ``ALTER TABLE`` 补列、``create_all`` 建新表，再为已有
        记录回填哈希并写入 version=1 的初始版本。
        """

        try:
            Base.metadata.create_all(self.engine)
            self._migrate_legacy_schema()
        except SQLAlchemyError as exc:
            raise StoreError(f"建表/迁移失败 db={self.path} error={exc}") from exc

    def _migrate_legacy_schema(self) -> None:
        """把旧库升级到带版本历史的 schema，可重复执行。"""

        inspector = inspect(self.engine)
        if "records" not in inspector.get_table_names():
            return
        columns = {column["name"] for column in inspector.get_columns("records")}
        if "content_hash" not in columns:
            with self.engine.begin() as connection:
                connection.execute(text("ALTER TABLE records ADD COLUMN content_hash VARCHAR(64)"))

        with Session(self.engine) as session:
            records = list(session.execute(select(RecordRow)).scalars())
            versioned = {
                (source, external_id)
                for source, external_id in session.execute(
                    select(RecordVersionRow.source, RecordVersionRow.external_id).distinct()
                ).all()
            }
            for row in records:
                digest = row.content_hash or compute_content_hash(row.title, row.content)
                row.content_hash = digest
                if (row.source, row.external_id) not in versioned:
                    session.add(
                        RecordVersionRow(
                            source=row.source,
                            external_id=row.external_id,
                            version=1,
                            content_hash=digest,
                            created_at=row.updated_at or row.fetched_at or _utcnow(),
                        )
                    )
            session.commit()

    def upsert_record(self, record: Record) -> bool:
        """按 (source, external_id) 幂等写入，并维护内容版本历史。

        对 ``title`` + ``content`` 计算哈希：内容变化时更新记录并追加一条
        ``record_versions``（版本号从 1 起步递增）；内容未变时只刷新
        ``updated_at``，不新增版本。新记录写入 version=1。

        返回 ``True`` 表示已有记录的内容发生了变化（新记录返回 ``False``）。
        """

        fetched_at = record.fetched_at or _utcnow()
        now = _utcnow()
        digest = compute_content_hash(record.title, record.content)
        try:
            with self._session_factory() as session:
                existing = session.execute(
                    select(RecordRow).where(
                        RecordRow.source == record.source,
                        RecordRow.external_id == record.external_id,
                    )
                ).scalar_one_or_none()

                if existing is None:
                    session.add(
                        RecordRow(
                            source=record.source,
                            external_id=record.external_id,
                            title=record.title,
                            category=record.category,
                            tags=list(record.tags),
                            published_at=record.published_at,
                            content=record.content,
                            url=record.url,
                            fetched_at=fetched_at,
                            updated_at=now,
                            content_hash=digest,
                        )
                    )
                    session.add(
                        RecordVersionRow(
                            source=record.source,
                            external_id=record.external_id,
                            version=1,
                            content_hash=digest,
                            created_at=now,
                        )
                    )
                    changed = False
                elif existing.content_hash != digest:
                    existing.title = record.title
                    existing.category = record.category
                    existing.tags = list(record.tags)
                    existing.published_at = record.published_at
                    existing.content = record.content
                    existing.url = record.url
                    existing.fetched_at = fetched_at
                    existing.updated_at = now
                    existing.content_hash = digest
                    latest_version = session.execute(
                        select(func.max(RecordVersionRow.version)).where(
                            RecordVersionRow.source == record.source,
                            RecordVersionRow.external_id == record.external_id,
                        )
                    ).scalar_one()
                    session.add(
                        RecordVersionRow(
                            source=record.source,
                            external_id=record.external_id,
                            version=(latest_version or 0) + 1,
                            content_hash=digest,
                            created_at=now,
                        )
                    )
                    changed = True
                else:
                    # 内容未变：保持幂等语义，只刷新 updated_at，不新增版本。
                    existing.updated_at = now
                    changed = False

                session.commit()
                return changed
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

    def count_versions(
        self, source: str | None = None, external_id: str | None = None
    ) -> int:
        """统计 ``record_versions`` 行数，可按源 / external_id 过滤。"""

        with Session(self.engine) as session:
            statement = select(func.count()).select_from(RecordVersionRow)
            if source is not None:
                statement = statement.where(RecordVersionRow.source == source)
            if external_id is not None:
                statement = statement.where(RecordVersionRow.external_id == external_id)
            return int(session.execute(statement).scalar_one())

    def history(
        self,
        source: str | None = None,
        external_id: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """返回版本时间线，按 source / external_id / version 升序排列。"""

        with Session(self.engine) as session:
            statement = select(RecordVersionRow).order_by(
                RecordVersionRow.source,
                RecordVersionRow.external_id,
                RecordVersionRow.version,
            )
            if source is not None:
                statement = statement.where(RecordVersionRow.source == source)
            if external_id is not None:
                statement = statement.where(RecordVersionRow.external_id == external_id)
            statement = statement.limit(limit)
            rows = session.execute(statement).scalars()
            return [
                {
                    "source": row.source,
                    "external_id": row.external_id,
                    "version": row.version,
                    "content_hash": row.content_hash,
                    "created_at": row.created_at,
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
