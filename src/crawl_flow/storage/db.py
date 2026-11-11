"""SQLite 持久化：建表、幂等 upsert、统计查询。

存储层基于 SQLAlchemy 2.x API：``select()``/``Session`` 新式查询、
``sqlite_insert ... on_conflict_do_update`` 幂等写入、
``sessionmaker.begin()`` 事务边界（成功提交、异常回滚，保证原子性）。
"""

from __future__ import annotations

import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, create_engine, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool, StaticPool

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

# SQLite 写锁竞争时的等待上限（busy_timeout，秒）：并发写入时等待锁释放，
# 而不是立即报 "database is locked"。
SQLITE_BUSY_TIMEOUT_SECONDS = 30


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Database:
    """单文件 SQLite 数据库封装，所有 SQLAlchemy 异常统一转 :class:`StoreError`。

    实例级 ``_lock`` 串行化本进程内的所有会话：文件库下与 SQLite 自身的
    写锁语义一致（写操作本就串行），内存库下保证多线程共享同一连接时
    事务不交叉。跨进程并发由 busy timeout 兜底。
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine: Engine = _create_engine(self.path)
        self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        self._lock = threading.RLock()

    def init(self) -> None:
        """创建两张表及索引（幂等，对已存在的旧版数据库文件是 no-op）。"""

        try:
            with self._lock:
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
            # 单条记录一个事务：commit 前任何异常都会整体回滚，
            # 不会留下半条脏数据，也不影响其他条目的写入。
            with self._lock, self._session_factory.begin() as session:
                statement = sqlite_insert(RecordRow).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=["source", "external_id"],
                    set_={field: statement.excluded[field] for field in RECORD_UPDATE_FIELDS},
                )
                session.execute(statement)
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
            with self._lock, self._session_factory.begin() as session:
                statement = sqlite_insert(RawRecordRow).values(**values)
                statement = statement.on_conflict_do_update(
                    index_elements=["source", "external_id"],
                    set_={field: statement.excluded[field] for field in RAW_UPDATE_FIELDS},
                )
                session.execute(statement)
        except SQLAlchemyError as exc:
            raise StoreError(
                f"raw_records 写入失败 source={raw.source} "
                f"external_id={raw.external_id} error={exc}"
            ) from exc

    def count_records(self, source: str | None = None) -> int:
        with self._lock, self._session_factory() as session:
            statement = select(func.count()).select_from(RecordRow)
            if source is not None:
                statement = statement.where(RecordRow.source == source)
            return int(session.execute(statement).scalar_one())

    def count_raw(self, source: str | None = None, status: str | None = None) -> int:
        with self._lock, self._session_factory() as session:
            statement = select(func.count()).select_from(RawRecordRow)
            if source is not None:
                statement = statement.where(RawRecordRow.source == source)
            if status is not None:
                statement = statement.where(RawRecordRow.status == status)
            return int(session.execute(statement).scalar_one())

    def stats(self) -> dict[str, Any]:
        """按源汇总 records 数量、raw 成功/失败数量与最近抓取时间。"""

        with self._lock, self._session_factory() as session:
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
    """按 SQLite 单文件特性创建引擎。

    - 文件库：``NullPool`` 不保留空闲连接，每次用后即还，多线程/多进程
      共享同一数据库文件时不会出现连接池耗尽；``timeout`` 设置 busy
      timeout，写锁竞争时等待而非立即失败。
    - 内存库：``StaticPool`` 全进程共享同一连接（否则每个连接都是独立的
      空库），并关闭 ``check_same_thread`` 以支持跨线程使用。
    """

    connect_args: dict[str, Any] = {"timeout": SQLITE_BUSY_TIMEOUT_SECONDS}
    if str(path) == ":memory:":
        return create_engine(
            "sqlite://",
            connect_args={"check_same_thread": False, **connect_args},
            poolclass=StaticPool,
        )
    return create_engine(
        f"sqlite:///{path}",
        connect_args=connect_args,
        poolclass=NullPool,
    )
