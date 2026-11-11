"""SQLite 持久化：建表、幂等 upsert、统计查询。

写连接启用 WAL 与 ``busy_timeout``：采集写入时，``db stats`` 这类只读连接
不会被写事务阻塞（WAL 允许读写并发），遇到瞬时锁也会等待而不是立刻报
``database is locked``。只读连接以 ``mode=ro`` 的 SQLite URI 打开，
物理上无法写入，也不会被部署级运行锁拦截。
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

from sqlalchemy import Engine, event, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from crawl_flow.errors import StoreError
from crawl_flow.models import Base, RawRecord, RawRecordRow, Record, RecordRow

# 写连接遇到锁时的等待上限（毫秒）；配合部署级互斥锁，正常不会触发。
BUSY_TIMEOUT_MS = 5000

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


class Database:
    """单文件 SQLite 数据库封装，所有 SQLAlchemy 异常统一转 :class:`StoreError`。

    ``read_only=True`` 时以只读 URI 打开（供 ``db stats`` 使用）：不会创建
    数据库文件、不会被写事务阻塞，也不可能误写。
    """

    def __init__(self, path: str | Path, *, read_only: bool = False) -> None:
        self.path = Path(path)
        self.read_only = read_only
        if str(self.path) != ":memory:" and not read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine: Engine = _create_engine(self.path, read_only=read_only)
        self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

    def init(self) -> None:
        """创建两张表及索引（幂等）；只读连接下为空操作。"""

        if self.read_only:
            return
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

    def close(self) -> None:
        self.engine.dispose()


def _create_engine(path: Path, *, read_only: bool) -> Engine:
    from sqlalchemy import create_engine

    if str(path) == ":memory:":
        engine = create_engine("sqlite://", future=True)
    elif read_only:
        # 只读 URI：文件不存在时 SQLite 直接报错，不会被悄悄创建。
        uri_path = "file:" + quote(str(path.resolve()), safe="/")
        engine = create_engine(
            f"sqlite:///{uri_path}?uri=true&mode=ro",
            future=True,
        )
    else:
        engine = create_engine(f"sqlite:///{path}", future=True)

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, _connection_record) -> None:  # noqa: ANN001
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(f"PRAGMA busy_timeout={BUSY_TIMEOUT_MS}")
            if read_only:
                # 双保险：连接级禁止写，哪怕上层误用 ORM 也写不进去。
                cursor.execute("PRAGMA query_only=ON")
            else:
                # WAL 是持久化属性，对老库首次连上即完成切换，之后所有连接
                # （含只读连接）都按 WAL 工作；读写不再互相阻塞。
                cursor.execute("PRAGMA journal_mode=WAL")
                # WAL 下 NORMAL 足够安全且更快，避免每次提交都 fsync。
                cursor.execute("PRAGMA synchronous=NORMAL")
        finally:
            cursor.close()

    return engine
