"""SQLite 持久化：建表、幂等 upsert、统计查询。"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import Engine, delete, func, select, text, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from crawl_flow.errors import StoreError
from crawl_flow.models import (
    ArchiveCheckpointRow,
    ArchiveSettingsRow,
    Base,
    RawRecord,
    RawRecordRow,
    Record,
    RecordRow,
)
from crawl_flow.storage.archive import (
    ArchiveStore,
    to_naive_utc,
    utcnow_naive,
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


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Database:
    """单文件 SQLite 数据库封装，所有 SQLAlchemy 异常统一转 :class:`StoreError`。

    ``archive_dir`` 仅用于临时覆盖归档目录；默认不传时，跨库查询会自动读取
    主库中由 archive 命令写入的 ``archive_settings``。没有该设置即保持原有
    单库行为。
    """

    def __init__(self, path: str | Path, *, archive_dir: str | Path | None = None) -> None:
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine: Engine = _create_engine(self.path)
        self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        self._archive_dir_override = (
            Path(archive_dir) if archive_dir is not None else None
        )
        self._archive_state: dict[str, Any] | None = None
        self._archive_state_loaded = False
        self._archive_store: ArchiveStore | None = None

    def init(self) -> None:
        """创建表及索引（幂等）。

        老库升级后只会看到新增的空表，不会改变原有查询行为；额外补建
        ``fetched_at`` 索引以便归档按时间范围选取冷数据。
        """

        try:
            Base.metadata.create_all(self.engine)
            with self.engine.begin() as connection:
                connection.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS ix_records_fetched_at "
                        "ON records (fetched_at)"
                    )
                )
                connection.execute(
                    text(
                        "CREATE INDEX IF NOT EXISTS ix_raw_records_fetched_at "
                        "ON raw_records (fetched_at)"
                    )
                )
        except SQLAlchemyError as exc:
            raise StoreError(f"建表失败 db={self.path} error={exc}") from exc
        self._archive_state_loaded = False
        self._archive_store = None


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

    def count_records(
        self,
        source: str | None = None,
        *,
        include_archive: bool | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> int:
        """统计唯一 ``(source, external_id)`` 数量。

        归档启用且 ``include_archive`` 未显式关闭时自动合并归档库；
        未启用归档的库行为与旧版本完全一致。
        """

        include = self.archive_enabled if include_archive is None else include_archive
        if not include:
            return self._count_hot_records(source=source, start=start, end=end)

        keys: set[tuple[str, str]] = set()
        with Session(self.engine) as session:
            statement = select(RecordRow.source, RecordRow.external_id)
            if source is not None:
                statement = statement.where(RecordRow.source == source)
            if start is not None:
                statement = statement.where(RecordRow.fetched_at >= to_naive_utc(start))
            if end is not None:
                statement = statement.where(RecordRow.fetched_at < to_naive_utc(end))
            keys.update(tuple(row) for row in session.execute(statement).all())
        store = self.archive_store()
        if store is not None:
            keys.update(
                store.fetch_keys("records", source=source, start=start, end=end)
            )
        return len(keys)

    def count_raw(
        self,
        source: str | None = None,
        status: str | None = None,
        *,
        include_archive: bool | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> int:
        include = self.archive_enabled if include_archive is None else include_archive
        if not include:
            return self._count_hot_raw(source=source, status=status, start=start, end=end)

        start_naive = to_naive_utc(start) if start is not None else None
        end_naive = to_naive_utc(end) if end is not None else None
        hot_state: dict[tuple[str, str], str] = {}
        with Session(self.engine) as session:
            statement = select(
                RawRecordRow.source, RawRecordRow.external_id, RawRecordRow.status
            )
            if source is not None:
                statement = statement.where(RawRecordRow.source == source)
            if start_naive is not None:
                statement = statement.where(RawRecordRow.fetched_at >= start_naive)
            if end_naive is not None:
                statement = statement.where(RawRecordRow.fetched_at < end_naive)
            for hot_source, external_id, hot_status in session.execute(statement).all():
                hot_state[(hot_source, external_id)] = hot_status

        store = self.archive_store()
        if store is not None:
            # 热库优先：同一 key 在热库中状态不同时，以热库状态为准。
            for row in store.fetch_raw_states(source=source, start=start, end=end):
                key = (row["source"], row["external_id"])
                hot_state.setdefault(key, row["status"])

        if status is None:
            return len(hot_state)
        return sum(1 for value in hot_state.values() if value == status)

    def _count_hot_records(
        self,
        *,
        source: str | None,
        start: datetime | None,
        end: datetime | None,
    ) -> int:
        with Session(self.engine) as session:
            statement = select(func.count()).select_from(RecordRow)
            if source is not None:
                statement = statement.where(RecordRow.source == source)
            if start is not None:
                statement = statement.where(RecordRow.fetched_at >= to_naive_utc(start))
            if end is not None:
                statement = statement.where(RecordRow.fetched_at < to_naive_utc(end))
            return int(session.execute(statement).scalar_one())

    def _count_hot_raw(
        self,
        *,
        source: str | None,
        status: str | None,
        start: datetime | None,
        end: datetime | None,
    ) -> int:
        with Session(self.engine) as session:
            statement = select(func.count()).select_from(RawRecordRow)
            if source is not None:
                statement = statement.where(RawRecordRow.source == source)
            if status is not None:
                statement = statement.where(RawRecordRow.status == status)
            if start is not None:
                statement = statement.where(RawRecordRow.fetched_at >= to_naive_utc(start))
            if end is not None:
                statement = statement.where(RawRecordRow.fetched_at < to_naive_utc(end))
            return int(session.execute(statement).scalar_one())

    # ---- 跨库查询 -----------------------------------------------------

    def query_records(
        self,
        source: str | None = None,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        include_archive: bool | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[Record]:
        """跨主库与归档库查询记录，无需手动切换数据库。

        同一条 ``(source, external_id)`` 同时存在于热库和归档库时，热库版本优先
        （因为热库代表最近一次采集）；归档分片内部的重复则取 ``fetched_at`` 最新者。
        """

        include = self.archive_enabled if include_archive is None else include_archive
        merged: dict[tuple[str, str], dict[str, Any]] = {}

        store = self.archive_store() if include else None
        if store is not None:
            for row in store.fetch_records(source=source, start=start, end=end):
                key = (row["source"], row["external_id"])
                current = merged.get(key)
                if current is None or _dt(row["fetched_at"]) >= _dt(current["fetched_at"]):
                    merged[key] = row

        with Session(self.engine) as session:
            statement = select(RecordRow)
            if source is not None:
                statement = statement.where(RecordRow.source == source)
            if start is not None:
                statement = statement.where(RecordRow.fetched_at >= to_naive_utc(start))
            if end is not None:
                statement = statement.where(RecordRow.fetched_at < to_naive_utc(end))
            for row in session.execute(statement).scalars():
                merged[(row.source, row.external_id)] = _record_row_values(row)

        ordered = sorted(
            merged.values(),
            key=lambda item: (
                _dt(item["fetched_at"]),
                item["source"],
                item["external_id"],
            ),
            reverse=True,
        )
        if offset:
            ordered = ordered[offset:]
        if limit is not None:
            ordered = ordered[:limit]
        return [_record_from_values(item) for item in ordered]

    def query_raw_records(
        self,
        source: str | None = None,
        *,
        status: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        include_archive: bool | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[RawRecord]:
        include = self.archive_enabled if include_archive is None else include_archive
        merged: dict[tuple[str, str], dict[str, Any]] = {}

        store = self.archive_store() if include else None
        if store is not None:
            for row in store.fetch_raw(source=source, start=start, end=end):
                key = (row["source"], row["external_id"])
                current = merged.get(key)
                if current is None or _dt(row["fetched_at"]) >= _dt(current["fetched_at"]):
                    merged[key] = row

        with Session(self.engine) as session:
            statement = select(RawRecordRow)
            if source is not None:
                statement = statement.where(RawRecordRow.source == source)
            if status is not None:
                statement = statement.where(RawRecordRow.status == status)
            if start is not None:
                statement = statement.where(RawRecordRow.fetched_at >= to_naive_utc(start))
            if end is not None:
                statement = statement.where(RawRecordRow.fetched_at < to_naive_utc(end))
            for row in session.execute(statement).scalars():
                merged[(row.source, row.external_id)] = _raw_row_values(row)

        ordered = sorted(
            merged.values(),
            key=lambda item: (
                _dt(item["fetched_at"]),
                item["source"],
                item["external_id"],
            ),
            reverse=True,
        )
        if offset:
            ordered = ordered[offset:]
        if limit is not None:
            ordered = ordered[:limit]
        if status is not None:
            ordered = [item for item in ordered if item["status"] == status]
        return [_raw_from_values(item) for item in ordered]

    def get_record(self, source: str, external_id: str) -> Record | None:
        with Session(self.engine) as session:
            row = session.execute(
                select(RecordRow).where(
                    (RecordRow.source == source)
                    & (RecordRow.external_id == external_id)
                )
            ).scalar_one_or_none()
            if row is not None:
                return _record_from_values(_record_row_values(row))
        store = self.archive_store()
        if store is None:
            return None
        values = store.get_record(source, external_id)
        return None if values is None else _record_from_values(values)

    def get_raw_record(self, source: str, external_id: str) -> RawRecord | None:
        with Session(self.engine) as session:
            row = session.execute(
                select(RawRecordRow).where(
                    (RawRecordRow.source == source)
                    & (RawRecordRow.external_id == external_id)
                )
            ).scalar_one_or_none()
            if row is not None:
                return _raw_from_values(_raw_row_values(row))
        store = self.archive_store()
        if store is None:
            return None
        values = store.get_raw(source, external_id)
        return None if values is None else _raw_from_values(values)

    # ---- 归档控制 -----------------------------------------------------

    def register_archive(
        self,
        *,
        archive_dir: str,
        cold_raw_dir: str,
        retention_days: int,
    ) -> None:
        """写入/刷新归档设置；存在该设置后跨库查询自动启用。"""

        now = _utcnow()
        try:
            with self._session_factory() as session:
                statement = sqlite_insert(ArchiveSettingsRow).values(
                    id=1,
                    archive_dir=archive_dir,
                    cold_raw_dir=cold_raw_dir,
                    retention_days=int(retention_days),
                    created_at=now,
                    updated_at=now,
                )
                statement = statement.on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "archive_dir": statement.excluded.archive_dir,
                        "cold_raw_dir": statement.excluded.cold_raw_dir,
                        "retention_days": statement.excluded.retention_days,
                        "updated_at": statement.excluded.updated_at,
                    },
                )
                session.execute(statement)
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"归档设置写入失败 error={exc}") from exc
        self._archive_state_loaded = False
        self._archive_store = None

    def prepare_archive_checkpoint(
        self,
        *,
        scope: str,
        archive_dir: str,
        cutoff: datetime,
        restart: bool = False,
    ) -> dict[str, Any]:
        """创建或恢复断点。未完成的 checkpoint 会沿用原 cutoff 与游标继续。"""

        cutoff = to_naive_utc(cutoff)
        now = utcnow_naive()
        try:
            with self._session_factory() as session:
                row = session.execute(
                    select(ArchiveCheckpointRow).where(
                        (ArchiveCheckpointRow.scope == scope)
                        & (ArchiveCheckpointRow.archive_dir == archive_dir)
                    )
                ).scalar_one_or_none()
                resumed = False
                if row is None:
                    row = ArchiveCheckpointRow(
                        scope=scope,
                        archive_dir=archive_dir,
                        cutoff=cutoff,
                        cursor_records=0,
                        cursor_raw=0,
                        status="running",
                        archived_records=0,
                        archived_raw=0,
                        started_at=now,
                        updated_at=now,
                    )
                    session.add(row)
                elif restart or row.status == "completed":
                    row.cutoff = cutoff
                    row.cursor_records = 0
                    row.cursor_raw = 0
                    row.archived_records = 0
                    row.archived_raw = 0
                    row.status = "running"
                    row.started_at = now
                    row.updated_at = now
                else:
                    resumed = True
                    row.status = "running"
                    row.updated_at = now
                session.commit()
                return {
                    "id": row.id,
                    "scope": row.scope,
                    "cutoff": row.cutoff,
                    "cursor_records": row.cursor_records,
                    "cursor_raw": row.cursor_raw,
                    "archived_records": row.archived_records,
                    "archived_raw": row.archived_raw,
                    "resumed": resumed,
                }
        except SQLAlchemyError as exc:
            raise StoreError(f"归档断点初始化失败 error={exc}") from exc

    def finish_archive_checkpoint(self, checkpoint_id: int, *, status: str) -> None:
        try:
            with self._session_factory() as session:
                session.execute(
                    update(ArchiveCheckpointRow)
                    .where(ArchiveCheckpointRow.id == checkpoint_id)
                    .values(status=status, updated_at=utcnow_naive())
                )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"归档断点收尾失败 error={exc}") from exc

    def count_archive_candidates(
        self,
        *,
        cutoff: datetime,
        source: str | None = None,
        start: datetime | None = None,
    ) -> tuple[int, int]:
        cutoff = to_naive_utc(cutoff)
        start_naive = to_naive_utc(start) if start is not None else None
        with Session(self.engine) as session:
            record_statement = (
                select(func.count())
                .select_from(RecordRow)
                .where(RecordRow.fetched_at < cutoff)
            )
            raw_statement = (
                select(func.count())
                .select_from(RawRecordRow)
                .where(RawRecordRow.fetched_at < cutoff)
            )
            if start_naive is not None:
                record_statement = record_statement.where(
                    RecordRow.fetched_at >= start_naive
                )
                raw_statement = raw_statement.where(
                    RawRecordRow.fetched_at >= start_naive
                )
            if source is not None:
                record_statement = record_statement.where(RecordRow.source == source)
                raw_statement = raw_statement.where(RawRecordRow.source == source)
            return (
                int(session.execute(record_statement).scalar_one()),
                int(session.execute(raw_statement).scalar_one()),
            )

    def select_archive_batch(
        self,
        *,
        table: str,
        cursor: int,
        cutoff: datetime,
        source: str | None,
        start: datetime | None = None,
        limit: int,
    ) -> list[dict[str, Any]]:
        cutoff = to_naive_utc(cutoff)
        start_naive = to_naive_utc(start) if start is not None else None
        model = RecordRow if table == "records" else RawRecordRow
        with Session(self.engine) as session:
            statement = select(model).where(
                (model.id > cursor) & (model.fetched_at < cutoff)
            )
            if start_naive is not None:
                statement = statement.where(model.fetched_at >= start_naive)
            if source is not None:
                statement = statement.where(model.source == source)
            statement = statement.order_by(model.id).limit(max(1, int(limit)))
            rows = session.execute(statement).scalars().all()
        return [(_record_row_values if table == "records" else _raw_row_values)(row) for row in rows]

    def delete_archive_batch(
        self,
        *,
        table: str,
        snapshots: list[dict[str, Any]],
        cutoff: datetime,
        checkpoint_id: int,
        cursor_field: str,
        counter_field: str,
        cursor_value: int,
    ) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
        """用短事务删除已归档主库行，同时推进断点游标。

        删除条件再次校验快照中的时间戳，保证归档期间被并发采集更新的行
        不会被误删/误迁移。
        """

        cutoff = to_naive_utc(cutoff)
        now = utcnow_naive()
        deleted: list[tuple[str, str]] = []
        skipped: list[tuple[str, str]] = []
        model = RecordRow if table == "records" else RawRecordRow
        try:
            with self._session_factory() as session:
                for snapshot in snapshots:
                    key = (snapshot["source"], snapshot["external_id"])
                    if table == "records":
                        condition = (
                            (RecordRow.id == snapshot["id"])
                            & (RecordRow.fetched_at == to_naive_utc(snapshot["fetched_at"]))
                            & (RecordRow.updated_at == to_naive_utc(snapshot["updated_at"]))
                            & (RecordRow.fetched_at < cutoff)
                        )
                    else:
                        condition = (
                            (RawRecordRow.id == snapshot["id"])
                            & (RawRecordRow.fetched_at == to_naive_utc(snapshot["fetched_at"]))
                            & (RawRecordRow.status == snapshot["status"])
                            & (RawRecordRow.fetched_at < cutoff)
                        )
                        if snapshot.get("raw_path") is None:
                            condition = condition & RawRecordRow.raw_path.is_(None)
                        else:
                            condition = condition & (
                                RawRecordRow.raw_path == snapshot["raw_path"]
                            )
                    result = session.execute(
                        delete(model)
                        .where(condition)
                        .execution_options(synchronize_session=False)
                    )
                    if result.rowcount and result.rowcount > 0:
                        deleted.append(key)
                    else:
                        skipped.append(key)

                counter_column = getattr(ArchiveCheckpointRow, counter_field)
                session.execute(
                    update(ArchiveCheckpointRow)
                    .where(ArchiveCheckpointRow.id == checkpoint_id)
                    .values(
                        {
                            getattr(ArchiveCheckpointRow, cursor_field): int(cursor_value),
                            counter_column: counter_column + len(deleted),
                            ArchiveCheckpointRow.updated_at: now,
                        }
                    )
                )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(
                f"归档主库删除失败 table={table} checkpoint={checkpoint_id} error={exc}"
            ) from exc
        return deleted, skipped

    # ---- 归档设置与跨库访问 -------------------------------------------

    def _ensure_archive_state(self) -> None:
        if self._archive_state_loaded:
            return
        self._archive_state_loaded = True
        settings: ArchiveSettingsRow | None = None
        try:
            with Session(self.engine) as session:
                settings = session.get(ArchiveSettingsRow, 1)
        except SQLAlchemyError:
            settings = None

        if self._archive_dir_override is not None:
            override = str(self._archive_dir_override)
            if settings is not None and str(Path(settings.archive_dir)) == override:
                cold = settings.cold_raw_dir
                retention = settings.retention_days
            else:
                cold = str(self._archive_dir_override / "raw")
                retention = settings.retention_days if settings is not None else 0
            self._archive_state = {
                "enabled": True,
                "archive_dir": override,
                "cold_raw_dir": cold,
                "retention_days": retention,
            }
        elif settings is not None:
            self._archive_state = {
                "enabled": True,
                "archive_dir": settings.archive_dir,
                "cold_raw_dir": settings.cold_raw_dir,
                "retention_days": settings.retention_days,
            }
        else:
            self._archive_state = None

    @property
    def archive_enabled(self) -> bool:
        self._ensure_archive_state()
        return self._archive_state is not None

    @property
    def archive_dir(self) -> Path | None:
        self._ensure_archive_state()
        if self._archive_state is None:
            return None
        return Path(self._archive_state["archive_dir"])

    @property
    def cold_raw_dir(self) -> Path | None:
        self._ensure_archive_state()
        if self._archive_state is None:
            return None
        return Path(self._archive_state["cold_raw_dir"])

    def archive_store(self) -> ArchiveStore | None:
        self._ensure_archive_state()
        if self._archive_state is None:
            return None
        if self._archive_store is None:
            self._archive_store = ArchiveStore(
                self._archive_state["archive_dir"],
                cold_raw_dir=self._archive_state["cold_raw_dir"],
            )
        return self._archive_store

    def stats(self, *, include_archive: bool = True) -> dict[str, Any]:
        """按源汇总 records、raw 成功/失败数量与最近抓取时间。

        归档启用时统计会自动合并归档分片，并按 ``(source, external_id)``
        去重；热库中重新采集过的记录优先。未启用归档时走原来的单库 SQL，
        输出与旧版本完全一致。``include_archive=False`` 可只看主库热数据。
        """

        store = self.archive_store() if include_archive else None
        if store is None:
            return self._stats_hot_only()
        return self._stats_with_archive(store)

    def _stats_hot_only(self) -> dict[str, Any]:
        with Session(self.engine) as session:
            record_counts = dict(
                session.execute(
                    select(RecordRow.source, func.count()).group_by(RecordRow.source)
                ).all()
            )
            raw_counts = {
                (source, status): int(count)
                for source, status, count in session.execute(
                    select(RawRecordRow.source, RawRecordRow.status, func.count()).group_by(
                        RawRecordRow.source, RawRecordRow.status
                    )
                ).all()
            }
            last_seen = dict(
                session.execute(
                    select(RawRecordRow.source, func.max(RawRecordRow.fetched_at)).group_by(
                        RawRecordRow.source
                    )
                ).all()
            )
        return _build_stats(record_counts, raw_counts, last_seen, {"enabled": False})

    def _stats_with_archive(self, store: ArchiveStore) -> dict[str, Any]:
        with Session(self.engine) as session:
            hot_record_keys = {
                (source, external_id)
                for source, external_id in session.execute(
                    select(RecordRow.source, RecordRow.external_id)
                ).all()
            }
            raw_state = {
                (source, external_id): status
                for source, external_id, status in session.execute(
                    select(RawRecordRow.source, RawRecordRow.external_id, RawRecordRow.status)
                ).all()
            }
            last_seen = dict(
                session.execute(
                    select(RawRecordRow.source, func.max(RawRecordRow.fetched_at)).group_by(
                        RawRecordRow.source
                    )
                ).all()
            )

        archived_record_keys = store.fetch_keys("records")
        archived_raw_rows = store.fetch_raw_states()
        archived_raw_keys = {
            (row["source"], row["external_id"]) for row in archived_raw_rows
        }

        record_keys = set(hot_record_keys)
        record_keys.update(archived_record_keys)
        for row in archived_raw_rows:
            key = (row["source"], row["external_id"])
            if key not in raw_state:
                raw_state[key] = row["status"]
            last = row["fetched_at"]
            current = last_seen.get(row["source"])
            if current is None or (last is not None and last > current):
                last_seen[row["source"]] = last

        record_counts: dict[str, int] = {}
        for source, _external_id in record_keys:
            record_counts[source] = record_counts.get(source, 0) + 1
        raw_counts: dict[tuple[str, str], int] = {}
        for (source, _external_id), status in raw_state.items():
            key = (source, status)
            raw_counts[key] = raw_counts.get(key, 0) + 1

        archive_info = {
            "enabled": True,
            "archive_dir": str(self.archive_dir),
            "cold_raw_dir": str(self.cold_raw_dir),
            "shards": len(store.iter_shards()),
            "records": len(archived_record_keys),
            "raw": len(archived_raw_keys),
        }
        return _build_stats(record_counts, raw_counts, last_seen, archive_info)

    def close(self) -> None:
        if self._archive_store is not None:
            self._archive_store.close()
            self._archive_store = None
        self.engine.dispose()


def _build_stats(
    record_counts: dict[str, int],
    raw_counts: dict[tuple[str, str], int],
    last_seen: dict[str, datetime | None],
    archive_info: dict[str, Any],
) -> dict[str, Any]:
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
    for (name, status), count in raw_counts.items():
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
    return {"sources": rows, "totals": totals, "archive": archive_info}


def _record_row_values(row: RecordRow) -> dict[str, Any]:
    return {column.name: getattr(row, column.name) for column in RecordRow.__table__.columns}


def _raw_row_values(row: RawRecordRow) -> dict[str, Any]:
    return {column.name: getattr(row, column.name) for column in RawRecordRow.__table__.columns}


def _record_from_values(values: dict[str, Any]) -> Record:
    return Record(
        source=values["source"],
        external_id=values["external_id"],
        title=values["title"],
        category=values.get("category"),
        tags=list(values.get("tags") or []),
        published_at=values.get("published_at"),
        content=values.get("content") or "",
        url=values.get("url") or "",
        fetched_at=values.get("fetched_at"),
    )


def _raw_from_values(values: dict[str, Any]) -> RawRecord:
    return RawRecord(
        source=values["source"],
        external_id=values["external_id"],
        url=values["url"],
        http_status=values.get("http_status"),
        fetched_at=values["fetched_at"],
        status=values["status"],
        error=values.get("error"),
        raw_path=values.get("raw_path"),
    )


def _dt(value: datetime | None) -> datetime:
    if value is None:
        return datetime.min
    return to_naive_utc(value)


def _create_engine(path: Path) -> Engine:
    from sqlalchemy import create_engine

    if str(path) == ":memory:":
        return create_engine("sqlite://", future=True)
    return create_engine(f"sqlite:///{path}", future=True)
