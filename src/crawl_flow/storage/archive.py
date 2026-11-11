"""冷热分层：按时间分片的归档库、原始响应冷存储与断点续传。

设计要点：

- 归档库按 ``fetched_at`` 的月份分片（``archive-YYYY-MM.db``），避免源多时
  产生过多小文件，同时让按时间范围的查询只扫描相关分片。
- 原始响应文件从热目录复制到 ``{archive_dir}/raw``，主库确认删除后才移除热文件。
- 每个批次先在主库外完成归档写入与完整性校验，最后用一个短事务删除主库行；
  主库事务只做 ``DELETE`` 与断点游标更新，锁表时间远小于 2 秒。
- 所有归档写入按 ``(source, external_id)`` 幂等，中断后重跑可从游标继续，
  不会重复归档或丢失数据。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

from sqlalchemy import Engine, create_engine, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError

from crawl_flow.errors import StoreError
from crawl_flow.models import RawRecordRow, RecordRow
from crawl_flow.storage.raw_store import safe_external_id

ARCHIVE_DB_PREFIX = "archive-"
ARCHIVE_DB_SUFFIX = ".db"

RECORD_ARCHIVE_FIELDS = (
    "source",
    "external_id",
    "title",
    "category",
    "tags",
    "published_at",
    "content",
    "url",
    "fetched_at",
    "updated_at",
)

RAW_ARCHIVE_FIELDS = (
    "source",
    "external_id",
    "url",
    "http_status",
    "fetched_at",
    "status",
    "error",
    "raw_path",
)

RAW_STATE_FIELDS = ("source", "external_id", "status", "fetched_at")


def to_naive_utc(value: datetime) -> datetime:
    """把 datetime 统一成 SQLite 中存储的 naive UTC。"""

    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def month_key(value: datetime) -> str:
    return to_naive_utc(value).strftime("%Y-%m")


def shard_filename(month: str) -> str:
    return f"{ARCHIVE_DB_PREFIX}{month}{ARCHIVE_DB_SUFFIX}"


def _normalize_value(value: Any) -> Any:
    if isinstance(value, datetime):
        return to_naive_utc(value).strftime("%Y-%m-%d %H:%M:%S.%f")
    if isinstance(value, list):
        return [str(item) for item in value]
    return value


def _digest(values: dict[str, Any], fields: tuple[str, ...]) -> str:
    payload = {field: _normalize_value(values.get(field)) for field in fields}
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def record_digest(values: dict[str, Any]) -> str:
    return _digest(values, RECORD_ARCHIVE_FIELDS)


def raw_digest(values: dict[str, Any]) -> str:
    return _digest(values, RAW_ARCHIVE_FIELDS)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def record_archive_values(snapshot: dict[str, Any]) -> dict[str, Any]:
    """主库行快照 → 归档库行（去掉自增 id）。"""

    return {
        "source": snapshot["source"],
        "external_id": snapshot["external_id"],
        "title": snapshot["title"],
        "category": snapshot.get("category"),
        "tags": list(snapshot.get("tags") or []),
        "published_at": snapshot.get("published_at"),
        "content": snapshot.get("content") or "",
        "url": snapshot.get("url") or "",
        "fetched_at": snapshot["fetched_at"],
        "updated_at": snapshot["updated_at"],
    }


def raw_archive_values(snapshot: dict[str, Any], raw_path: str | None) -> dict[str, Any]:
    return {
        "source": snapshot["source"],
        "external_id": snapshot["external_id"],
        "url": snapshot["url"],
        "http_status": snapshot.get("http_status"),
        "fetched_at": snapshot["fetched_at"],
        "status": snapshot["status"],
        "error": snapshot.get("error"),
        "raw_path": raw_path,
    }


@dataclass
class StagedFile:
    """一次原始响应文件冷迁移的结果。"""

    hot_path: Path
    cold_path: Path
    copied: bool
    hot_existed: bool
    sha256: str | None = None


@dataclass
class ArchiveSummary:
    """归档运行汇总。"""

    scope: str
    cutoff: datetime
    archive_dir: str
    cold_raw_dir: str
    retention_days: int
    records_archived: int = 0
    raw_archived: int = 0
    batches: int = 0
    max_lock_ms: int = 0
    resumed: bool = False
    integrity_ok: bool = True
    dry_run: bool = False
    shards: list[str] = field(default_factory=list)


class ArchiveStore:
    """归档 SQLite 分片与冷 raw 目录的读写封装。"""

    def __init__(
        self,
        archive_dir: str | Path,
        *,
        raw_root: str | Path | None = None,
        cold_raw_dir: str | Path | None = None,
    ) -> None:
        self.archive_dir = Path(archive_dir)
        self.raw_root = Path(raw_root) if raw_root is not None else None
        self.cold_raw_dir = (
            Path(cold_raw_dir) if cold_raw_dir is not None else self.archive_dir / "raw"
        )
        self._engines: dict[str, Engine] = {}

    # ---- 分片管理 -----------------------------------------------------

    def shard_path(self, month: str) -> Path:
        return self.archive_dir / shard_filename(month)

    def engine_for(self, month: str) -> Engine:
        engine = self._engines.get(month)
        if engine is not None:
            return engine
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        path = self.shard_path(month)
        try:
            engine = create_engine(
                f"sqlite:///{path}",
                future=True,
                connect_args={"timeout": 30},
            )
            RecordRow.__table__.create(engine, checkfirst=True)
            RawRecordRow.__table__.create(engine, checkfirst=True)
        except SQLAlchemyError as exc:
            raise StoreError(f"归档库初始化失败 path={path} error={exc}") from exc
        self._engines[month] = engine
        return engine

    def iter_shards(
        self, *, start: datetime | None = None, end: datetime | None = None
    ) -> list[tuple[str, Path]]:
        """返回落在时间范围内的归档分片 ``(month, path)``。"""

        if not self.archive_dir.is_dir():
            return []
        months: list[str] = []
        for path in self.archive_dir.glob(f"{ARCHIVE_DB_PREFIX}*{ARCHIVE_DB_SUFFIX}"):
            month = path.name[len(ARCHIVE_DB_PREFIX) : -len(ARCHIVE_DB_SUFFIX)]
            try:
                datetime.strptime(month, "%Y-%m")
            except ValueError:
                continue
            months.append(month)
        filtered = _filter_months(months, start, end)
        return [(month, self.shard_path(month)) for month in sorted(filtered)]

    def close(self) -> None:
        for engine in self._engines.values():
            engine.dispose()
        self._engines.clear()

    # ---- 写入 ---------------------------------------------------------

    def insert_record(self, values: dict[str, Any]) -> None:
        month = month_key(values["fetched_at"])
        self._upsert(month, RecordRow.__table__, values)

    def insert_raw(self, values: dict[str, Any]) -> None:
        month = month_key(values["fetched_at"])
        self._upsert(month, RawRecordRow.__table__, values)

    def restore_record(self, values: dict[str, Any]) -> None:
        self.insert_record(values)

    def restore_raw(self, values: dict[str, Any]) -> None:
        self.insert_raw(values)

    def _upsert(self, month: str, table: Any, values: dict[str, Any]) -> None:
        engine = self.engine_for(month)
        statement = sqlite_insert(table).values(**values)
        statement = statement.on_conflict_do_update(
            index_elements=["source", "external_id"],
            set_={
                field: statement.excluded[field]
                for field in values
                if field not in {"source", "external_id"}
            },
        )
        try:
            with engine.begin() as connection:
                connection.execute(statement)
        except SQLAlchemyError as exc:
            raise StoreError(
                f"归档写入失败 month={month} source={values.get('source')} "
                f"external_id={values.get('external_id')} error={exc}"
            ) from exc

    def delete_record(self, month: str, key: tuple[str, str]) -> None:
        self._delete(month, RecordRow.__table__, key)

    def delete_raw(self, month: str, key: tuple[str, str]) -> None:
        self._delete(month, RawRecordRow.__table__, key)

    def _delete(self, month: str, table: Any, key: tuple[str, str]) -> None:
        engine = self._engines.get(month)
        if engine is None:
            return
        try:
            with engine.begin() as connection:
                connection.execute(
                    table.delete().where(
                        (table.c.source == key[0]) & (table.c.external_id == key[1])
                    )
                )
        except SQLAlchemyError as exc:
            raise StoreError(f"归档回滚失败 month={month} key={key} error={exc}") from exc

    # ---- 读取 ---------------------------------------------------------

    def fetch_existing_records(
        self, month: str, keys: set[tuple[str, str]]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        return self._fetch_existing(month, RecordRow.__table__, keys, RECORD_ARCHIVE_FIELDS)

    def fetch_existing_raw(
        self, month: str, keys: set[tuple[str, str]]
    ) -> dict[tuple[str, str], dict[str, Any]]:
        return self._fetch_existing(month, RawRecordRow.__table__, keys, RAW_ARCHIVE_FIELDS)

    def _fetch_existing(
        self,
        month: str,
        table: Any,
        keys: set[tuple[str, str]],
        fields: tuple[str, ...],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        engine = self._engines.get(month)
        if engine is None or not keys:
            return {}
        try:
            with engine.connect() as connection:
                rows = connection.execute(
                    select(table).where(
                        table.c.source.in_([key[0] for key in keys])
                        & table.c.external_id.in_([key[1] for key in keys])
                    )
                )
        except SQLAlchemyError as exc:
            raise StoreError(f"归档库读取失败 month={month} error={exc}") from exc
        result: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows.mappings():
            key = (row["source"], row["external_id"])
            if key in keys:
                result[key] = {field: row[field] for field in fields}
        return result

    def fetch_records(
        self,
        *,
        source: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        return self._fetch_rows(
            RecordRow.__table__, RECORD_ARCHIVE_FIELDS, source=source, start=start, end=end
        )

    def fetch_raw(
        self,
        *,
        source: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        return self._fetch_rows(
            RawRecordRow.__table__, RAW_ARCHIVE_FIELDS, source=source, start=start, end=end
        )

    def fetch_raw_states(
        self,
        *,
        source: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """只取 raw 的状态与时间，供统计/计数使用，避免读出正文列。"""

        return self._fetch_rows(
            RawRecordRow.__table__,
            RAW_STATE_FIELDS,
            source=source,
            start=start,
            end=end,
        )

    def fetch_keys(
        self,
        table_name: Literal["records", "raw_records"],
        *,
        source: str | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        status: str | None = None,
    ) -> set[tuple[str, str]]:
        """只取 (source, external_id)，用于跨库精确计数。"""

        table = RecordRow.__table__ if table_name == "records" else RawRecordRow.__table__
        keys: set[tuple[str, str]] = set()
        for month, _path in self.iter_shards(start=start, end=end):
            engine = self._engines.get(month) or self.engine_for(month)
            statement = select(table.c.source, table.c.external_id)
            if source is not None:
                statement = statement.where(table.c.source == source)
            if start is not None:
                statement = statement.where(table.c.fetched_at >= to_naive_utc(start))
            if end is not None:
                statement = statement.where(table.c.fetched_at < to_naive_utc(end))
            if status is not None and table_name == "raw_records":
                statement = statement.where(table.c.status == status)
            try:
                with engine.connect() as connection:
                    rows = connection.execute(statement).all()
            except SQLAlchemyError as exc:
                raise StoreError(f"归档库计数失败 month={month} error={exc}") from exc
            keys.update((row[0], row[1]) for row in rows)
        return keys

    def get_record(self, source: str, external_id: str) -> dict[str, Any] | None:
        return self._get_one(
            RecordRow.__table__, RECORD_ARCHIVE_FIELDS, source, external_id
        )

    def get_raw(self, source: str, external_id: str) -> dict[str, Any] | None:
        return self._get_one(
            RawRecordRow.__table__, RAW_ARCHIVE_FIELDS, source, external_id
        )

    def _get_one(
        self,
        table: Any,
        fields: tuple[str, ...],
        source: str,
        external_id: str,
    ) -> dict[str, Any] | None:
        found: dict[str, Any] | None = None
        for month, _path in self.iter_shards():
            engine = self._engines.get(month) or self.engine_for(month)
            try:
                with engine.connect() as connection:
                    row = connection.execute(
                        select(table).where(
                            (table.c.source == source)
                            & (table.c.external_id == external_id)
                        )
                    ).mappings().first()
            except SQLAlchemyError as exc:
                raise StoreError(f"归档库读取失败 month={month} error={exc}") from exc
            if row is None:
                continue
            candidate = {field: row[field] for field in fields}
            if found is None or candidate["fetched_at"] > found["fetched_at"]:
                found = candidate
        return found

    def _fetch_rows(
        self,
        table: Any,
        fields: tuple[str, ...],
        *,
        source: str | None,
        start: datetime | None,
        end: datetime | None,
    ) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = []
        columns = [table.c[field] for field in fields]
        for month, _path in self.iter_shards(start=start, end=end):
            engine = self._engines.get(month) or self.engine_for(month)
            statement = select(*columns)
            if source is not None:
                statement = statement.where(table.c.source == source)
            if start is not None:
                statement = statement.where(table.c.fetched_at >= to_naive_utc(start))
            if end is not None:
                statement = statement.where(table.c.fetched_at < to_naive_utc(end))
            try:
                with engine.connect() as connection:
                    rows = connection.execute(statement).mappings().all()
            except SQLAlchemyError as exc:
                raise StoreError(f"归档库查询失败 month={month} error={exc}") from exc
            result.extend({field: row[field] for field in fields} for row in rows)
        return result

    def counts(self) -> tuple[
        dict[str, int], dict[tuple[str, str], int], dict[str, datetime | None]
    ]:
        """返回归档分片中的 (records 按源计数, raw 按源/status 计数, 最近抓取时间)。"""

        record_counts: dict[str, int] = {}
        raw_counts: dict[tuple[str, str], int] = {}
        last_seen: dict[str, datetime | None] = {}
        for month, _path in self.iter_shards():
            engine = self._engines.get(month) or self.engine_for(month)
            try:
                with engine.connect() as connection:
                    for source, count in connection.execute(
                        select(RecordRow.__table__.c.source, func.count()).group_by(
                            RecordRow.__table__.c.source
                        )
                    ):
                        record_counts[source] = record_counts.get(source, 0) + int(count)
                    for source, status, count in connection.execute(
                        select(
                            RawRecordRow.__table__.c.source,
                            RawRecordRow.__table__.c.status,
                            func.count(),
                        ).group_by(
                            RawRecordRow.__table__.c.source,
                            RawRecordRow.__table__.c.status,
                        )
                    ):
                        key = (source, status)
                        raw_counts[key] = raw_counts.get(key, 0) + int(count)
                    for source, last in connection.execute(
                        select(
                            RawRecordRow.__table__.c.source,
                            func.max(RawRecordRow.__table__.c.fetched_at),
                        ).group_by(RawRecordRow.__table__.c.source)
                    ):
                        if last is None:
                            continue
                        current = last_seen.get(source)
                        if current is None or last > current:
                            last_seen[source] = last
            except SQLAlchemyError as exc:
                raise StoreError(f"归档统计失败 month={month} error={exc}") from exc
        return record_counts, raw_counts, last_seen

    # ---- 原始文件 -----------------------------------------------------

    def cold_path_for(
        self,
        *,
        source: str,
        external_id: str,
        fetched_at: datetime,
        raw_path: str | None,
    ) -> Path:
        name = (
            Path(raw_path).name
            if raw_path
            else f"{safe_external_id(external_id)}.json"
        )
        date_key = to_naive_utc(fetched_at).strftime("%Y-%m-%d")
        return self.cold_raw_dir / source / date_key / name

    def stage_raw_file(
        self,
        *,
        source: str,
        external_id: str,
        fetched_at: datetime,
        raw_path: str | None,
    ) -> StagedFile | None:
        """把热 raw 文件复制到冷目录；已存在或缺失时也能安全返回。"""

        cold_path = self.cold_path_for(
            source=source,
            external_id=external_id,
            fetched_at=fetched_at,
            raw_path=raw_path,
        )
        hot_path = Path(raw_path) if raw_path else None
        hot_existed = bool(hot_path and hot_path.is_file())
        if hot_existed:
            assert hot_path is not None
            try:
                if _same_path(hot_path, cold_path):
                    return StagedFile(
                        hot_path=hot_path,
                        cold_path=cold_path,
                        copied=False,
                        hot_existed=True,
                        sha256=_file_sha256(cold_path),
                    )
                cold_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path = cold_path.with_name(f"{cold_path.name}.tmp")
                shutil.copyfile(hot_path, temp_path)
                os.replace(temp_path, cold_path)
            except OSError as exc:
                raise StoreError(
                    f"原始响应冷迁移失败 path={hot_path} -> {cold_path} error={exc}"
                ) from exc
            return StagedFile(
                hot_path=hot_path,
                cold_path=cold_path,
                copied=True,
                hot_existed=True,
                sha256=_file_sha256(cold_path),
            )
        if cold_path.is_file():
            # 上一次中断时已经复制过，续跑直接复用；本批并未复制，回滚时保留。
            return StagedFile(
                hot_path=hot_path,
                cold_path=cold_path,
                copied=False,
                hot_existed=False,
                sha256=_file_sha256(cold_path),
            )
        return None

    def rollback_staged(self, staged: StagedFile) -> None:
        """校验失败时清理本批复制出的冷文件。"""

        if staged.copied and staged.cold_path.is_file():
            try:
                staged.cold_path.unlink()
            except OSError:
                pass

    def remove_hot_file(self, staged: StagedFile) -> None:
        """主库删除成功后移除热文件；内容变化过则保留，避免误删新数据。"""

        if not staged.hot_existed or not staged.hot_path.is_file():
            return
        if _same_path(staged.hot_path, staged.cold_path):
            return
        if staged.sha256 is not None:
            try:
                if _file_sha256(staged.hot_path) != staged.sha256:
                    return
            except OSError:
                return
        try:
            staged.hot_path.unlink()
        except OSError:
            pass


# ---- 分片时间过滤 ------------------------------------------------------


def _filter_months(
    months: list[str], start: datetime | None, end: datetime | None
) -> list[str]:
    if start is None and end is None:
        return list(months)
    start_key = None if start is None else month_key(start)
    end_key = None if end is None else month_key(end)
    result = []
    for month in months:
        if start_key is not None and month < start_key:
            continue
        if end_key is not None and month > end_key:
            continue
        result.append(month)
    return result


def _same_path(left: Path, right: Path) -> bool:
    try:
        return left.resolve() == right.resolve()
    except OSError:
        return False


# ---- 归档编排 ----------------------------------------------------------


def archive_cold_data(
    database: Any,
    *,
    archive_dir: str | Path,
    cold_raw_dir: str | Path | None = None,
    cutoff: datetime,
    source: str | None = None,
    start: datetime | None = None,
    retention_days: int = 90,
    batch_size: int = 100,
    dry_run: bool = False,
    restart: bool = False,
    logger: logging.Logger | None = None,
    max_batches: int | None = None,
    batch_hook: Callable[[str, str, list[dict[str, Any]]], None] | None = None,
) -> ArchiveSummary:
    """把 ``fetched_at < cutoff`` 的冷数据迁移到时间分片归档库。

    ``batch_hook(phase, table, snapshots)`` 仅用于测试注入故障/并发写，
    生产调用不传。
    """

    log = logger or logging.getLogger("crawl_flow.archive")
    cutoff = to_naive_utc(cutoff)
    resolved_archive_dir = Path(archive_dir).resolve()
    resolved_cold_raw_dir = (
        Path(cold_raw_dir).resolve() if cold_raw_dir is not None else None
    )
    archive_store = ArchiveStore(
        resolved_archive_dir,
        raw_root=getattr(database, "raw_root", None),
        cold_raw_dir=resolved_cold_raw_dir,
    )
    summary = ArchiveSummary(
        scope=source or "*",
        cutoff=cutoff,
        archive_dir=str(resolved_archive_dir),
        cold_raw_dir=str(archive_store.cold_raw_dir),
        retention_days=retention_days,
        dry_run=dry_run,
    )

    if dry_run:
        records, raw_rows = database.count_archive_candidates(
            cutoff=cutoff, source=source, start=start
        )
        summary.records_archived = records
        summary.raw_archived = raw_rows
        summary.integrity_ok = True
        log.info(
            "archive dry-run scope=%s cutoff=%s records=%d raw=%d",
            summary.scope,
            cutoff.isoformat(),
            records,
            raw_rows,
        )
        return summary

    try:
        database.register_archive(
            archive_dir=str(resolved_archive_dir),
            cold_raw_dir=str(archive_store.cold_raw_dir),
            retention_days=retention_days,
        )
        checkpoint = database.prepare_archive_checkpoint(
            scope=summary.scope,
            archive_dir=str(resolved_archive_dir),
            cutoff=cutoff,
            restart=restart,
        )
        summary.resumed = bool(checkpoint["resumed"])
        summary.cutoff = checkpoint["cutoff"]
        effective_batch = max(1, int(batch_size))

        batch_counter = 0
        for table in ("records", "raw_records"):
            cursor_key = "cursor_records" if table == "records" else "cursor_raw"
            counter_key = "archived_records" if table == "records" else "archived_raw"
            while True:
                if max_batches is not None and batch_counter >= max_batches:
                    log.info(
                        "archive stopped early batches=%d scope=%s", batch_counter, summary.scope
                    )
                    return summary
                snapshots = database.select_archive_batch(
                    table=table,
                    cursor=int(checkpoint[cursor_key]),
                    cutoff=summary.cutoff,
                    source=source,
                    start=start,
                    limit=effective_batch,
                )
                if not snapshots:
                    break
                max_id = max(snapshot["id"] for snapshot in snapshots)

                prepared = _prepare_batch(
                    table=table,
                    snapshots=snapshots,
                    store=archive_store,
                )
                try:
                    _stage_batch(table=table, prepared=prepared, store=archive_store)
                    if not _verify_batch(table=table, prepared=prepared, store=archive_store):
                        summary.integrity_ok = False
                        raise StoreError(
                            "归档完整性校验失败，已回滚本批迁移 "
                            f"table={table} scope={summary.scope}"
                        )
                    if batch_hook is not None:
                        batch_hook("before_delete", table, snapshots)

                    started = time.perf_counter()
                    deleted, skipped = database.delete_archive_batch(
                        table=table,
                        snapshots=snapshots,
                        cutoff=summary.cutoff,
                        checkpoint_id=int(checkpoint["id"]),
                        cursor_field=cursor_key,
                        counter_field=counter_key,
                        cursor_value=max_id,
                    )
                    lock_ms = int((time.perf_counter() - started) * 1000)
                    summary.max_lock_ms = max(summary.max_lock_ms, lock_ms)
                    if lock_ms > 2000:
                        log.warning(
                            "archive main-db lock exceeded budget lock_ms=%d table=%s batch=%d",
                            lock_ms,
                            table,
                            len(snapshots),
                        )
                    batch_counter += 1
                    summary.batches += 1
                    checkpoint[cursor_key] = max_id
                    if table == "records":
                        summary.records_archived += len(deleted)
                        checkpoint[counter_key] = checkpoint.get(counter_key, 0) + len(deleted)
                    else:
                        summary.raw_archived += len(deleted)
                        checkpoint[counter_key] = checkpoint.get(counter_key, 0) + len(deleted)

                    _finalize_batch(
                        table=table,
                        prepared=prepared,
                        deleted_keys=deleted,
                        skipped_keys=skipped,
                        store=archive_store,
                    )
                except StoreError:
                    _safe_rollback(table=table, prepared=prepared, store=archive_store)
                    raise
                except SQLAlchemyError as exc:
                    _safe_rollback(table=table, prepared=prepared, store=archive_store)
                    raise StoreError(f"归档批次失败 table={table} error={exc}") from exc
                except Exception:
                    _safe_rollback(table=table, prepared=prepared, store=archive_store)
                    raise

                if lock_ms > 1500 and effective_batch > 10:
                    effective_batch = max(10, effective_batch // 2)
                    log.info("archive reduced batch size to %d", effective_batch)

        database.finish_archive_checkpoint(int(checkpoint["id"]), status="completed")
        summary.integrity_ok = True
        summary.shards = sorted(
            path.name for _month, path in archive_store.iter_shards()
        )
        log.info(
            "archive summary scope=%s cutoff=%s records=%d raw=%d batches=%d max_lock_ms=%d",
            summary.scope,
            summary.cutoff.isoformat(),
            summary.records_archived,
            summary.raw_archived,
            summary.batches,
            summary.max_lock_ms,
        )
        return summary
    except SQLAlchemyError as exc:
        raise StoreError(f"归档失败 scope={summary.scope} error={exc}") from exc
    finally:
        archive_store.close()


@dataclass
class _PreparedBatch:
    table: str
    snapshots: list[dict[str, Any]]
    values: list[dict[str, Any]]
    keys: list[tuple[str, str]]
    months: list[str]
    preexisting: dict[tuple[str, str], dict[str, Any]]
    staged_files: dict[tuple[str, str], StagedFile] = field(default_factory=dict)


def _prepare_batch(
    *, table: str, snapshots: list[dict[str, Any]], store: ArchiveStore
) -> _PreparedBatch:
    if table == "records":
        values = [record_archive_values(snapshot) for snapshot in snapshots]
        months = [month_key(value["fetched_at"]) for value in values]
        staged_files: dict[tuple[str, str], StagedFile] = {}
    else:
        values = []
        months = []
        staged_files = {}
        try:
            for snapshot in snapshots:
                key = (snapshot["source"], snapshot["external_id"])
                staged = store.stage_raw_file(
                    source=snapshot["source"],
                    external_id=snapshot["external_id"],
                    fetched_at=snapshot["fetched_at"],
                    raw_path=snapshot.get("raw_path"),
                )
                if staged is not None:
                    staged_files[key] = staged
                raw_path = (
                    str(staged.cold_path) if staged is not None else snapshot.get("raw_path")
                )
                value = raw_archive_values(snapshot, raw_path)
                values.append(value)
                months.append(month_key(value["fetched_at"]))
        except StoreError:
            for staged in staged_files.values():
                store.rollback_staged(staged)
            raise

    keys = [(snapshot["source"], snapshot["external_id"]) for snapshot in snapshots]
    preexisting: dict[tuple[str, str], dict[str, Any]] = {}
    # 同一个月可能有多个 key；按月抓取已存在的旧版本，供校验失败时恢复。
    for month in sorted(set(months)):
        keys_in_month = {
            key for key, item_month in zip(keys, months, strict=True) if item_month == month
        }
        if table == "records":
            preexisting.update(store.fetch_existing_records(month, keys_in_month))
        else:
            preexisting.update(store.fetch_existing_raw(month, keys_in_month))

    return _PreparedBatch(
        table=table,
        snapshots=snapshots,
        values=values,
        keys=keys,
        months=months,
        preexisting=preexisting,
        staged_files=staged_files,
    )


def _stage_batch(*, table: str, prepared: _PreparedBatch, store: ArchiveStore) -> None:
    for value in prepared.values:
        if table == "records":
            store.insert_record(value)
        else:
            store.insert_raw(value)


def _safe_rollback(*, table: str, prepared: _PreparedBatch, store: ArchiveStore) -> None:
    try:
        _rollback_batch(table=table, prepared=prepared, store=store)
    except StoreError:
        # 回滚失败不应掩盖原始异常；主库行尚未删除，重跑仍可修复。
        pass


def _verify_batch(*, table: str, prepared: _PreparedBatch, store: ArchiveStore) -> bool:
    # 1) 归档行的内容摘要必须与主库快照一致。
    expected_by_month: dict[str, list[dict[str, Any]]] = {}
    for value, month in zip(prepared.values, prepared.months, strict=True):
        expected_by_month.setdefault(month, []).append(value)

    for month, expected_values in expected_by_month.items():
        keys = {(value["source"], value["external_id"]) for value in expected_values}
        if table == "records":
            existing = store.fetch_existing_records(month, keys)
        else:
            existing = store.fetch_existing_raw(month, keys)
        if len(existing) != len(keys):
            return False
        for value in expected_values:
            key = (value["source"], value["external_id"])
            actual = existing.get(key)
            if actual is None:
                return False
            if table == "records":
                if record_digest(actual) != record_digest(value):
                    return False
            elif raw_digest(actual) != raw_digest(value):
                return False

    # 2) 本批需要迁移的 raw 文件必须已经完整落到冷目录。
    if table == "raw_records":
        for staged in prepared.staged_files.values():
            if not staged.cold_path.is_file():
                return False
            if staged.sha256 is not None:
                try:
                    if _file_sha256(staged.cold_path) != staged.sha256:
                        return False
                except OSError:
                    return False
    return True


def _rollback_batch(*, table: str, prepared: _PreparedBatch, store: ArchiveStore) -> None:
    """撤销本批归档写入与冷文件复制，主库保持原样。"""

    if table == "raw_records":
        for staged in prepared.staged_files.values():
            store.rollback_staged(staged)
    for value, month in zip(prepared.values, prepared.months, strict=True):
        key = (value["source"], value["external_id"])
        old = prepared.preexisting.get(key)
        if old is not None:
            if table == "records":
                store.restore_record(old)
            else:
                store.restore_raw(old)
        else:
            if table == "records":
                store.delete_record(month, key)
            else:
                store.delete_raw(month, key)


def _finalize_batch(
    *,
    table: str,
    prepared: _PreparedBatch,
    deleted_keys: list[tuple[str, str]],
    skipped_keys: list[tuple[str, str]],
    store: ArchiveStore,
) -> None:
    deleted = set(deleted_keys)

    for key in skipped_keys:
        index = prepared.keys.index(key)
        month = prepared.months[index]
        old = prepared.preexisting.get(key)
        if old is not None:
            if table == "records":
                store.restore_record(old)
            else:
                store.restore_raw(old)
        else:
            if table == "records":
                store.delete_record(month, key)
            else:
                store.delete_raw(month, key)
        staged = prepared.staged_files.get(key)
        if staged is not None:
            store.rollback_staged(staged)

    if table == "raw_records":
        for key in deleted:
            staged = prepared.staged_files.get(key)
            if staged is not None:
                store.remove_hot_file(staged)
