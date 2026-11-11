"""并发与事务原子性：多线程写入不报锁错误、不留脏数据、单条失败隔离。

说明：高并发用例主要跑在 ``:memory:`` 库上（StaticPool 共享连接 + Database
实例锁串行化），文件库并发由小规模用例与外部写锁用例覆盖，避免在
fsync 较慢的环境上拖慢测试套件。
"""

from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from crawl_flow.errors import StoreError
from crawl_flow.models import RawRecord, Record, RecordRow
from crawl_flow.storage import db as db_module
from crawl_flow.storage.db import SQLITE_BUSY_TIMEOUT_SECONDS, Database


def _record(external_id: str, title: str | None = None, source: str = "concurrent") -> Record:
    return Record(
        source=source,
        external_id=external_id,
        title=title or f"title-{external_id}",
        content=f"content-{external_id}",
        url=f"https://example.com/{external_id}",
    )


def _raw(external_id: str, source: str = "concurrent") -> RawRecord:
    return RawRecord(
        source=source,
        external_id=external_id,
        url=f"https://example.com/{external_id}",
        http_status=200,
        fetched_at=datetime.now(timezone.utc),
        status="ok",
    )


def _run_threads(threads: list[threading.Thread]) -> None:
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    assert all(not thread.is_alive() for thread in threads), "存在未结束的线程（疑似死锁）"


def _hammer(db: Database, workers: int, per_worker: int, *, with_raw: bool) -> list[BaseException]:
    errors: list[BaseException] = []

    def worker(worker_id: int) -> None:
        try:
            for i in range(per_worker):
                external_id = f"w{worker_id}-{i}"
                db.upsert_record(_record(external_id))
                if with_raw:
                    db.upsert_raw(_raw(external_id))
        except BaseException as exc:  # noqa: BLE001 - 汇总线程异常统一断言
            errors.append(exc)

    _run_threads([threading.Thread(target=worker, args=(n,)) for n in range(workers)])
    return errors


def _assert_rows_integrity(db: Database, expected: int) -> None:
    with Session(db.engine) as session:
        rows = session.execute(select(RecordRow)).scalars().all()
    assert len(rows) == expected
    for row in rows:  # 每条行内容完整，不存在写了一半的脏数据
        assert row.title == f"title-{row.external_id}"
        assert row.content == f"content-{row.external_id}"


def test_busy_timeout_configured(db: Database) -> None:
    """引擎连接必须带 busy timeout，锁竞争时等待而不是立即失败。"""

    with db.engine.connect() as conn:
        value = conn.exec_driver_sql("PRAGMA busy_timeout").scalar_one()
    assert value == SQLITE_BUSY_TIMEOUT_SECONDS * 1000


def test_concurrent_writers_file_db(db: Database) -> None:
    """文件库多线程 upsert：无 database is locked，行数与内容精确。"""

    workers, per_worker = 3, 4
    errors = _hammer(db, workers, per_worker, with_raw=True)

    assert errors == []
    assert db.count_records("concurrent") == workers * per_worker
    assert db.count_raw("concurrent") == workers * per_worker
    assert db.count_raw("concurrent", "ok") == workers * per_worker
    _assert_rows_integrity(db, workers * per_worker)


def test_concurrent_writers_memory_db_high_volume() -> None:
    """:memory: 库跨线程共享同一连接（StaticPool），高并发写入互相可见且无错。"""

    db = Database(":memory:")
    db.init()
    workers, per_worker = 6, 20
    try:
        errors = _hammer(db, workers, per_worker, with_raw=True)
        assert errors == []
        assert db.count_records("concurrent") == workers * per_worker
        assert db.count_raw("concurrent") == workers * per_worker
        _assert_rows_integrity(db, workers * per_worker)
    finally:
        db.close()


def test_concurrent_upsert_same_key_stays_single_row() -> None:
    """多线程对同一 (source, external_id) upsert：最终仍只有一行。"""

    db = Database(":memory:")
    db.init()
    workers, per_worker = 6, 10
    barrier = threading.Barrier(workers)
    errors: list[BaseException] = []

    def worker(worker_id: int) -> None:
        try:
            barrier.wait(timeout=10)
            for i in range(per_worker):
                db.upsert_record(_record("same-id", title=f"t{worker_id}-{i}"))
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    try:
        _run_threads([threading.Thread(target=worker, args=(n,)) for n in range(workers)])
        assert errors == []
        assert db.count_records("concurrent") == 1
    finally:
        db.close()


def test_upsert_waits_out_external_write_lock(tmp_path: Path) -> None:
    """外部连接持有写锁时，upsert 等待锁释放后成功，而不是报 database is locked。"""

    db_path = tmp_path / "locked.db"
    db = Database(db_path)
    db.init()

    hold_seconds = 1.5
    lock_held = threading.Event()
    lock_errors: list[BaseException] = []

    def hold_and_release() -> None:
        try:
            # 锁持有连接必须在本线程内创建与释放（sqlite3 线程亲和性）
            locker = sqlite3.connect(db_path, isolation_level=None)
            locker.execute("BEGIN IMMEDIATE")
            lock_held.set()
            time.sleep(hold_seconds)
            locker.execute("COMMIT")
            locker.close()
        except BaseException as exc:  # noqa: BLE001
            lock_errors.append(exc)

    releaser = threading.Thread(target=hold_and_release)
    releaser.start()
    assert lock_held.wait(timeout=10), "外部写锁未能建立"
    started = time.perf_counter()
    try:
        db.upsert_record(_record("waiting-id"))
    finally:
        releaser.join(timeout=30)
        db.close()

    assert lock_errors == []
    elapsed = time.perf_counter() - started
    assert elapsed >= hold_seconds, "upsert 没有等待写锁（busy timeout 未生效）"

    db = Database(db_path)
    try:
        assert db.count_records("concurrent") == 1
    finally:
        db.close()


def test_failed_statement_rolls_back_leaving_no_partial_row(db: Database) -> None:
    """同一事务里后一条语句失败 → 整体回滚，已执行的插入也不留脏数据。"""

    values = {
        "source": "concurrent",
        "external_id": "tx-1",
        "title": "t",
        "tags": [],
        "content": "",
        "url": "https://example.com/tx-1",
        "fetched_at": datetime.now(timezone.utc),
        "updated_at": datetime.now(timezone.utc),
    }
    with pytest.raises(SQLAlchemyError):
        with Session(db.engine) as session:
            session.execute(db_module.sqlite_insert(RecordRow).values(**values))
            session.execute(db_module.sqlite_insert(RecordRow).values(**values))  # 唯一键冲突
            session.commit()

    assert db.count_records("concurrent") == 0


def test_single_item_failure_isolated_from_others(
    db: Database, monkeypatch: pytest.MonkeyPatch
) -> None:
    """单条 upsert 抛 StoreError：不留该行数据，后续条目照常写入。"""

    real_sqlite_insert = db_module.sqlite_insert
    calls = {"n": 0}

    def flaky_insert(row_cls: type) -> object:
        calls["n"] += 1
        if calls["n"] == 3:  # 第三条记录在事务内失败
            raise SQLAlchemyError("simulated mid-transaction failure")
        return real_sqlite_insert(row_cls)

    monkeypatch.setattr(db_module, "sqlite_insert", flaky_insert)

    for i in range(5):
        if i == 2:
            with pytest.raises(StoreError):
                db.upsert_record(_record(f"item-{i}"))
        else:
            db.upsert_record(_record(f"item-{i}"))

    assert db.count_records("concurrent") == 4
    with Session(db.engine) as session:
        missing = session.execute(
            select(RecordRow).where(RecordRow.external_id == "item-2")
        ).scalar_one_or_none()
    assert missing is None
