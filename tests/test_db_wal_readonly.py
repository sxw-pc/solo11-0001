"""WAL 读写并发与只读连接：采集写入不阻塞 db stats，老库平滑切换。"""

from __future__ import annotations

import threading
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from crawl_flow.models import Record
from crawl_flow.storage.db import Database


def _record(external_id: str) -> Record:
    return Record(
        source="demo_api",
        external_id=external_id,
        title="标题",
        content="正文",
    )


def test_wal_mode_enabled_for_new_database(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl.db"
    db = Database(db_path)
    db.init()
    try:
        with db.engine.connect() as conn:
            assert conn.execute(text("PRAGMA journal_mode")).scalar() == "wal"
        db.upsert_record(_record("api-1"))
        # 有未落盘的 WAL 帧时侧车文件存在。
        assert db_path.with_name("crawl.db-wal").exists()
    finally:
        db.close()
    # WAL 是持久化属性：即使最后一个连接关闭时把帧 checkpoint 回主库，
    # 重新打开仍是 wal 模式，老数据完好。
    reopened = Database(db_path)
    try:
        with reopened.engine.connect() as conn:
            assert conn.execute(text("PRAGMA journal_mode")).scalar() == "wal"
        assert reopened.count_records("demo_api") == 1
    finally:
        reopened.close()


def test_legacy_rollback_journal_database_switches_to_wal(tmp_path: Path) -> None:
    """老库（默认 delete 日志模式）首次被新代码打开即切到 WAL，数据无损。"""

    db_path = tmp_path / "legacy.db"
    legacy = Database(db_path)
    legacy.init()
    legacy.upsert_record(_record("api-1"))
    with legacy.engine.begin() as conn:
        conn.execute(text("PRAGMA journal_mode=DELETE"))
    legacy.close()
    assert not db_path.with_name("legacy.db-wal").exists()

    migrated = Database(db_path)
    try:
        with migrated.engine.connect() as conn:
            assert conn.execute(text("PRAGMA journal_mode")).scalar() == "wal"
        assert migrated.count_records("demo_api") == 1
        migrated.upsert_record(_record("api-1"))  # 老数据仍可正常 upsert
        assert migrated.count_records("demo_api") == 1
        migrated.upsert_record(_record("api-2"))
        assert migrated.count_records("demo_api") == 2
    finally:
        migrated.close()


def test_reader_not_blocked_during_open_write_transaction(tmp_path: Path) -> None:
    """写事务未提交期间，只读 stats 必须立刻返回（WAL 快照读）。"""

    db_path = tmp_path / "crawl.db"
    writer = Database(db_path)
    writer.init()
    writer.upsert_record(_record("api-1"))

    read_db = Database(db_path, read_only=True)
    read_db.init()
    barrier = threading.Event()
    errors: list[Exception] = []

    def hold_write_transaction() -> None:
        session = writer._session_factory()
        try:
            session.execute(
                text("INSERT INTO records (source, external_id, title, tags, content, "
                     "url, fetched_at, updated_at) VALUES "
                     "('demo_api','api-hold','hold','[]','', '', CURRENT_TIMESTAMP, "
                     "CURRENT_TIMESTAMP)")
            )
            session.flush()
            barrier.set()
            threading.Event().wait(1.5)  # 保持事务打开
            session.rollback()
        except Exception as exc:  # pragma: no cover - 仅用于暴露线程内错误
            errors.append(exc)
            barrier.set()
        finally:
            session.close()

    thread = threading.Thread(target=hold_write_transaction)
    thread.start()
    try:
        assert barrier.wait(timeout=5)
        # 写事务挂起时反复只读统计，都应在很短时间内返回且读到已提交数据。
        import time as _time

        for _ in range(5):
            started = _time.monotonic()
            stats = read_db.stats()
            assert _time.monotonic() - started < 1.0
            assert stats["totals"]["records"] == 1
    finally:
        thread.join(timeout=10)
        read_db.close()
        writer.close()
    assert errors == []


def test_readonly_connection_rejects_writes(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl.db"
    writer = Database(db_path)
    writer.init()
    writer.close()

    reader = Database(db_path, read_only=True)
    try:
        with pytest.raises(OperationalError, match="readonly"):
            with reader.engine.begin() as conn:
                conn.execute(text("CREATE TABLE should_fail(x int)"))
    finally:
        reader.close()


def test_readonly_does_not_create_missing_file(tmp_path: Path) -> None:
    missing = tmp_path / "ghost.db"
    with pytest.raises(Exception, match="unable to open database file"):
        Database(missing, read_only=True).engine.connect()
    assert not missing.exists()
