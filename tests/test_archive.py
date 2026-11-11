"""冷热分层：30 天冷数据归档、跨库查询、断点续传、并发一致性与回滚。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.errors import StoreError
from crawl_flow.models import RawRecord, RawRecordRow, Record, RecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage import archive as archive_module
from crawl_flow.storage.archive import archive_cold_data
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def _now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _seed(
    db: Database,
    raw_store: RawStore,
    *,
    source: str = "demo_api",
    prefix: str = "old",
    count: int = 5,
    fetched_at: datetime | None = None,
    with_raw: bool = True,
) -> list[str]:
    """直接写入指定抓取时间的历史记录与 raw 文件（绕过 upsert 的 updated_at=now）。"""

    stamp = fetched_at or (_now() - timedelta(days=31))
    external_ids: list[str] = []
    with Session(db.engine) as session:
        for index in range(count):
            external_id = f"{prefix}-{index}"
            external_ids.append(external_id)
            raw_path = None
            if with_raw:
                raw_path = raw_store.save(
                    source=source,
                    external_id=external_id,
                    payload={"external_id": external_id, "body": f"old-{index}"},
                    fetched_at=stamp,
                )
            session.add(
                RecordRow(
                    source=source,
                    external_id=external_id,
                    title=f"历史记录 {index}",
                    category="技术",
                    tags=["归档", "冷数据"],
                    published_at=stamp,
                    content=f"历史正文 {index}",
                    url=f"http://example/{external_id}",
                    fetched_at=stamp,
                    updated_at=stamp,
                )
            )
            if with_raw:
                session.add(
                    RawRecordRow(
                        source=source,
                        external_id=external_id,
                        url=f"http://example/{external_id}",
                        http_status=200,
                        fetched_at=stamp,
                        status="ok",
                        error=None,
                        raw_path=raw_path,
                    )
                )
        session.commit()
    return external_ids


def _hot_record(db: Database, raw_store: RawStore, external_id: str, source: str = "demo_api") -> None:
    stamp = _now()
    raw_path = raw_store.save(
        source=source,
        external_id=external_id,
        payload={"external_id": external_id, "body": "hot"},
        fetched_at=stamp,
    )
    db.upsert_record(
        Record(
            source=source,
            external_id=external_id,
            title=f"热记录 {external_id}",
            category="技术",
            tags=["热数据"],
            content="热正文",
            url=f"http://example/{external_id}",
            fetched_at=stamp,
        )
    )
    db.upsert_raw(
        RawRecord(
            source=source,
            external_id=external_id,
            url=f"http://example/{external_id}",
            http_status=200,
            fetched_at=stamp,
            status="ok",
            raw_path=raw_path,
        )
    )


def _main_rows(db: Database) -> tuple[int, int]:
    with Session(db.engine) as session:
        records = len(session.execute(select(RecordRow)).scalars().all())
        raw = len(session.execute(select(RawRecordRow)).scalars().all())
    return records, raw


def _archive_rows(archive_dir: Path, table: str) -> int:
    from sqlalchemy import create_engine

    total = 0
    for path in archive_dir.glob("archive-*.db"):
        engine = create_engine(f"sqlite:///{path}", future=True)
        try:
            with engine.connect() as connection:
                model = RecordRow.__table__ if table == "records" else RawRecordRow.__table__
                total += len(connection.execute(select(model)).all())
        finally:
            engine.dispose()
    return total


def test_archive_30_day_data_cross_db_query_and_raw_paths(tmp_path: Path) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    cutoff = _now() - timedelta(days=30)
    _seed(db, raw_store, count=5, fetched_at=cutoff - timedelta(days=1))
    _hot_record(db, raw_store, "hot-live-1")
    _hot_record(db, raw_store, "hot-live-2")

    assert db.archive_enabled is False  # 默认不开启
    assert db.count_records("demo_api") == 7
    assert db.count_raw("demo_api") == 7

    summary = archive_cold_data(
        db,
        archive_dir=tmp_path / "archive",
        cold_raw_dir=tmp_path / "archive" / "raw",
        cutoff=cutoff,
        source="demo_api",
        retention_days=30,
        batch_size=2,
    )
    assert summary.integrity_ok is True
    assert summary.records_archived == 5
    assert summary.raw_archived == 5
    assert summary.max_lock_ms < 2000
    db.close()

    # 重新打开主库：查询逻辑自动发现归档设置，无需手动切换数据库。
    db = Database(tmp_path / "crawl.db")
    db.init()
    assert db.archive_enabled is True
    hot_count, hot_raw_count = _main_rows(db)
    assert hot_count == 2
    assert hot_raw_count == 2
    assert _archive_rows(tmp_path / "archive", "records") == 5
    assert _archive_rows(tmp_path / "archive", "raw_records") == 5

    # 主库 SQL 查询不到冷数据；跨库 API 能取到全部数据。
    with Session(db.engine) as session:
        assert session.execute(
            select(RecordRow).where(RecordRow.external_id.like("old-%"))
        ).first() is None
    records = db.query_records(source="demo_api")
    assert {record.external_id for record in records} == {
        "old-0",
        "old-1",
        "old-2",
        "old-3",
        "old-4",
        "hot-live-1",
        "hot-live-2",
    }
    assert db.count_records("demo_api") == 7
    assert db.count_raw("demo_api") == 7

    # raw 文件已迁到冷目录，归档库中的 raw_path 同步更新，热文件删除。
    cold_raw = db.get_raw_record("demo_api", "old-0")
    assert cold_raw is not None
    assert cold_raw.raw_path is not None
    cold_path = Path(cold_raw.raw_path)
    assert cold_path.is_file()
    assert (tmp_path / "archive" / "raw") in cold_path.parents
    assert not (tmp_path / "raw" / "demo_api").exists() or not list(
        (tmp_path / "raw" / "demo_api").rglob("old-*.json")
    )
    hot_raw = db.get_raw_record("demo_api", "hot-live-1")
    assert hot_raw is not None and hot_raw.raw_path is not None
    assert Path(hot_raw.raw_path).is_file()
    assert (tmp_path / "raw") in Path(hot_raw.raw_path).parents

    # 统计自动包含归档分片。
    stats = db.stats()
    assert stats["archive"]["enabled"] is True
    assert stats["totals"]["records"] == 7
    assert stats["totals"]["raw_ok"] == 7
    db.close()


def test_archive_shards_by_month_and_time_range_query(tmp_path: Path) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    cutoff = _now() - timedelta(days=30)
    older = cutoff - timedelta(days=70)
    _seed(db, raw_store, prefix="m1", count=2, fetched_at=older, with_raw=False)
    _seed(db, raw_store, prefix="m2", count=2, fetched_at=cutoff - timedelta(days=1), with_raw=False)
    _seed(db, raw_store, prefix="hot", count=1, fetched_at=_now(), with_raw=False)

    archive_cold_data(
        db,
        archive_dir=tmp_path / "archive",
        cutoff=cutoff,
        source="demo_api",
        batch_size=10,
    )
    db.close()

    shards = sorted(path.name for path in (tmp_path / "archive").glob("archive-*.db"))
    assert len(shards) == 2  # 按月份分片，而不是按源分片

    db = Database(tmp_path / "crawl.db")
    db.init()
    # 时间范围查询只命中对应月份分片。
    mid_start = cutoff - timedelta(days=2)
    mid_end = cutoff + timedelta(days=1)
    ranged = db.query_records(source="demo_api", start=mid_start, end=mid_end)
    assert {record.external_id for record in ranged} == {"m2-0", "m2-1"}
    db.close()


def test_resume_after_interruption_no_loss_or_duplicates(tmp_path: Path) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    cutoff = _now() - timedelta(days=30)
    _seed(db, raw_store, count=8, fetched_at=cutoff - timedelta(days=1))

    # 只跑一批就退出，模拟进程中断；checkpoint 保持 running。
    first = archive_cold_data(
        db,
        archive_dir=tmp_path / "archive",
        cutoff=cutoff,
        source="demo_api",
        batch_size=2,
        max_batches=1,
    )
    assert first.records_archived == 2
    assert _main_rows(db)[0] > 0
    db.close()

    db = Database(tmp_path / "crawl.db")
    db.init()
    second = archive_cold_data(
        db,
        archive_dir=tmp_path / "archive",
        cutoff=cutoff,  # 续跑沿用 checkpoint 中的原 cutoff
        source="demo_api",
        batch_size=2,
    )
    assert second.resumed is True
    assert _main_rows(db) == (0, 0)
    assert _archive_rows(tmp_path / "archive", "records") == 8
    assert _archive_rows(tmp_path / "archive", "raw_records") == 8
    assert db.count_records("demo_api") == 8
    assert db.count_raw("demo_api") == 8
    db.close()

    # 已完成任务再次执行不会重复归档。
    db = Database(tmp_path / "crawl.db")
    db.init()
    third = archive_cold_data(
        db,
        archive_dir=tmp_path / "archive",
        cutoff=cutoff,
        source="demo_api",
        batch_size=2,
    )
    assert third.resumed is False
    assert _archive_rows(tmp_path / "archive", "records") == 8
    assert _archive_rows(tmp_path / "archive", "raw_records") == 8
    db.close()


def test_archive_time_range_start_filter(tmp_path: Path) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    cutoff = _now() - timedelta(days=30)
    start = cutoff - timedelta(days=60)
    _seed(db, raw_store, prefix="too-old", count=2, fetched_at=start - timedelta(days=20), with_raw=False)
    _seed(db, raw_store, prefix="in-range", count=2, fetched_at=cutoff - timedelta(days=1), with_raw=False)

    summary = archive_cold_data(
        db,
        archive_dir=tmp_path / "archive",
        cutoff=cutoff,
        start=start,
        source="demo_api",
        batch_size=10,
    )
    assert summary.records_archived == 2

    with Session(db.engine) as session:
        remaining = {
            row.external_id for row in session.execute(select(RecordRow)).scalars().all()
        }
    assert remaining == {"too-old-0", "too-old-1"}  # start 之前的行不在本次范围内
    assert db.count_records("demo_api") == 4  # 跨库仍能查到全部
    db.close()


def test_concurrent_hot_write_is_not_migrated(tmp_path: Path) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    cutoff = _now() - timedelta(days=30)
    _seed(db, raw_store, count=4, fetched_at=cutoff - timedelta(days=1))

    updated: dict[str, Any] = {}

    def hook(phase: str, table: str, snapshots: list[dict[str, Any]]) -> None:
        if phase != "before_delete" or table != "records" or updated:
            return
        # 归档批次已复制但尚未删除主库行时，模拟并发采集把 old-0 刷新成热数据。
        key = "old-0"
        stamp = _now()
        with Session(db.engine) as session:
            session.execute(
                RecordRow.__table__.update()
                .where(
                    (RecordRow.source == "demo_api")
                    & (RecordRow.external_id == key)
                )
                .values(fetched_at=stamp, updated_at=stamp, title="并发刷新")
            )
            session.execute(
                RawRecordRow.__table__.update()
                .where(
                    (RawRecordRow.source == "demo_api")
                    & (RawRecordRow.external_id == key)
                )
                .values(fetched_at=stamp)
            )
            session.commit()
        # 同时写入一条全新热数据。
        with Session(db.engine) as session:
            session.add(
                RecordRow(
                    source="demo_api",
                    external_id="during-archive",
                    title="归档期间新增",
                    category=None,
                    tags=[],
                    published_at=None,
                    content="new",
                    url="http://example/during",
                    fetched_at=stamp,
                    updated_at=stamp,
                )
            )
            session.commit()
        updated["done"] = True

    archive_cold_data(
        db,
        archive_dir=tmp_path / "archive",
        cutoff=cutoff,
        source="demo_api",
        batch_size=10,
        batch_hook=hook,
    )

    # 被并发刷新为热数据的行仍在主库，且没有残留在归档库。
    assert db.get_record("demo_api", "old-0") is not None
    assert db.get_record("demo_api", "during-archive") is not None
    assert _archive_rows(tmp_path / "archive", "records") == 3
    with Session(db.engine) as session:
        hot_ids = {
            row.external_id
            for row in session.execute(select(RecordRow)).scalars().all()
        }
    assert {"old-0", "during-archive"}.issubset(hot_ids)
    assert db.count_records("demo_api") == 5
    db.close()


def test_integrity_failure_rolls_back_migration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    cutoff = _now() - timedelta(days=30)
    _seed(db, raw_store, count=3, fetched_at=cutoff - timedelta(days=1))

    monkeypatch.setattr(archive_module, "_verify_batch", lambda **kwargs: False)
    with pytest.raises(StoreError, match="完整性校验失败"):
        archive_cold_data(
            db,
            archive_dir=tmp_path / "archive",
            cutoff=cutoff,
            source="demo_api",
            batch_size=10,
        )

    # 主库数据与热 raw 文件原样保留；归档库无残留行；冷文件被清理。
    assert _main_rows(db) == (3, 3)
    assert _archive_rows(tmp_path / "archive", "records") == 0
    assert _archive_rows(tmp_path / "archive", "raw_records") == 0
    assert len(list((tmp_path / "raw" / "demo_api").rglob("old-*.json"))) == 3
    cold_files = list((tmp_path / "archive" / "raw").rglob("*.json")) if (
        tmp_path / "archive" / "raw"
    ).exists() else []
    assert cold_files == []
    db.close()

    # 去掉故障注入后可以正常重跑完成归档。
    monkeypatch.undo()
    db = Database(tmp_path / "crawl.db")
    db.init()
    summary = archive_cold_data(
        db,
        archive_dir=tmp_path / "archive",
        cutoff=cutoff,
        source="demo_api",
        batch_size=10,
    )
    assert summary.integrity_ok is True
    assert summary.resumed is True
    assert _main_rows(db) == (0, 0)
    assert _archive_rows(tmp_path / "archive", "records") == 3
    db.close()


def test_cli_archive_and_stats(tmp_path: Path) -> None:
    db_path = tmp_path / "cli.db"
    raw_dir = tmp_path / "raw"
    archive_dir = tmp_path / "archive"
    db = Database(db_path)
    db.init()
    raw_store = RawStore(raw_dir)
    cutoff = _now() - timedelta(days=30)
    _seed(db, raw_store, count=3, fetched_at=cutoff - timedelta(days=1))
    _hot_record(db, raw_store, "hot-cli")
    db.close()

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "archive",
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
            "--archive-dir",
            str(archive_dir),
            "--days",
            "30",
            "--source",
            "demo_api",
            "--batch-size",
            "10",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "records=3" in result.output
    assert "integrity_ok=true" in result.output

    stats = runner.invoke(app, ["db", "stats", "--db", str(db_path)])
    assert stats.exit_code == 0, stats.output
    assert "archive enabled" in stats.output
    assert "TOTAL" in stats.output
    assert "4" in stats.output  # 3 条冷数据 + 1 条热数据在统计中仍然可见

    hot_stats = runner.invoke(app, ["db", "stats", "--db", str(db_path), "--hot-only"])
    assert hot_stats.exit_code == 0, hot_stats.output
    assert "archive enabled" not in hot_stats.output
    assert "1" in hot_stats.output  # 只看主库热数据时仅剩 1 条


def test_collection_and_stats_still_work_after_archive(
    tmp_path: Path, api_source: Any
) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    cutoff = _now() - timedelta(days=30)
    _seed(db, raw_store, count=2, fetched_at=cutoff - timedelta(days=1))
    archive_cold_data(
        db,
        archive_dir=tmp_path / "archive",
        cutoff=cutoff,
        source="demo_api",
        batch_size=10,
    )

    # 原有采集流程照常工作，新采集数据进入热库。
    summary = run_source(api_source, db, raw_store, limit=2)
    assert summary.ok == 2
    assert db.count_records("demo_api") == 4  # 2 归档 + 2 新采集
    stats = db.stats()
    assert stats["totals"]["records"] == 4
    with Session(db.engine) as session:
        # 主库只保留热数据，不含已归档的冷数据。
        rows = session.execute(select(RecordRow)).scalars().all()
        assert len(rows) == 2
        assert all(row.external_id.startswith("api-") for row in rows)
    db.close()
