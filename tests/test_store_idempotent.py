"""存储幂等：同一源重复采集行数不变、updated_at 更新。"""

from __future__ import annotations

import time
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from crawl_flow.config import SourceConfig
from crawl_flow.models import RecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def _updated_at(db: Database, source: str) -> list[datetime]:
    with Session(db.engine) as session:
        return list(
            session.execute(
                select(RecordRow.updated_at)
                .where(RecordRow.source == source)
                .order_by(RecordRow.external_id)
            ).scalars()
        )


def test_second_run_updates_but_does_not_duplicate(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
) -> None:
    first = run_source(api_source, db, raw_store, limit=5)
    assert (first.ok, first.failed, first.skipped) == (5, 0, 10)
    assert db.count_records("demo_api") == 5
    assert db.count_raw("demo_api") == 5
    first_times = _updated_at(db, "demo_api")

    time.sleep(0.02)
    second = run_source(api_source, db, raw_store, limit=5)
    assert (second.ok, second.failed) == (5, 0)

    assert db.count_records("demo_api") == 5
    assert db.count_raw("demo_api") == 5
    second_times = _updated_at(db, "demo_api")
    assert len(second_times) == 5
    assert all(after > before for before, after in zip(first_times, second_times, strict=True))

    files = sorted(raw_store.root.rglob("*.json"))
    assert len(files) == 5


def test_records_store_normalized_fields(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
) -> None:
    run_source(api_source, db, raw_store, limit=2)
    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "api-2")
        ).scalar_one()

    assert row.source == "demo_api"
    assert row.title == "开源数据库性能基准测试汇总"
    assert row.category == "技术"
    assert row.tags == ["数据库", "性能", "基线"]
    # 2024-03-02T09:30:00+08:00 换算到 UTC 墙钟时间入库
    assert row.published_at == datetime(2024, 3, 2, 1, 30)
    assert "三款开源数据库" in row.content
    assert row.url.endswith("/api/items/api-2")
    assert row.fetched_at is not None
    assert row.updated_at is not None
