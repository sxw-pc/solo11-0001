"""日期落盘链路：统一 UTC 入库、raw 快照与库内 fetched_at 一致、重复采集稳定。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from crawl_flow.config import FieldMapping, SourceConfig
from crawl_flow.models import Record, RecordRow
from crawl_flow.pipeline.normalize import normalize_record
from crawl_flow.storage.db import Database, to_naive_utc
from crawl_flow.storage.raw_store import RawStore

TZ_PLUS8 = timezone(timedelta(hours=8))
TZ_MINUS5 = timezone(timedelta(hours=-5))


def _source() -> SourceConfig:
    return SourceConfig(
        name="date_demo",
        type="html",
        base_url="http://127.0.0.1:1",
        field_map=FieldMapping(),
    )


def test_to_naive_utc_converts_aware_and_keeps_naive() -> None:
    aware = datetime(2024, 3, 1, 9, 30, tzinfo=TZ_PLUS8)
    assert to_naive_utc(aware) == datetime(2024, 3, 1, 1, 30)
    naive = datetime(2024, 3, 1, 1, 30)
    assert to_naive_utc(naive) == naive


def test_aware_published_at_stored_as_utc_wall_clock(db: Database) -> None:
    source = _source()
    fetched_at = datetime(2024, 3, 1, 1, 30, tzinfo=timezone.utc)
    record = normalize_record(
        source,
        "d-1",
        {"title": "标题", "published_at": "2024-03-01T09:30:00+08:00"},
        detail_url="http://detail.test/d-1",
        fetched_at=fetched_at,
    )
    db.upsert_record(record)

    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "d-1")
        ).scalar_one()
    # 旧实现会存成 09:30（丢 tzinfo 的固定 8 小时偏差），现在必须是 UTC 01:30
    assert row.published_at == datetime(2024, 3, 1, 1, 30)
    assert row.published_at.tzinfo is None


def test_negative_offset_stored_as_utc_crossing_midnight(db: Database) -> None:
    source = _source()
    record = normalize_record(
        source,
        "d-2",
        {"title": "标题", "published_at": "2026-11-10T20:00:00-04:00"},
        detail_url="http://detail.test/d-2",
        fetched_at=datetime(2026, 11, 11, 0, 0, tzinfo=timezone.utc),
    )
    db.upsert_record(record)

    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "d-2")
        ).scalar_one()
    # -04:00 的 20:00 等于次日 UTC 00:00，日期必须跨到 11 日
    assert row.published_at == datetime(2026, 11, 11, 0, 0)


def test_naive_local_format_treated_as_utc_wall_clock(db: Database) -> None:
    source = _source()
    record = normalize_record(
        source,
        "d-3",
        {"title": "标题", "published_at": "2024年03月02日 10时20分"},
        detail_url="http://detail.test/d-3",
        fetched_at=datetime(2024, 3, 2, 10, 20, tzinfo=timezone.utc),
    )
    db.upsert_record(record)

    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "d-3")
        ).scalar_one()
    assert row.published_at == datetime(2024, 3, 2, 10, 20)


def test_unparseable_published_at_stored_as_null(db: Database) -> None:
    source = _source()
    record = normalize_record(
        source,
        "d-4",
        {"title": "标题", "published_at": "不是日期"},
        detail_url="http://detail.test/d-4",
        fetched_at=datetime(2024, 3, 2, tzinfo=timezone.utc),
    )
    db.upsert_record(record)

    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "d-4")
        ).scalar_one()
    assert row.published_at is None


def test_repeated_upserts_keep_stable_published_at(db: Database) -> None:
    source = _source()
    raw_fields = {"title": "标题", "published_at": "2024-03-01T09:30:00+08:00"}
    for _ in range(3):
        record = normalize_record(
            source,
            "d-5",
            raw_fields,
            detail_url="http://detail.test/d-5",
            fetched_at=datetime(2024, 3, 1, 1, 30, tzinfo=timezone.utc),
        )
        db.upsert_record(record)

    with Session(db.engine) as session:
        rows = list(
            session.execute(select(RecordRow).where(RecordRow.external_id == "d-5")).scalars()
        )
    assert len(rows) == 1
    assert rows[0].published_at == datetime(2024, 3, 1, 1, 30)


def test_raw_snapshot_fetched_at_matches_database_instant(
    db: Database, raw_store: RawStore
) -> None:
    source = _source()
    fetched_at = datetime(2024, 3, 1, 1, 30, tzinfo=timezone.utc)
    record = normalize_record(
        source,
        "d-6",
        {"title": "标题", "published_at": "2024-03-01T09:30:00+08:00"},
        detail_url="http://detail.test/d-6",
        fetched_at=fetched_at,
    )
    payload = {
        "source": source.name,
        "external_id": "d-6",
        "fetched_at": fetched_at.isoformat(),
        "body": "<html></html>",
    }
    raw_path = raw_store.save(
        source=source.name,
        external_id="d-6",
        payload=payload,
        fetched_at=fetched_at,
    )
    db.upsert_record(record)

    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "d-6")
        ).scalar_one()

    snapshot = json.loads(Path(raw_path).read_text(encoding="utf-8"))
    snapshot_instant = datetime.fromisoformat(snapshot["fetched_at"]).astimezone(timezone.utc)
    # 快照里是带偏移的 UTC 时刻，库里是 naive UTC 墙钟，两者指向同一瞬间
    assert snapshot_instant.replace(tzinfo=None) == row.fetched_at
    assert snapshot_instant == fetched_at


@pytest.mark.parametrize(
    ("raw_date", "expected_utc"),
    [
        # 正偏移跨 UTC 零点：本地 11 日凌晨 → UTC 10 日晚间
        ("2026-11-11T01:30:00+05:00", datetime(2026, 11, 10, 20, 30)),
        # 负偏移跨 UTC 零点：本地 10 日晚间 → UTC 11 日零点
        ("2026-11-10T20:00:00-04:00", datetime(2026, 11, 11, 0, 0)),
        # Z 后缀不发生日期平移
        ("2026-11-11T00:00:00Z", datetime(2026, 11, 11, 0, 0)),
    ],
)
def test_cross_utc_midnight_storage(
    db: Database, raw_date: str, expected_utc: datetime
) -> None:
    source = _source()
    record = normalize_record(
        source,
        f"mid-{raw_date[:10]}",
        {"title": "标题", "published_at": raw_date},
        detail_url="http://detail.test/mid",
        fetched_at=datetime(2026, 11, 11, tzinfo=timezone.utc),
    )
    db.upsert_record(record)

    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == f"mid-{raw_date[:10]}")
        ).scalar_one()
    assert row.published_at == expected_utc
    assert abs((row.published_at - expected_utc).total_seconds()) <= 1


def test_other_fields_unchanged_by_date_pipeline(db: Database) -> None:
    source = _source()
    record = Record(
        source=source.name,
        external_id="d-7",
        title="  带空格的标题  ",
        category="技术",
        tags=["缓存", "架构"],
        published_at=datetime(2024, 3, 1, 9, 30, tzinfo=TZ_PLUS8),
        content="正文",
        url="http://example.test/d-7",
        fetched_at=datetime(2024, 3, 1, 1, 30, tzinfo=timezone.utc),
    )
    db.upsert_record(record)

    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "d-7")
        ).scalar_one()
    assert row.title == "  带空格的标题  "
    assert row.category == "技术"
    assert row.tags == ["缓存", "架构"]
    assert row.content == "正文"
    assert row.url == "http://example.test/d-7"
