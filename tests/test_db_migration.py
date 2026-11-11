"""旧版数据库迁移：records 表自动补齐规则引擎的三个质量列。"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.orm import Session

from crawl_flow.models import Record, RecordRow
from crawl_flow.storage.db import Database


def test_init_migrates_legacy_records_table(tmp_path) -> None:
    db_path = tmp_path / "legacy.db"
    # 先用裸 SQL 建一个不含质量列的旧版 records 表。
    legacy = Database(db_path)
    with legacy.engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE records ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "source VARCHAR(128) NOT NULL, external_id VARCHAR(256) NOT NULL, "
                "title VARCHAR(512) NOT NULL, category VARCHAR(128), "
                "tags JSON NOT NULL, published_at DATETIME, content TEXT NOT NULL, "
                "url VARCHAR(2048) NOT NULL, fetched_at DATETIME NOT NULL, "
                "updated_at DATETIME NOT NULL, "
                "UNIQUE(source, external_id))"
            )
        )
    legacy.close()

    # 重新打开并 init，应自动补列且不报错。
    migrated = Database(db_path)
    migrated.init()
    record = Record(source="s", external_id="old-1", title="历史记录")
    migrated.upsert_record(record)

    with Session(migrated.engine) as session:
        row = session.execute(
            RecordRow.__table__.select().where(RecordRow.external_id == "old-1")
        ).one()
        assert row.quality_score == 100.0
        assert row.low_quality is False
        assert row.quality_report == {}
    migrated.close()
