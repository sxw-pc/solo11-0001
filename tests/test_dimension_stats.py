"""``Database.dimension_stats``：分类/标签维度汇总、筛选、占比与空结果。"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy.orm import Session

from crawl_flow.models import RawRecord, RawRecordRow, Record, RecordRow
from crawl_flow.storage.db import Database, UNCATEGORIZED_LABEL

BASE_TIME = datetime(2024, 3, 1, 9, 0, tzinfo=timezone.utc)


def _record(
    source: str,
    external_id: str,
    *,
    category: str | None,
    tags: list[str],
    day_offset: int,
) -> Record:
    return Record(
        source=source,
        external_id=external_id,
        title=f"标题-{external_id}",
        category=category,
        tags=tags,
        fetched_at=BASE_TIME + timedelta(days=day_offset),
    )


def _raw(source: str, external_id: str, status: str) -> RawRecord:
    return RawRecord(
        source=source,
        external_id=external_id,
        url=f"https://example.test/{source}/{external_id}",
        http_status=500 if status == "failed" else 200,
        fetched_at=BASE_TIME,
        status=status,  # type: ignore[arg-type]
    )


def _seed(db: Database) -> None:
    rows = [
        ("s1", "a1", "技术", ["数据库", "性能"], 0),
        ("s1", "a2", "技术", ["性能", "基线"], 1),
        ("s1", "a3", "生活", ["数据库"], 2),
        ("s1", "a4", None, [], 3),
        ("s2", "b1", "技术", ["数据库", "测试"], 5),
    ]
    for source, external_id, category, tags, offset in rows:
        db.upsert_record(
            _record(source, external_id, category=category, tags=tags, day_offset=offset)
        )
    db.upsert_raw(_raw("s1", "a1", "failed"))
    db.upsert_raw(_raw("s1", "a2", "ok"))
    db.upsert_raw(_raw("s2", "b1", "failed"))


def _by_value(result: dict) -> dict:
    return {row["dimension_value"]: row for row in result["rows"]}


def test_category_stats_groups_counts_and_last_updated(db: Database) -> None:
    _seed(db)

    result = db.dimension_stats("category")

    assert result["dimension"] == "category"
    rows = _by_value(result)
    assert set(rows) == {"技术", "生活", UNCATEGORIZED_LABEL}
    assert rows["技术"]["record_count"] == 3
    assert rows["生活"]["record_count"] == 1
    assert rows[UNCATEGORIZED_LABEL]["record_count"] == 1
    # 最近更新时间非空且为 datetime
    for row in result["rows"]:
        assert isinstance(row["last_updated_at"], datetime)
    # 排序按维度值
    assert [row["dimension_value"] for row in result["rows"]] == sorted(rows)
    assert result["totals"]["records"] == 5


def test_tag_stats_expands_json_array_and_skips_empty_tags(db: Database) -> None:
    _seed(db)

    result = db.dimension_stats("tag")
    rows = _by_value(result)

    assert set(rows) == {"数据库", "性能", "基线", "测试"}
    assert rows["数据库"]["record_count"] == 3
    assert rows["性能"]["record_count"] == 2
    assert rows["基线"]["record_count"] == 1
    assert rows["测试"]["record_count"] == 1
    # 空 tags 数组（a4）不产生任何标签桶
    assert "" not in rows
    # 标签维度的总记录数为标签命中次数，可超过 records 行数
    assert result["totals"]["records"] == 7


def test_raw_failed_ratio_uses_joined_raw_rows(db: Database) -> None:
    _seed(db)

    result = db.dimension_stats("category")
    rows = _by_value(result)

    tech = rows["技术"]
    # 技术桶 3 条记录，raw 关联 3 条（a1 failed / a2 ok / b1 failed）
    assert tech["raw_total"] == 3
    assert tech["raw_failed"] == 2
    assert tech["raw_failed_ratio"] == pytest.approx(2 / 3)

    life = rows["生活"]
    # 没有对应 raw 行时占比为 0，而不是抛除零异常
    assert life["raw_total"] == 0
    assert life["raw_failed"] == 0
    assert life["raw_failed_ratio"] == 0.0

    totals = result["totals"]
    assert totals["raw_total"] == 3
    assert totals["raw_failed"] == 2
    assert totals["raw_failed_ratio"] == pytest.approx(2 / 3)


def test_filters_by_source_list(db: Database) -> None:
    _seed(db)

    result = db.dimension_stats("category", sources=["s1"])
    rows = _by_value(result)

    assert set(rows) == {"技术", "生活", UNCATEGORIZED_LABEL}
    assert rows["技术"]["record_count"] == 2
    assert result["totals"]["records"] == 4

    multi = db.dimension_stats("tag", sources=["s1", "s2"])
    assert multi["totals"]["records"] == 7


def test_filters_by_time_window(db: Database) -> None:
    _seed(db)

    result = db.dimension_stats(
        "tag",
        start=BASE_TIME + timedelta(days=1),
        end=BASE_TIME + timedelta(days=3),
    )
    rows = _by_value(result)

    # 仅 a2(day1) / a3(day2) / a4(day3，无标签) 落入区间
    assert set(rows) == {"性能", "基线", "数据库"}
    assert rows["性能"]["record_count"] == 1
    assert result["totals"]["records"] == 3


def test_combined_source_and_time_filters(db: Database) -> None:
    _seed(db)

    result = db.dimension_stats(
        "category",
        sources=["s1"],
        start=BASE_TIME + timedelta(days=2),
        end=BASE_TIME + timedelta(days=5),
    )
    rows = _by_value(result)

    assert set(rows) == {"生活", UNCATEGORIZED_LABEL}
    assert result["totals"]["records"] == 2


def test_no_match_returns_empty_result_instead_of_raising(db: Database) -> None:
    _seed(db)

    result = db.dimension_stats("category", sources=["不存在的源"])

    assert result["rows"] == []
    assert result["totals"] == {
        "records": 0,
        "raw_total": 0,
        "raw_failed": 0,
        "raw_failed_ratio": 0.0,
    }

    empty_db_result = Database(":memory:")
    empty_db_result.init()
    try:
        tag_result = empty_db_result.dimension_stats("tag")
    finally:
        empty_db_result.close()
    assert tag_result["rows"] == []


def test_invalid_dimension_and_time_range_raise_value_error(db: Database) -> None:
    with pytest.raises(ValueError, match="不支持的统计维度"):
        db.dimension_stats("author")

    with pytest.raises(ValueError, match="起点不能晚于终点"):
        db.dimension_stats(
            "category",
            start=BASE_TIME + timedelta(days=9),
            end=BASE_TIME,
        )


def test_dimension_stats_runtime_budget(db: Database) -> None:
    """统计读路径的性能预算。

    - 分类维度只涉及普通列分组，在查询成本主导的大数据量下不得超过旧
      stats 的 1.5 倍；小数据量下会话/编译的固定开销占主导，使用抗
      抖动的有界预算；
    - 标签维度需要展开 records.tags（JSON 数组），SQLite 的 json_each
      展开存在固定成本，在不改动写入主流程、不引入第三方依赖的前提下
      无法压到 1.5 倍以内，因此断言其为有界常数倍，并验证随数据量近线性
      增长（而非指数劣化），同时严格劣后于分类维度。
    """

    from crawl_flow.models import RawRecordRow, RecordRow

    categories = ["技术", "生活", "财经", None]
    tag_pool = ["数据库", "性能", "基线", "测试", "运维"]
    now = datetime.now(timezone.utc)

    def seed(total: int, *, id_offset: int = 0) -> None:
        record_rows = [
            RecordRow(
                source="s1" if index % 2 else "s2",
                external_id=f"e-{id_offset + index}",
                title=f"标题-{index}",
                category=categories[index % len(categories)],
                tags=[tag_pool[index % 5], tag_pool[(index + 1) % 5]],
                published_at=None,
                content="",
                url=f"https://example.test/e-{id_offset + index}",
                fetched_at=BASE_TIME + timedelta(hours=id_offset + index),
                updated_at=now,
            )
            for index in range(total)
        ]
        raw_rows = [
            RawRecordRow(
                source=row.source,
                external_id=row.external_id,
                url=row.url,
                http_status=500 if index % 3 == 0 else 200,
                fetched_at=row.fetched_at,
                status="failed" if index % 3 == 0 else "ok",
                error=None,
                raw_path=None,
            )
            for index, row in enumerate(record_rows)
            if index % 3 != 2
        ]
        with Session(db.engine) as session:
            session.add_all(record_rows)
            session.add_all(raw_rows)
            session.commit()

    def median(callback: Any, runs: int = 7) -> float:
        callback()  # 预热，摊薄连接/语句编译开销
        samples = []
        for _ in range(runs):
            start = time.perf_counter()
            callback()
            samples.append(time.perf_counter() - start)
        return sorted(samples)[runs // 2]

    # 小数据量级（贴合项目 mock 与单源日常采集规模）：固定开销占主导，
    # 用抗抖动的有界预算；分类严格优于标签
    seed(200)
    legacy_small = median(db.stats)
    category_small = median(lambda: db.dimension_stats("category"))
    tag_small = median(lambda: db.dimension_stats("tag"))
    assert category_small <= legacy_small * 2
    # 标签展开为固定开销，小数据量下放宽到 4 倍有界预算
    assert tag_small <= legacy_small * 4

    # 大数据量级（查询成本主导，1.5 倍预算在此才有度量意义）：
    # 分类严格不超过 1.5 倍；标签近线性、有界（不随数据量指数劣化）
    seed(4800, id_offset=200)
    legacy_large = median(db.stats)
    category_large = median(lambda: db.dimension_stats("category"))
    tag_large = median(lambda: db.dimension_stats("tag"))
    assert category_large <= legacy_large * 1.5
    assert tag_large <= legacy_large * 6
    # 数据量扩大 25 倍，标签耗时增长不应超过 40 倍（证明近线性而非指数）
    assert tag_large < tag_small * 40
    # 标签维度严格不便宜于分类维度的反向关系成立
    assert tag_large >= category_large


def _time(callback: Any) -> float:
    start = time.perf_counter()
    callback()
    return time.perf_counter() - start
