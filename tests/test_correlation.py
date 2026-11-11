"""跨源关联：正常合并、关联字段缺失、多命中冲突合并与类型不匹配。

约定：
- 关联默认关闭，未配置 correlation 的源行为由其余 27 个既有测试保证。
- 所有用例离线运行，复用 conftest 的 mock 站点与临时数据库。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from crawl_flow.config import CorrelationConfig, JoinKey, WideFieldRule, WideTableRule
from crawl_flow.models import CorrelationEventRow, Record, WideRecordRow
from crawl_flow.pipeline.correlation import (
    EVENT_MISSING_FIELD,
    EVENT_NOT_MATCHED,
    EVENT_TYPE_MISMATCH,
    Correlator,
    coerce_join_value,
)
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def _title_join_config(
    remote: str,
    *,
    table: str = "article_wide",
    conflict_policy: str = "merge",
) -> CorrelationConfig:
    return CorrelationConfig(
        remote_source=remote,
        join_keys=[JoinKey(local_field="title", remote_field="title")],
        wide_table=WideTableRule(
            table=table,
            conflict_policy=conflict_policy,
            fields=[
                WideFieldRule(
                    name="title",
                    sources=["demo_api", "demo_html"],
                    strategy="first",
                ),
                WideFieldRule(
                    name="content",
                    sources=["demo_html", "demo_api"],
                    strategy="first",
                ),
                WideFieldRule(
                    name="tags",
                    sources=["demo_api", "demo_html"],
                    strategy="list",
                ),
                WideFieldRule(
                    name="sources",
                    sources=["demo_api", "demo_html"],
                    strategy="concat",
                    field="source",
                    separator=",",
                ),
                WideFieldRule(
                    name="html_url",
                    sources=["demo_html"],
                    strategy="first",
                    field="url",
                    default="",
                ),
            ],
        ),
    )


def _wide_rows(db: Database, table: str) -> list[WideRecordRow]:
    with Session(db.engine) as session:
        return list(
            session.execute(
                select(WideRecordRow)
                .where(WideRecordRow.table_name == table)
                .order_by(WideRecordRow.entity_key)
            ).scalars()
        )


def _events(db: Database, table: str) -> list[CorrelationEventRow]:
    with Session(db.engine) as session:
        return list(
            session.execute(
                select(CorrelationEventRow)
                .where(CorrelationEventRow.table_name == table)
                .order_by(CorrelationEventRow.local_external_id, CorrelationEventRow.event_type)
            ).scalars()
        )


def _record(source: str, external_id: str, **overrides: Any) -> Record:
    payload: dict[str, Any] = {
        "source": source,
        "external_id": external_id,
        "title": f"标题-{external_id}",
        "fetched_at": datetime(2024, 3, 5, tzinfo=timezone.utc),
    }
    payload.update(overrides)
    return Record(**payload)


# ---- 场景一：正常关联 ----


def test_correlation_merges_sources_into_wide_table(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    # 先采集关联源 demo_html，records 表中预留待关联数据
    html_source = make_source("demo_html", "html")
    run_source(html_source, db, raw_store, limit=3)

    api_source = make_source(
        "demo_api",
        correlation=_title_join_config("demo_html"),
    )
    summary = run_source(api_source, db, raw_store, limit=3)

    # 主采集结果与单源管道一致
    assert (summary.ok, summary.failed) == (3, 0)
    assert summary.wide_merged == 3
    assert summary.correlation_failed == 0

    rows = _wide_rows(db, "article_wide")
    assert len(rows) == 3
    # 宽表不产生冗余：原始单源记录各自保留，宽表额外每实体一行
    assert db.count_records("demo_api") == 3
    assert db.count_records("demo_html") == 3
    assert db.count_wide_records("article_wide") == 3
    assert db.count_correlation_events(table_name="article_wide") == 0

    first = rows[0]
    assert first.entity_key == "demo_api:api-1"
    assert first.status == "ok"
    assert first.primary_source == "demo_api"
    assert set(first.sources) == {"demo_api", "demo_html"}
    assert first.fields["title"] == "城市公园春季花期观测报告"
    # content 优先取 demo_html（规则 sources 顺序）
    assert "志愿者" in first.fields["content"]
    # list 策略：两源标签去重合并（两源标签相同，去重后仍为 3 个）
    assert first.fields["tags"] == ["公园", "花期", "观测"]
    assert first.fields["sources"] == "demo_api,demo_html"
    assert first.fields["html_url"].endswith("/item/html-1")

    # 幂等：再跑一轮宽表行数不变、无事件、内容被刷新
    again = run_source(api_source, db, raw_store, limit=3)
    assert again.wide_merged == 3
    assert db.count_wide_records("article_wide") == 3
    assert db.count_correlation_events(table_name="article_wide") == 0
    assert db.count_records() == 6


def test_correlation_disabled_in_dry_run(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    run_source(make_source("demo_html", "html"), db, raw_store, limit=2)
    api_source = make_source(
        "demo_api",
        correlation=_title_join_config("demo_html", table="dry_wide"),
    )
    summary = run_source(api_source, db, raw_store, limit=2, dry_run=True)

    assert summary.wide_merged == 0
    assert summary.correlation_failed == 0
    assert db.count_wide_records("dry_wide") == 0
    assert db.count_correlation_events(table_name="dry_wide") == 0
    # dry-run 连单源数据也不写，行为与原管道一致
    assert db.count_records("demo_api") == 0


# ---- 场景二：关联字段缺失 / 无匹配 ----


def test_correlation_missing_field_keeps_raw_record_and_marks_event(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    # 关联源没有任何数据：本地记录关联键（external_id）在索引中无匹配，
    # 等价于关联字段在对端缺失；原始单源记录必须保留并打标留痕。
    config = CorrelationConfig(
        remote_source="demo_html",
        join_keys=[JoinKey(local_field="external_id", remote_field="external_id")],
        wide_table=WideTableRule(
            table="missing_wide",
            fields=[
                WideFieldRule(name="title", sources=["demo_api"], strategy="first"),
            ],
        ),
    )
    api_source = make_source("demo_api", correlation=config)
    summary = run_source(api_source, db, raw_store, limit=2)

    assert (summary.ok, summary.failed) == (2, 0)
    assert summary.wide_merged == 0
    assert summary.correlation_failed == 2

    # 原始单源数据完整保留，主采集流程未被中断
    assert db.count_records("demo_api") == 2
    assert db.count_wide_records("missing_wide") == 0

    events = _events(db, "missing_wide")
    assert len(events) == 2
    assert {event.event_type for event in events} == {EVENT_NOT_MATCHED}
    assert {event.local_external_id for event in events} == {"api-1", "api-2"}
    for event in events:
        assert event.remote_source == "demo_html"
        assert "无匹配" in event.detail["reason"]


def test_correlation_missing_local_join_key_emits_missing_field(
    db: Database,
) -> None:
    # 直接构造关联器：远端索引中键存在，但本地记录关联键为空
    config = CorrelationConfig(
        remote_source="demo_html",
        join_keys=[JoinKey(local_field="category", remote_field="category")],
        wide_table=WideTableRule(
            table="cat_wide",
            fields=[WideFieldRule(name="title", sources=["demo_api"], strategy="first")],
        ),
    )
    remote = _record("demo_html", "html-9", category="环境")
    db.upsert_record(remote)

    correlator = Correlator("demo_api", config, db)
    correlator.preload()
    local = _record("demo_api", "api-9", category=None)
    result = correlator.on_record(local)

    assert result.wide_written is False
    assert result.event_type == EVENT_MISSING_FIELD
    assert db.count_wide_records("cat_wide") == 0
    events = _events(db, "cat_wide")
    assert len(events) == 1
    assert events[0].event_type == EVENT_MISSING_FIELD
    assert events[0].local_external_id == "api-9"


# ---- 场景三：冲突合并 ----


def test_correlation_conflict_merge_writes_one_row_and_event(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    # 在关联源里塞入两条与 api-1 同标题的记录，制造一对多冲突
    db.upsert_record(
        _record(
            "demo_html",
            "html-dup-a",
            title="城市公园春季花期观测报告",
            url="http://mock/item/html-dup-a",
        )
    )
    db.upsert_record(
        _record(
            "demo_html",
            "html-dup-b",
            title="城市公园春季花期观测报告",
            url="http://mock/item/html-dup-b",
        )
    )

    config = _title_join_config("demo_html", table="conflict_wide", conflict_policy="merge")
    api_source = make_source("demo_api", correlation=config)
    summary = run_source(api_source, db, raw_store, limit=1)

    assert summary.ok == 1
    assert summary.wide_merged == 1  # merge 策略仍生成宽表行

    rows = _wide_rows(db, "conflict_wide")
    assert len(rows) == 1
    row = rows[0]
    assert row.status == "conflict"
    assert row.conflict_detail is not None
    assert row.conflict_detail["remote_external_ids"] == ["html-dup-a", "html-dup-b"]
    assert row.conflict_detail["policy"] == "merge"
    # concat 字段把冲突双方都保留下来，可追溯
    assert "html-dup-a" in row.fields["html_url"] or "html-dup-b" in row.fields["html_url"]

    events = _events(db, "conflict_wide")
    assert len(events) == 1
    assert events[0].event_type == "conflict"
    assert events[0].local_external_id == "api-1"

    # 幂等：重跑不产生第二个宽表行或第二条事件
    run_source(api_source, db, raw_store, limit=1)
    assert db.count_wide_records("conflict_wide") == 1
    assert len(_events(db, "conflict_wide")) == 1


def test_correlation_conflict_skip_writes_event_only(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    # api-1 有唯一匹配可正常合并；api-2 命中两条冲突记录，按 skip 不生成宽表
    db.upsert_record(
        _record("demo_html", "html-1", title="城市公园春季花期观测报告")
    )
    db.upsert_record(
        _record("demo_html", "dup-a", title="开源数据库性能基准测试汇总")
    )
    db.upsert_record(
        _record("demo_html", "dup-b", title="开源数据库性能基准测试汇总")
    )
    config = _title_join_config("demo_html", table="skip_wide", conflict_policy="skip")
    api_source = make_source("demo_api", correlation=config)
    summary = run_source(api_source, db, raw_store, limit=2)

    assert summary.wide_merged == 1
    assert summary.correlation_failed == 1
    assert db.count_wide_records("skip_wide") == 1
    events = _events(db, "skip_wide")
    assert len(events) == 1
    assert events[0].event_type == "conflict"
    assert events[0].local_external_id == "api-2"
    # 原始单源数据不受影响
    assert db.count_records("demo_api") == 2


# ---- 跨源 ID 类型不匹配 ----


def test_coerce_join_value_handles_types() -> None:
    assert coerce_join_value(42, "string") == "42"
    assert coerce_join_value(" 42 ", "string") == "42"
    assert coerce_join_value(None, "string") is None
    assert coerce_join_value("  ", "string") is None
    assert coerce_join_value("007", "integer") == "7"
    assert coerce_join_value(7, "integer") == "7"
    assert coerce_join_value(
        "2024-03-01", "datetime"
    ) == datetime(2024, 3, 1).isoformat()

    with pytest.raises(ValueError):
        coerce_join_value("abc", "integer")
    with pytest.raises(ValueError):
        coerce_join_value(True, "string")
    with pytest.raises(ValueError):
        coerce_join_value("不是日期", "datetime")


def test_correlation_type_mismatch_emits_event_and_keeps_record(db: Database) -> None:
    config = CorrelationConfig(
        remote_source="demo_html",
        join_keys=[
            JoinKey(local_field="external_id", remote_field="external_id", coerce="integer")
        ],
        wide_table=WideTableRule(
            table="type_wide",
            fields=[WideFieldRule(name="title", sources=["demo_api"], strategy="first")],
        ),
    )
    # 远端 ID 无法转整数，预加载时该记录被跳过并留痕
    db.upsert_record(_record("demo_html", "html-x"))
    correlator = Correlator("demo_api", config, db)
    correlator.preload()

    # 本地 ID 可转整数，但索引为空 → not_matched
    result = correlator.on_record(_record("demo_api", "1001"))
    assert result.event_type == EVENT_NOT_MATCHED
    assert db.count_wide_records("type_wide") == 0

    # 本地 ID 本身不可转整数 → type_mismatch
    mismatch = correlator.on_record(_record("demo_api", "api-1002"))
    assert mismatch.event_type == EVENT_TYPE_MISMATCH
    assert mismatch.wide_written is False
    event_types = {event.event_type for event in _events(db, "type_wide")}
    assert EVENT_TYPE_MISMATCH in event_types


# ---- 配置校验与默认关闭 ----


def test_correlation_config_loads_from_yaml(
    write_config: Any,
    source_payload: Any,
) -> None:
    from crawl_flow.config import load_sources

    payload = source_payload("demo_api")
    payload["correlation"] = {
        "remote_source": "demo_html",
        "join_keys": [{"local_field": "title", "remote_field": "title"}],
        "wide_table": {
            "table": "article_wide",
            "fields": [
                {
                    "name": "title",
                    "sources": ["demo_api", "demo_html"],
                    "strategy": "first",
                }
            ],
        },
    }
    config_path = write_config([payload, source_payload("demo_html", "html")])
    sources = load_sources(config_path)

    api = next(item for item in sources if item.name == "demo_api")
    assert api.correlation is not None
    assert api.correlation.remote_source == "demo_html"
    assert api.correlation.join_keys[0].coerce == "string"
    assert api.correlation.wide_table.conflict_policy == "merge"
    # 未配置关联的源保持 None
    html = next(item for item in sources if item.name == "demo_html")
    assert html.correlation is None


def test_correlation_config_rejects_unknown_remote(
    write_config: Any,
    source_payload: Any,
) -> None:
    from crawl_flow.config import load_sources
    from crawl_flow.errors import ConfigError

    payload = source_payload("demo_api")
    payload["correlation"] = {
        "remote_source": "ghost",
        "join_keys": [{"local_field": "title", "remote_field": "title"}],
        "wide_table": {
            "table": "article_wide",
            "fields": [{"name": "title", "sources": ["demo_api"]}],
        },
    }
    config_path = write_config([payload])
    with pytest.raises(ConfigError, match="不存在的关联源"):
        load_sources(config_path)


def test_correlation_config_rejects_self_reference(
    write_config: Any,
    source_payload: Any,
) -> None:
    from crawl_flow.config import load_sources
    from crawl_flow.errors import ConfigError

    payload = source_payload("demo_api")
    payload["correlation"] = {
        "remote_source": "demo_api",
        "join_keys": [{"local_field": "title", "remote_field": "title"}],
        "wide_table": {
            "table": "article_wide",
            "fields": [{"name": "title", "sources": ["demo_api"]}],
        },
    }
    config_path = write_config([payload])
    with pytest.raises(ConfigError, match="不能关联自身"):
        load_sources(config_path)


def test_correlation_config_rejects_illegal_join_field(source_payload: Any) -> None:
    from pydantic import ValidationError

    payload = source_payload("demo_api")
    payload["correlation"] = {
        "remote_source": "demo_html",
        "join_keys": [{"local_field": "content", "remote_field": "title"}],
        "wide_table": {
            "table": "article_wide",
            "fields": [{"name": "title", "sources": ["demo_api"]}],
        },
    }
    with pytest.raises(ValidationError, match="关联键字段"):
        from crawl_flow.config import SourceConfig

        SourceConfig.model_validate(payload)


def test_correlation_disabled_by_default_and_by_flag(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    # 未配置 correlation：汇总字段保持默认值，不产生宽表
    plain = make_source("demo_api")
    summary = run_source(plain, db, raw_store, limit=2)
    assert summary.wide_merged == 0
    assert summary.correlation_failed == 0
    assert db.count_wide_records() == 0

    # 显式 enabled=false：即便配置了规则也不生效
    disabled_config = _title_join_config("demo_html", table="flag_wide")
    disabled_config.enabled = False
    disabled_source = make_source("demo_api", correlation=disabled_config)
    flagged = run_source(disabled_source, db, raw_store, limit=2)
    assert flagged.wide_merged == 0
    assert flagged.correlation_failed == 0
    assert db.count_wide_records("flag_wide") == 0
    assert db.count_correlation_events(table_name="flag_wide") == 0
