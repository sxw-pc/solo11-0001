"""规则引擎集成：采集主流程、三种失败处置模式、CLI 与数据库迁移。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.config import SourceConfig
from crawl_flow.models import RecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.rules.config import RuleFileConfig
from crawl_flow.rules.engine import RuleSet
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

runner = CliRunner()


def make_rule_set(
    source: str = "demo_api",
    *,
    threshold: int = 60,
    on_failure: str = "mark",
    fields: dict[str, list[dict[str, Any]]] | None = None,
) -> RuleSet:
    payload = {
        "quality_threshold": threshold,
        "on_failure": on_failure,
        "sources": {
            source: {
                "fields": fields
                or {"category": [{"rule": "enum", "values": ["环境"]}]},
            }
        },
    }
    return RuleSet(RuleFileConfig.model_validate(payload))


def _write_rules(tmp_path: Path, payload: Any) -> Path:
    path = tmp_path / "rules.yaml"
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(
            yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
    return path


def _records(db: Database, source: str = "demo_api") -> list[Any]:
    from sqlalchemy import select

    with Session(db.engine) as session:
        return list(
            session.execute(
                select(RecordRow)
                .where(RecordRow.source == source)
                .order_by(RecordRow.external_id)
            ).scalars()
        )


# ---------------------------------------------------------------------------
# 正常流程：干净数据满分入库
# ---------------------------------------------------------------------------


def test_clean_data_scores_100_and_persists(
    db: Database, raw_store: RawStore, api_source: SourceConfig
) -> None:
    rule_set = make_rule_set(
        fields={"category": [{"rule": "enum", "values": ["环境", "技术", "文化", "交通", "经济", "教育"]}]}
    )
    summary = run_source(
        api_source, db, raw_store, limit=3, rule_engine=rule_set.engine_for("demo_api")
    )
    assert (summary.ok, summary.failed, summary.low_quality) == (3, 0, 0)
    rows = _records(db)
    assert len(rows) == 3
    assert all(row.quality_score == 100 for row in rows)
    assert all(row.quality_flags in (None, []) for row in rows)


def test_rules_disabled_keeps_quality_null(
    db: Database, raw_store: RawStore, api_source: SourceConfig
) -> None:
    summary = run_source(api_source, db, raw_store, limit=2)
    assert summary.ok == 2
    assert all(row.quality_score is None for row in _records(db))


# ---------------------------------------------------------------------------
# 三种失败处置模式
# ---------------------------------------------------------------------------


def test_mark_mode_flags_low_quality_records(
    db: Database, raw_store: RawStore, api_source: SourceConfig
) -> None:
    # 只允许「环境」：api-1 满分，api-2/3 零分 → mark 保留并打标
    rule_set = make_rule_set(on_failure="mark")
    summary = run_source(
        api_source, db, raw_store, limit=3, rule_engine=rule_set.engine_for("demo_api")
    )
    assert (summary.ok, summary.failed, summary.low_quality) == (3, 0, 2)
    rows = {row.external_id: row for row in _records(db)}
    assert rows["api-1"].quality_score == 100
    assert rows["api-2"].quality_score == 0
    assert "low_quality" in rows["api-2"].quality_flags
    assert "validate:category:enum" in rows["api-2"].quality_flags


def test_skip_mode_drops_low_quality_records_but_archives_raw(
    db: Database, raw_store: RawStore, api_source: SourceConfig
) -> None:
    rule_set = make_rule_set(on_failure="skip")
    summary = run_source(
        api_source, db, raw_store, limit=3, rule_engine=rule_set.engine_for("demo_api")
    )
    # api-1 通过；api-2/api-3 被质量规则丢弃；其余 12 条因 limit 记 skipped
    assert (summary.ok, summary.failed, summary.skipped) == (1, 0, 14)
    assert summary.low_quality == 2
    assert db.count_records("demo_api") == 1
    # 原始响应照常留档，状态为 ok（抓取本身成功）
    assert db.count_raw("demo_api", "ok") == 3
    skipped = [r for r in summary.results if r.status == "skipped"]
    assert {r.external_id for r in skipped} == {"api-2", "api-3"}
    assert all(r.quality_score == 0 for r in skipped)


def test_alert_mode_keeps_records_and_logs_warning(
    db: Database,
    raw_store: RawStore,
    api_source: SourceConfig,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rule_set = make_rule_set(on_failure="alert")
    with caplog.at_level(logging.WARNING, logger="crawl_flow"):
        summary = run_source(
            api_source,
            db,
            raw_store,
            limit=2,
            rule_engine=rule_set.engine_for("demo_api"),
        )
    assert (summary.ok, summary.low_quality) == (2, 1)
    assert db.count_records("demo_api") == 2
    quality_logs = [r for r in caplog.records if "event=quality" in r.getMessage()]
    assert any(r.levelno == logging.WARNING for r in quality_logs)


# ---------------------------------------------------------------------------
# 可追溯性：日志含处理前后值
# ---------------------------------------------------------------------------


def test_rule_trace_logs_contain_before_and_after(
    db: Database,
    raw_store: RawStore,
    api_source: SourceConfig,
    caplog: pytest.LogCaptureFixture,
) -> None:
    rule_set = make_rule_set(
        fields={"title": [{"rule": "strip"}], "category": [{"rule": "enum", "values": ["环境"]}]}
    )
    with caplog.at_level(logging.DEBUG, logger="crawl_flow"):
        run_source(
            api_source, db, raw_store, limit=1, rule_engine=rule_set.engine_for("demo_api")
        )
    traces = [r.getMessage() for r in caplog.records if "event=rule_trace" in r.getMessage()]
    assert traces, "应输出逐字段规则轨迹"
    assert any("field=title" in t and "before=" in t and "after=" in t for t in traces)
    assert any("field=category" in t and "rule=enum" in t for t in traces)


# ---------------------------------------------------------------------------
# 降级：规则异常不影响采集主流程
# ---------------------------------------------------------------------------


def test_engine_crash_falls_back_to_default_cleaning(
    db: Database,
    raw_store: RawStore,
    api_source: SourceConfig,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rule_set = make_rule_set()
    engine = rule_set.engine_for("demo_api")
    assert engine is not None

    def boom(fields, report):
        raise RuntimeError("模拟规则引擎崩溃")

    monkeypatch.setattr(engine, "clean_fields", boom)
    summary = run_source(api_source, db, raw_store, limit=2, rule_engine=engine)
    assert (summary.ok, summary.failed) == (2, 0)
    # 降级为默认清洗：记录正常入库，无质量分
    rows = _records(db)
    assert len(rows) == 2
    assert all(row.quality_score is None for row in rows)


def test_cli_run_with_broken_rules_config_still_collects(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config([source_payload("demo_api")])
    rules_path = _write_rules(tmp_path, "sources: [broken\n")
    db_path = tmp_path / "cli.db"

    result = runner.invoke(
        app,
        [
            "run", "--source", "demo_api", "--limit", "2",
            "--config", str(config_path),
            "--rules", str(rules_path),
            "--db", str(db_path),
            "--raw-dir", str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "规则配置错误" in result.output
    assert "默认清洗逻辑" in result.output
    assert "ok=2" in result.output

    database = Database(db_path)
    database.init()
    try:
        assert database.count_records("demo_api") == 2
    finally:
        database.close()


def test_cli_run_with_missing_rules_file_warns_and_continues(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = runner.invoke(
        app,
        [
            "run", "--source", "demo_api", "--limit", "1",
            "--config", str(config_path),
            "--rules", str(tmp_path / "no-such-rules.yaml"),
            "--db", str(tmp_path / "cli.db"),
            "--raw-dir", str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "规则配置不存在" in result.output
    assert "ok=1" in result.output


def test_cli_run_with_valid_rules_file(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config([source_payload("demo_api")])
    rules_path = _write_rules(
        tmp_path,
        {
            "quality_threshold": 60,
            "on_failure": "mark",
            "sources": {
                "demo_api": {
                    "fields": {"category": [{"rule": "enum", "values": ["环境"]}]}
                }
            },
        },
    )
    db_path = tmp_path / "cli.db"
    result = runner.invoke(
        app,
        [
            "run", "--source", "demo_api", "--limit", "3",
            "--config", str(config_path),
            "--rules", str(rules_path),
            "--db", str(db_path),
            "--raw-dir", str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "low_quality=2" in result.output


# ---------------------------------------------------------------------------
# rules check：启动时校验
# ---------------------------------------------------------------------------


def test_rules_check_ok(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path,
        {
            "quality_threshold": 60,
            "on_failure": "mark",
            "defaults": {"fields": {"title": [{"rule": "strip"}, {"rule": "required"}]}},
        },
    )
    result = runner.invoke(app, ["rules", "check", "--rules", str(rules_path)])
    assert result.exit_code == 0, result.output
    assert "规则配置有效" in result.output


def test_rules_check_reports_error_and_exits_2(tmp_path: Path) -> None:
    rules_path = _write_rules(
        tmp_path,
        {"sources": {"demo_api": {"fields": {"title": [{"rule": "magic"}]}}}},
    )
    result = runner.invoke(app, ["rules", "check", "--rules", str(rules_path)])
    assert result.exit_code == 2
    assert "规则配置错误" in result.output
    assert "未知规则" in result.output


def test_rules_check_missing_file_exits_2(tmp_path: Path) -> None:
    result = runner.invoke(
        app, ["rules", "check", "--rules", str(tmp_path / "nope.yaml")]
    )
    assert result.exit_code == 2
    assert "不存在" in result.output


# ---------------------------------------------------------------------------
# 数据库迁移：老库自动补齐 quality 列
# ---------------------------------------------------------------------------


def test_existing_db_is_migrated_with_quality_columns(tmp_path: Path) -> None:
    db_path = tmp_path / "old.db"
    # 模拟规则引擎引入之前的老库：records 表没有 quality_* 列
    database = Database(db_path)
    with database.engine.connect() as conn:
        conn.execute(
            text(
                "CREATE TABLE records ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "source VARCHAR(128) NOT NULL, "
                "external_id VARCHAR(256) NOT NULL, "
                "title VARCHAR(512) NOT NULL, "
                "category VARCHAR(128), "
                "tags JSON NOT NULL, "
                "published_at DATETIME, "
                "content TEXT NOT NULL, "
                "url VARCHAR(2048) NOT NULL, "
                "fetched_at DATETIME NOT NULL, "
                "updated_at DATETIME NOT NULL, "
                "CONSTRAINT uq_records_source_external UNIQUE (source, external_id))"
            )
        )
        conn.commit()
    database.init()  # 应自动 ALTER TABLE 补齐两列
    with database.engine.connect() as conn:
        columns = {row[1] for row in conn.execute(text("PRAGMA table_info(records)"))}
    assert {"quality_score", "quality_flags"} <= columns
    database.close()

    # 迁移后可以正常写入带质量分的记录
    from crawl_flow.models import Record

    database = Database(db_path)
    database.init()
    try:
        database.upsert_record(
            Record(
                source="s",
                external_id="e1",
                title="t",
                quality_score=55,
                quality_flags=["low_quality"],
            )
        )
        assert database.count_records("s") == 1
    finally:
        database.close()


def test_record_row_stores_quality_fields(
    db: Database, raw_store: RawStore, api_source: SourceConfig
) -> None:
    rule_set = make_rule_set(on_failure="mark")
    run_source(
        api_source, db, raw_store, limit=2, rule_engine=rule_set.engine_for("demo_api")
    )
    with Session(db.engine) as session:
        rows = list(
            session.execute(
                text("SELECT quality_score, quality_flags FROM records ORDER BY external_id")
            ).all()
        )
    assert rows[0][0] == 100
    assert rows[1][0] == 0
    assert "low_quality" in rows[1][1]
