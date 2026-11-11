"""规则引擎与采集管道的集成测试：端到端清洗、质量入库、三种处置模式与降级。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.models import RecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.rules import load_rules
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

runner = CliRunner()


def _write_rules(tmp_path: Path, payload: str) -> Path:
    path = tmp_path / "rules.yaml"
    path.write_text(payload, encoding="utf-8")
    return path


def test_end_to_end_pipeline_with_rules(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    tmp_path: Path,
) -> None:
    rules_path = _write_rules(
        tmp_path,
        """
version: 1
quality: {threshold: 50, on_validation_fail: mark}
fields:
  title:
    type_hint: text
    rules:
      - {type: trim}
      - {type: truncate, params: {max_length: 10}}
  category:
    type_hint: text
    validations:
      - type: enum
        on_fail: warn
        params: {values: [环境, 技术, 文化, 教育, 经济, 交通]}
  content:
    type_hint: multiline
    rules:
      - {type: strip_html}
""",
    )
    engine = load_rules(rules_path)
    source = make_source("demo_api")
    summary = run_source(source, db, raw_store, limit=3, engine=engine)

    assert summary.ok == 3
    assert summary.failed == 0
    with Session(db.engine) as session:
        rows = list(session.execute(select(RecordRow).order_by(RecordRow.external_id)).scalars())
    assert len(rows) == 3
    for row in rows:
        assert row.quality_score == 100.0
        assert row.low_quality is False
        report = row.quality_report
        assert report["action"] == "pass"
        assert "trace" in report and report["trace"]
        # 标题被截断到 10 字以内
        assert len(row.title) <= 10
        # HTML 标签已剥离
        assert "<" not in row.content


def test_low_quality_records_are_flagged(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    tmp_path: Path,
) -> None:
    rules_path = _write_rules(
        tmp_path,
        """
version: 1
quality: {threshold: 99, on_validation_fail: mark}
fields:
  category:
    type_hint: text
    validations:
      - type: enum
        params: {values: [不存在的分类]}
""",
    )
    engine = load_rules(rules_path)
    summary = run_source(make_source("demo_api"), db, raw_store, limit=2, engine=engine)

    assert summary.ok == 2
    assert summary.low_quality == 2
    with Session(db.engine) as session:
        rows = list(session.execute(select(RecordRow)).scalars())
    assert all(row.low_quality is True for row in rows)
    assert all(row.quality_score < 99 for row in rows)


def test_skip_mode_does_not_insert_record_but_keeps_raw(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    tmp_path: Path,
) -> None:
    rules_path = _write_rules(
        tmp_path,
        """
version: 1
quality: {on_validation_fail: skip}
fields:
  title:
    type_hint: text
    validations:
      - type: length
        params: {min: 999}
""",
    )
    engine = load_rules(rules_path)
    summary = run_source(make_source("demo_api"), db, raw_store, limit=3, engine=engine)

    assert summary.ok == 0
    quality_skips = [r for r in summary.results if r.status == "skipped_quality"]
    assert len(quality_skips) == 3
    assert db.count_records() == 0
    # raw 仍然留档，且轨迹中记录了校验失败原因
    assert db.count_raw("demo_api", "ok") == 3
    from crawl_flow.models import RawRecordRow

    with Session(db.engine) as session:
        raw_rows = list(session.execute(select(RawRecordRow)).scalars())
    payload = json.loads(Path(raw_rows[0].raw_path).read_text(encoding="utf-8"))
    assert payload["quality"]["action"] == "skip"
    assert payload["rule_trace"]
    assert any(entry["phase"] == "validate" and entry["passed"] is False for entry in payload["rule_trace"])


def test_warn_mode_inserts_and_logs_but_does_not_flag(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    tmp_path: Path,
) -> None:
    rules_path = _write_rules(
        tmp_path,
        """
version: 1
quality: {threshold: 0, on_validation_fail: warn}
fields:
  category:
    type_hint: text
    validations:
      - type: enum
        params: {values: [其他]}
""",
    )
    engine = load_rules(rules_path)
    summary = run_source(make_source("demo_api"), db, raw_store, limit=2, engine=engine)

    assert summary.ok == 2
    # threshold=0，任何评分都不会被判低质量
    assert summary.low_quality == 0
    assert db.count_records() == 2


def test_missing_rules_file_runs_with_default_cleaning(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    tmp_path: Path,
) -> None:
    engine = load_rules(tmp_path / "nope.yaml")
    assert engine.missing
    summary = run_source(make_source("demo_api"), db, raw_store, limit=2, engine=engine)
    assert summary.ok == 2
    with Session(db.engine) as session:
        rows = list(session.execute(select(RecordRow)).scalars())
    assert all(row.quality_score == 100.0 for row in rows)
    assert all(row.quality_report == {} for row in rows)


def test_source_specific_rules_override_global(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    tmp_path: Path,
) -> None:
    rules_path = _write_rules(
        tmp_path,
        """
version: 1
fields:
  title:
    type_hint: text
    rules:
      - {type: truncate, params: {max_length: 2}}
sources:
  demo_api:
    fields:
      title:
        type_hint: text
        rules:
          - {type: trim}
""",
    )
    engine = load_rules(rules_path)
    run_source(make_source("demo_api"), db, raw_store, limit=1, engine=engine)
    with Session(db.engine) as session:
        row = session.execute(select(RecordRow)).scalar_one()
    # 来源级规则只有 trim，标题不会被截断成 2 个字
    assert len(row.title) > 2


def test_cli_invalid_rules_degrades_without_crashing(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    rules_path = tmp_path / "bad-rules.yaml"
    rules_path.write_text(
        """
version: 1
fields:
  title:
    rules:
      - type: truncate
        params: {}
""",
        encoding="utf-8",
    )
    db_path = tmp_path / "cli.db"
    result = runner.invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "2",
            "--config",
            str(config_path),
            "--rules",
            str(rules_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )
    # 任务不崩溃，退出码 0，且有明确降级提示
    assert result.exit_code == 0, result.output
    assert "规则配置错误" in result.output
    assert "降级" in result.output
    database = Database(db_path)
    database.init()
    try:
        assert database.count_records("demo_api") == 2
    finally:
        database.close()


def test_cli_validate_rules_command(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    write_config([source_payload("demo_api")])
    good = tmp_path / "good-rules.yaml"
    good.write_text(
        """
version: 1
fields:
  title:
    type_hint: text
    rules: [{type: trim}]
""",
        encoding="utf-8",
    )
    ok = runner.invoke(app, ["validate-rules", "--rules", str(good)])
    assert ok.exit_code == 0, ok.output
    assert "规则校验通过" in ok.output

    bad = tmp_path / "bad-rules.yaml"
    bad.write_text(
        """
version: 1
fields:
  title:
    rules:
      - {type: unknown_op}
""",
        encoding="utf-8",
    )
    failed = runner.invoke(app, ["validate-rules", "--rules", str(bad)])
    assert failed.exit_code == 2
    assert "规则校验失败" in failed.output
    assert "unknown_op" in failed.output


def test_rule_trace_records_before_and_after_for_every_field(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    tmp_path: Path,
) -> None:
    rules_path = _write_rules(
        tmp_path,
        """
version: 1
fields:
  title:
    type_hint: text
    rules: [{type: trim}]
  content:
    type_hint: multiline
    rules: [{type: strip_html}]
""",
    )
    engine = load_rules(rules_path)
    run_source(make_source("demo_api"), db, raw_store, limit=1, engine=engine)

    from crawl_flow.models import RawRecordRow

    with Session(db.engine) as session:
        raw_row = session.execute(select(RawRecordRow)).scalar_one()
    payload = json.loads(Path(raw_row.raw_path).read_text(encoding="utf-8"))
    trace = payload["rule_trace"]
    title_entries = [t for t in trace if t["field"] == "title" and t["phase"] == "clean"]
    assert title_entries and all("before" in t and "after" in t for t in title_entries)
    content_entries = [t for t in trace if t["field"] == "content"]
    assert any("<p>" in str(t["before"]) for t in content_entries)
    assert all("<" not in str(t["after"]) for t in content_entries if t["phase"] == "clean")
