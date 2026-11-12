"""dry-run 差异对比：新增 / 修改 / 无变化 / 存量缺失 与只读性。"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.models import Record, RecordRow
from crawl_flow.pipeline.diff import (
    STATUS_ADDED,
    STATUS_ANOMALY,
    STATUS_MODIFIED,
    STATUS_UNCHANGED,
    DiffReport,
    compare_snapshot,
)
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

runner = CliRunner()


def _run_args(config: Path, db_path: Path, raw_dir: Path, *extra: str) -> list[str]:
    return [
        "run",
        "--config",
        str(config),
        "--db",
        str(db_path),
        "--raw-dir",
        str(raw_dir),
        *extra,
    ]


def _record(source: str, external_id: str, **overrides: Any) -> Record:
    payload = {
        "source": source,
        "external_id": external_id,
        "title": f"标题 {external_id}",
        "content": f"正文 {external_id}",
        "published_at": datetime(2024, 3, 1, 9, 30),
        "fetched_at": datetime(2024, 3, 1),
    }
    payload.update(overrides)
    return Record(**payload)


def _stats(report: DiffReport) -> tuple[int, int, int, int]:
    return report.added, report.modified, report.unchanged, report.anomaly


# ---------------------------------------------------------------- 单元测试


def test_compare_snapshot_reports_added() -> None:
    fetched = [_record("s", "x-1"), _record("s", "x-2")]
    report = compare_snapshot(
        source="s",
        fetched=fetched,
        stored={},
        stored_ids=[],
        observed_ids=["x-1", "x-2"],
        fields=["title", "content", "published_at"],
    )
    assert _stats(report) == (2, 0, 0, 0)
    assert {entry.status for entry in report.entries} == {STATUS_ADDED}


def test_compare_snapshot_reports_modified_and_unchanged() -> None:
    fetched = [
        _record("s", "x-1", title="新标题"),
        _record("s", "x-2"),
    ]
    stored = {
        "x-1": {
            "title": "旧标题",
            "content": "正文 x-1",
            "published_at": datetime(2024, 3, 1, 9, 30),
        },
        "x-2": {
            "title": "标题 x-2",
            "content": "正文 x-2",
            "published_at": datetime(2024, 3, 1, 9, 30),
        },
    }
    report = compare_snapshot(
        source="s",
        fetched=fetched,
        stored=stored,
        stored_ids=["x-1", "x-2"],
        observed_ids=["x-1", "x-2"],
        fields=["title", "content", "published_at"],
    )
    assert _stats(report) == (0, 1, 1, 0)
    modified = [entry for entry in report.entries if entry.status == STATUS_MODIFIED]
    assert modified[0].external_id == "x-1"
    assert [change.field for change in modified[0].changes] == ["title"]
    # 正文变更摘要只给长度，不泄露正文。
    assert "旧标题" in report.entries[0].changes[0].render()


def test_compare_snapshot_reports_stored_missing() -> None:
    fetched = [_record("s", "x-1")]
    report = compare_snapshot(
        source="s",
        fetched=fetched,
        stored={
            "x-1": {
                "title": "标题 x-1",
                "content": "正文 x-1",
                "published_at": datetime(2024, 3, 1, 9, 30),
            }
        },
        stored_ids=["x-1", "x-9"],
        observed_ids=["x-1"],
        fields=["title", "content", "published_at"],
    )
    assert _stats(report) == (0, 0, 1, 1)
    anomaly = report.entries[-1]
    assert anomaly.external_id == "x-9"
    assert anomaly.reason_code == "stored_missing"


def test_compare_snapshot_tolerates_tag_order_and_null_types() -> None:
    fetched = [
        _record("s", "x-1", tags=["b", "a"], category=None),
    ]
    stored = {
        "x-1": {
            "title": "标题 x-1",
            "content": "正文 x-1",
            "published_at": datetime(2024, 3, 1, 9, 30),
            # JSON 标签无序、字符串形式的旧数据、空值都视为等价。
            "tags": '["a", "b"]',
            "category": None,
        }
    }
    report = compare_snapshot(
        source="s",
        fetched=fetched,
        stored=stored,
        stored_ids=["x-1"],
        observed_ids=["x-1"],
        fields=["title", "content", "published_at", "tags", "category"],
    )
    assert _stats(report) == (0, 0, 1, 0)


def test_compare_snapshot_flags_type_mismatch_as_anomaly() -> None:
    fetched = [_record("s", "x-1")]
    stored = {
        "x-1": {
            "title": {"unexpected": "object"},
            "content": "正文 x-1",
            "published_at": None,
        }
    }
    report = compare_snapshot(
        source="s",
        fetched=fetched,
        stored=stored,
        stored_ids=["x-1"],
        observed_ids=["x-1"],
        fields=["title", "content", "published_at"],
    )
    assert _stats(report) == (0, 0, 0, 1)
    anomaly = report.entries[0]
    assert anomaly.status == STATUS_ANOMALY
    assert anomaly.reason_code == "compare_error"
    assert "类型不匹配" in (anomaly.reason or "")


def test_compare_snapshot_ignores_unobserved_ids_not_in_store() -> None:
    """限量采集时，列表页出现但未抓取的 ID 不应误判为存量缺失。"""

    fetched = [_record("s", "x-1")]
    report = compare_snapshot(
        source="s",
        fetched=fetched,
        stored={},
        stored_ids=[],
        observed_ids=["x-1", "x-2", "x-3"],
        fields=["title"],
    )
    assert _stats(report) == (1, 0, 0, 0)


# ---------------------------------------------------------------- 存储批量查询


def test_fetch_diff_snapshot_batches_without_writes(db: Database) -> None:
    for index in range(3):
        db.upsert_record(_record("s", f"x-{index}"))

    snapshot = db.fetch_diff_snapshot("s", [f"x-{index}" for index in range(3)], ["title"])
    assert snapshot == {"x-0": {"title": "标题 x-0"}, "x-1": {"title": "标题 x-1"}, "x-2": {"title": "标题 x-2"}}
    assert db.count_records("s") == 3


# ---------------------------------------------------------------- CLI 集成


def test_cli_diff_added_scenario_is_read_only(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "added.db"
    raw_dir = tmp_path / "added-raw"

    result = runner.invoke(
        app,
        _run_args(
            config_path,
            db_path,
            raw_dir,
            "--source",
            "demo_api",
            "--limit",
            "3",
            "--dry-run",
            "--diff",
        ),
    )
    assert result.exit_code == 0, result.output
    assert "added=3 modified=0 unchanged=0 anomaly=0" in result.output

    database = Database(db_path)
    database.init()
    try:
        assert database.count_records() == 0
        assert database.count_raw() == 0
    finally:
        database.close()
    assert not raw_dir.exists()


def test_cli_diff_unchanged_and_modified(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "modified.db"
    raw_dir = tmp_path / "modified-raw"
    args = _run_args(config_path, db_path, raw_dir, "--source", "demo_api", "--limit", "3")

    first = runner.invoke(app, [*args, "--dry-run", "--diff"])
    assert first.exit_code == 0, first.output
    assert "added=3 modified=0 unchanged=0 anomaly=0" in first.output

    # 正式写库后再 dry-run：应全部无变化。
    assert runner.invoke(app, args).exit_code == 0
    second = runner.invoke(app, [*args, "--dry-run", "--diff"])
    assert second.exit_code == 0, second.output
    assert "added=0 modified=0 unchanged=3 anomaly=0" in second.output

    # 人为把 api-1 的标题和正文改掉，再 dry-run：应命中 1 条修改。
    database = Database(db_path)
    database.init()
    try:
        with Session(database.engine) as session:
            row = session.execute(
                select(RecordRow).where(RecordRow.external_id == "api-1")
            ).scalar_one()
            row.title = "被篡改的标题"
            row.content = "被篡改的正文内容"
            session.commit()
    finally:
        database.close()

    third = runner.invoke(app, [*args, "--dry-run", "--diff"])
    assert third.exit_code == 0, third.output
    assert "added=0 modified=1 unchanged=2 anomaly=0" in third.output
    assert "字段变更统计 title=1 content=1" in third.output
    # 只输出字段摘要，不打印完整正文。
    assert "被篡改的正文内容" not in third.output
    assert "长度" in third.output


def test_cli_diff_reports_stored_missing(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "missing.db"
    raw_dir = tmp_path / "missing-raw"
    args = _run_args(config_path, db_path, raw_dir, "--source", "demo_api", "--limit", "2")

    assert runner.invoke(app, args).exit_code == 0
    # 插入一条本轮列表不会出现的存量记录。
    database = Database(db_path)
    database.init()
    try:
        database.upsert_record(_record("demo_api", "api-9999"))
    finally:
        database.close()

    result = runner.invoke(app, [*args, "--dry-run", "--diff"])
    assert result.exit_code == 0, result.output
    assert "added=0 modified=0 unchanged=2 anomaly=1" in result.output
    assert "reason=stored_missing" in result.output
    assert "api-9999" in result.output


def test_cli_diff_requires_dry_run(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = runner.invoke(
        app,
        _run_args(
            config_path,
            tmp_path / "plain.db",
            tmp_path / "plain-raw",
            "--source",
            "demo_api",
            "--diff",
        ),
    )
    assert result.exit_code == 2
    assert "--diff 必须与 --dry-run" in result.output


def test_cli_diff_rejects_unknown_field(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = runner.invoke(
        app,
        _run_args(
            config_path,
            tmp_path / "badfield.db",
            tmp_path / "badfield-raw",
            "--source",
            "demo_api",
            "--dry-run",
            "--diff",
            "--diff-fields",
            "title,unknown",
        ),
    )
    assert result.exit_code == 2
    assert "--diff-fields 无效" in result.output


def test_cli_diff_fields_override_changes_scope(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "scope.db"
    raw_dir = tmp_path / "scope-raw"
    args = _run_args(config_path, db_path, raw_dir, "--source", "demo_api", "--limit", "2")
    assert runner.invoke(app, args).exit_code == 0

    result = runner.invoke(
        app,
        [*args, "--dry-run", "--diff", "--diff-fields", "category,tags"],
    )
    assert result.exit_code == 0, result.output
    assert "fields=category,tags" in result.output
    assert "unchanged=2" in result.output


def test_config_compare_fields_extension(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    path = tmp_path / "withdiff.yaml"
    path.write_text(
        Path(config_path).read_text(encoding="utf-8")
        + "\ndiff:\n  compare_fields: [title, category]\n",
        encoding="utf-8",
    )
    db_path = tmp_path / "cfg.db"
    raw_dir = tmp_path / "cfg-raw"
    args = _run_args(path, db_path, raw_dir, "--source", "demo_api", "--limit", "2")
    assert runner.invoke(app, args).exit_code == 0

    result = runner.invoke(app, [*args, "--dry-run", "--diff"])
    assert result.exit_code == 0, result.output
    assert "fields=title,category" in result.output
    assert "unchanged=2" in result.output


def test_dry_run_without_diff_behaviour_unchanged(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    """未开启 --diff 时 dry-run 不查询存量，也不输出对比行。"""

    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "nodiff.db"
    raw_dir = tmp_path / "nodiff-raw"

    database = Database(db_path)
    database.init()
    try:
        database.upsert_record(_record("demo_api", "api-1"))
    finally:
        database.close()

    result = runner.invoke(
        app,
        _run_args(
            config_path,
            db_path,
            raw_dir,
            "--source",
            "demo_api",
            "--limit",
            "2",
            "--dry-run",
        ),
    )
    assert result.exit_code == 0, result.output
    assert "差异对比" not in result.output
    assert "added=" not in result.output


def test_diff_stats_match_actual_writes(
    api_source: Any,
    db: Database,
    raw_store: RawStore,
    tmp_path: Path,
) -> None:
    """先 dry-run 对比空库，再正式写库，预测的新增数与实际行数一致。"""

    dry = run_source(
        api_source,
        db,
        raw_store,
        limit=5,
        dry_run=True,
        diff_fields=["title", "content", "published_at"],
    )
    assert dry.diff is not None
    assert _stats(dry.diff) == (5, 0, 0, 0)
    assert db.count_records() == 0
    assert not raw_store.root.exists()

    real = run_source(api_source, db, raw_store, limit=5)
    assert real.ok == 5
    assert db.count_records("demo_api") == dry.diff.added

    again = run_source(
        api_source,
        db,
        raw_store,
        limit=5,
        dry_run=True,
        diff_fields=["title", "content", "published_at"],
    )
    assert again.diff is not None
    assert _stats(again.diff) == (0, 0, 5, 0)


def test_diff_does_not_write_or_lock_database(
    api_source: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    """对比前后 updated_at 与行数都不变，且期间可并发读写。"""

    run_source(api_source, db, raw_store, limit=3)
    before = _snapshot_rows(db)
    raw_before = sorted(raw_store.root.rglob("*.json"))

    summary = run_source(
        api_source,
        db,
        raw_store,
        limit=3,
        dry_run=True,
        diff_fields=["title", "content", "published_at"],
    )
    after = _snapshot_rows(db)
    raw_after = sorted(raw_store.root.rglob("*.json"))

    assert summary.diff is not None
    assert _stats(summary.diff) == (0, 0, 3, 0)
    assert before == after
    assert raw_before == raw_after


def _snapshot_rows(db: Database) -> list[tuple[Any, ...]]:
    with Session(db.engine) as session:
        return list(
            session.execute(
                select(
                    RecordRow.source,
                    RecordRow.external_id,
                    RecordRow.title,
                    RecordRow.content,
                    RecordRow.updated_at,
                ).order_by(RecordRow.external_id)
            ).all()
        )
