"""布控端到端测试：CLI 两轮采集幂等、命中查询过滤、非法规则启动拦截、dry-run。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.models import WatchHitRow
from crawl_flow.storage.db import Database

runner = CliRunner()


def _write_watch_config(
    tmp_path: Path,
    source_payload: Any,
    *,
    global_rules: list[dict[str, Any]] | None = None,
    source_rules: list[dict[str, Any]] | None = None,
    watch_settings: dict[str, Any] | None = None,
    filename: str = "sources.yaml",
) -> Path:
    api_payload = source_payload("demo_api")
    if source_rules is not None:
        api_payload["watch"] = {"rules": source_rules}
    document: dict[str, Any] = {"sources": [api_payload]}
    watch_block: dict[str, Any] = {"rules": global_rules or []}
    if watch_settings:
        watch_block.update(watch_settings)
    document["watch"] = watch_block
    path = tmp_path / filename
    path.write_text(
        yaml.safe_dump(document, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def _run_collect(config_path: Path, tmp_path: Path, *extra: str):
    return runner.invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "watch.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
            *extra,
        ],
    )


def _open_db(tmp_path: Path) -> Database:
    database = Database(tmp_path / "watch.db")
    database.init()
    return database


def test_two_rounds_produce_no_duplicate_hits(
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = _write_watch_config(
        tmp_path,
        source_payload,
        global_rules=[
            {"name": "brand_database", "type": "word", "pattern": "数据库"},
            {"name": "metro_flow", "type": "regex", "pattern": r"地铁.{0,6}客流"},
        ],
    )

    first = _run_collect(config_path, tmp_path, "--limit", "5")
    assert first.exit_code == 0, first.output
    second = _run_collect(config_path, tmp_path, "--limit", "5")
    assert second.exit_code == 0, second.output

    database = _open_db(tmp_path)
    try:
        total = database.count_watch_hits()
        # 第二轮重扫不得堆积重复行
        assert total > 0
        rows = database.query_watch_hits(limit=1000)
        keys = [(r.source, r.external_id, r.rule_name, r.field) for r in rows]
        assert len(keys) == len(set(keys))

        # 直接查表确认没有任何重复唯一键组合
        with Session(database.engine) as session:
            raw_rows = list(session.execute(select(WatchHitRow)).scalars())
        assert len(raw_rows) == total
        first_round_ids = {row.id for row in raw_rows}
    finally:
        database.close()

    # 第三轮再扫，行数与行 ID 都应保持不变（首次命中被保留）
    third = _run_collect(config_path, tmp_path, "--limit", "5")
    assert third.exit_code == 0, third.output
    database = _open_db(tmp_path)
    try:
        assert database.count_watch_hits() == total
        with Session(database.engine) as session:
            assert {row.id for row in session.execute(select(WatchHitRow)).scalars()} == first_round_ids
    finally:
        database.close()


def test_hits_contain_source_id_rule_and_context(
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = _write_watch_config(
        tmp_path,
        source_payload,
        global_rules=[{"name": "brand_database", "type": "word", "pattern": "数据库"}],
        watch_settings={"context_chars": 8},
    )
    result = _run_collect(config_path, tmp_path, "--limit", "5")
    assert result.exit_code == 0, result.output

    database = _open_db(tmp_path)
    try:
        rows = database.query_watch_hits(rule_name="brand_database", limit=1000)
    finally:
        database.close()

    assert rows, "应至少命中一条「数据库」"
    hit = rows[0]
    assert hit.source == "demo_api"
    assert hit.external_id.startswith("api-")
    assert hit.rule_name == "brand_database"
    assert hit.rule_type == "word"
    assert "数据库" in hit.matched
    assert "数据库" in hit.context


def test_invalid_rule_blocks_startup(
    source_payload: Any,
    tmp_path: Path,
) -> None:
    # 非法正则 + 普通词 + 合法正则：非法规则必须在启动阶段拦下整轮
    config_path = _write_watch_config(
        tmp_path,
        source_payload,
        global_rules=[
            {"name": "ok_word", "type": "word", "pattern": "数据库"},
            {"name": "broken_regex", "type": "regex", "pattern": "([a-z+"},
            {"name": "ok_regex", "type": "regex", "pattern": r"地铁.{0,6}客流"},
        ],
    )
    result = _run_collect(config_path, tmp_path, "--limit", "2")
    assert result.exit_code == 2
    assert "配置错误" in result.output
    assert "broken_regex" in result.output
    # 启动即失败，数据库文件不应产生命中
    assert not (tmp_path / "watch.db").exists() or _open_db(tmp_path).count_watch_hits() == 0


def test_watch_hits_cli_filters_by_rule_source_and_time(
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = _write_watch_config(
        tmp_path,
        source_payload,
        global_rules=[
            {"name": "brand_database", "type": "word", "pattern": "数据库"},
            {"name": "metro_flow", "type": "regex", "pattern": r"地铁.{0,6}客流"},
        ],
    )
    result = _run_collect(config_path, tmp_path, "--limit", "5")
    assert result.exit_code == 0, result.output
    db_arg = ["--db", str(tmp_path / "watch.db")]

    # 按规则过滤
    by_rule = runner.invoke(app, ["watch", "hits", "--rule", "brand_database", *db_arg])
    assert by_rule.exit_code == 0, by_rule.output
    assert "brand_database" in by_rule.output
    assert "metro_flow" not in by_rule.output

    # 按源过滤
    by_source = runner.invoke(app, ["watch", "hits", "--source", "demo_api", *db_arg])
    assert by_source.exit_code == 0
    assert "source=demo_api" in by_source.output

    # 按时间范围：极宽窗口应返回全部，未来窗口返回 0 条
    database = _open_db(tmp_path)
    try:
        total = database.count_watch_hits()
    finally:
        database.close()

    wide = runner.invoke(
        app, ["watch", "hits", "--start", "2000-01-01", "--end", "2100-01-01", *db_arg]
    )
    assert wide.exit_code == 0
    assert f"命中 {total} 条" in wide.output

    future = runner.invoke(
        app, ["watch", "hits", "--start", "2099-01-01", *db_arg]
    )
    assert future.exit_code == 0
    assert "命中 0 条" in future.output

    # 未知规则名返回空清单但退出码 0
    unknown = runner.invoke(app, ["watch", "hits", "--rule", "no_such_rule", *db_arg])
    assert unknown.exit_code == 0
    assert "命中 0 条" in unknown.output

    # JSON 输出可被解析且字段齐全
    json_out = runner.invoke(
        app, ["watch", "hits", "--rule", "brand_database", "--json", *db_arg]
    )
    import json

    payload = json.loads(json_out.output)
    assert payload and all(
        {"source", "external_id", "rule_name", "field", "matched", "context", "hit_at"}
        <= set(item)
        for item in payload
    )


def test_watch_hits_bad_time_argument_exits_2(
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = _write_watch_config(
        tmp_path,
        source_payload,
        global_rules=[{"name": "r1", "type": "word", "pattern": "数据库"}],
    )
    assert _run_collect(config_path, tmp_path, "--limit", "1").exit_code == 0
    result = runner.invoke(
        app,
        ["watch", "hits", "--start", "not-a-date", "--db", str(tmp_path / "watch.db")],
    )
    assert result.exit_code == 2
    assert "参数错误" in result.output


def test_dry_run_writes_no_watch_hits(
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = _write_watch_config(
        tmp_path,
        source_payload,
        global_rules=[{"name": "brand_database", "type": "word", "pattern": "数据库"}],
    )
    result = _run_collect(config_path, tmp_path, "--limit", "3", "--dry-run")
    assert result.exit_code == 0, result.output
    # dry-run 不写任何业务行；库文件可能已由建表初始化创建
    database = _open_db(tmp_path)
    try:
        assert database.count_watch_hits() == 0
        assert database.count_records() == 0
        assert database.count_raw() == 0
    finally:
        database.close()


def test_source_level_rule_only_applies_to_its_source(
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = _write_watch_config(
        tmp_path,
        source_payload,
        global_rules=[],
        source_rules=[
            {"name": "tag_library", "type": "word", "pattern": "图书馆", "fields": ["tags"]}
        ],
    )
    result = _run_collect(config_path, tmp_path, "--limit", "5")
    assert result.exit_code == 0, result.output

    database = _open_db(tmp_path)
    try:
        rows = database.query_watch_hits(rule_name="tag_library", limit=1000)
        assert rows
        assert all(row.source == "demo_api" and row.field == "tags" for row in rows)
    finally:
        database.close()


def test_disabled_watch_skips_engine(
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = _write_watch_config(
        tmp_path,
        source_payload,
        global_rules=[{"name": "brand_database", "type": "word", "pattern": "数据库"}],
        watch_settings={"enabled": False},
    )
    result = _run_collect(config_path, tmp_path, "--limit", "5")
    assert result.exit_code == 0, result.output

    database = _open_db(tmp_path)
    try:
        assert database.count_watch_hits() == 0
        # 条目本身正常入库，布控关闭不影响主链路
        assert database.count_records("demo_api") == 5
    finally:
        database.close()


def test_catastrophic_regex_does_not_kill_the_round(
    source_payload: Any,
    tmp_path: Path,
) -> None:
    # 病态规则 + 正常规则：超时熔断后，条目照常入库、正常规则照常命中
    config_path = _write_watch_config(
        tmp_path,
        source_payload,
        global_rules=[
            {"name": "evil", "type": "regex", "pattern": r"(a+)+$"},
            {"name": "brand_database", "type": "word", "pattern": "数据库"},
        ],
        watch_settings={"timeout_seconds": 0.2},
    )
    started = __import__("time").perf_counter()
    result = _run_collect(config_path, tmp_path, "--limit", "5")
    elapsed = __import__("time").perf_counter() - started

    assert result.exit_code == 0, result.output
    # 5 条内容都会触发病态回溯，若没有硬超时，这里需要数十秒
    assert elapsed < 10.0

    database = _open_db(tmp_path)
    try:
        assert database.count_records("demo_api") == 5
        assert database.count_watch_hits(rule_name="evil") == 0
        assert database.count_watch_hits(rule_name="brand_database") > 0
    finally:
        database.close()


def test_watch_hits_on_missing_db_exits_1(tmp_path: Path) -> None:
    result = runner.invoke(app, ["watch", "hits", "--db", str(tmp_path / "missing.db")])
    assert result.exit_code == 1
    assert "数据库不存在" in result.output
