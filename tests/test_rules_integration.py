"""规则引擎与采集主流程 / CLI 的集成行为。

覆盖：质量分与低质量标记落库、skip/mark/warn 三种处理模式、
规则引擎异常降级、规则配置错误时的启动校验与降级、老库列迁移。
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import select, text
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.config import SourceConfig
from crawl_flow.fetch.types import FetchResult
from crawl_flow.models import RecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.rules import RuleEngine, RuleSetConfig
from crawl_flow.sources.base import ListItem, ListPage
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

runner = CliRunner()


class _FakeAdapter:
    """返回固定脏数据的适配器，用于精确控制规则命中情况。"""

    def __init__(self, source: SourceConfig, data: dict[str, dict[str, Any]]) -> None:
        self.source = source
        self._data = data

    def iter_list_pages(self) -> Any:
        items = [ListItem(external_id=eid, url=self.detail_url(eid)) for eid in self._data]
        yield ListPage(page=1, items=items)

    def fetch_detail(self, external_id: str) -> FetchResult:
        return FetchResult(
            url=self.detail_url(external_id), status_code=200, body="{}"
        )

    def parse_detail(self, result: FetchResult, external_id: str) -> dict[str, Any]:
        return dict(self._data[external_id])

    def detail_url(self, external_id: str) -> str:
        return f"http://detail.test/{external_id}"


@pytest.fixture
def install_adapter(monkeypatch: pytest.MonkeyPatch) -> Any:
    def _install(data: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        monkeypatch.setattr(
            "crawl_flow.pipeline.run.create_adapter",
            lambda source, fetcher: _FakeAdapter(source, data),
        )
        return data

    return _install


def _engine(payload: dict[str, Any]) -> RuleEngine:
    return RuleEngine(RuleSetConfig.model_validate(payload), logger=logging.getLogger("crawl_flow.rules"))


def _dirty_item() -> dict[str, Any]:
    return {
        "title": "  坏标题  ",
        "category": "未知分类",
        "tags": "甲、乙",
        "published_at": "不是日期",
        "content": "<p>正文段落</p>",
    }


def test_quality_score_and_low_quality_flag_persisted(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    install_adapter: Any,
) -> None:
    install_adapter({"d-1": _dirty_item()})
    engine = _engine(
        {
            "sources": {
                "demo_api": {
                    "threshold": 60,
                    "on_quality_failure": "mark",
                    "fields": {
                        "title": {"rules": [{"name": "strip"}]},
                        "category": {
                            "rules": [
                                {
                                    "name": "enum",
                                    "params": {"values": ["新闻"]},
                                    "weight": 50,
                                }
                            ]
                        },
                    },
                }
            }
        }
    )

    summary = run_source(
        api_source, db, raw_store, limit=1, rule_engine=engine
    )

    assert (summary.ok, summary.failed, summary.low_quality) == (1, 0, 1)
    with Session(db.engine) as session:
        row = session.execute(select(RecordRow)).scalar_one()
    assert row.quality_score == 50
    assert row.low_quality is True
    assert row.title == "坏标题"  # 规则清洗后的值入库
    assert row.category == "未知分类"


def test_skip_mode_drops_record_and_archives_raw(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    install_adapter: Any,
) -> None:
    install_adapter({"d-1": _dirty_item()})
    engine = _engine(
        {
            "sources": {
                "demo_api": {
                    "fields": {
                        "category": {
                            "rules": [
                                {
                                    "name": "enum",
                                    "params": {"values": ["新闻"]},
                                    "weight": 10,
                                    "on_failure": "skip",
                                }
                            ]
                        }
                    }
                }
            }
        }
    )

    summary = run_source(api_source, db, raw_store, limit=1, rule_engine=engine)

    assert (summary.ok, summary.failed, summary.skipped) == (0, 0, 1)
    assert db.count_records("demo_api") == 0
    # 抓取成功、仅被质量闸门拦截：原始响应仍留档
    assert db.count_raw("demo_api", status="ok") == 1
    assert len(list(raw_store.root.rglob("*.json"))) == 1


def test_warn_mode_stores_without_low_quality_flag(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    install_adapter: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    install_adapter({"d-1": _dirty_item()})
    engine = _engine(
        {
            "sources": {
                "demo_api": {
                    "threshold": 60,
                    "on_quality_failure": "warn",
                    "fields": {
                        "category": {
                            "rules": [
                                {
                                    "name": "enum",
                                    "params": {"values": ["新闻"]},
                                    "weight": 50,
                                }
                            ]
                        }
                    },
                }
            }
        }
    )

    with caplog.at_level(logging.WARNING, logger="crawl_flow.rules"):
        summary = run_source(api_source, db, raw_store, limit=1, rule_engine=engine)

    assert summary.ok == 1
    assert summary.low_quality == 0
    with Session(db.engine) as session:
        row = session.execute(select(RecordRow)).scalar_one()
    assert row.quality_score == 50
    assert row.low_quality is False  # 仅告警，不打标
    assert "quality_gate" in caplog.text and "action=warn" in caplog.text


def test_rule_trace_log_contains_before_and_after(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    install_adapter: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    install_adapter({"d-1": _dirty_item()})
    engine = _engine(
        {
            "sources": {
                "demo_api": {
                    "fields": {"title": {"rules": [{"name": "strip"}]}},
                }
            }
        }
    )

    with caplog.at_level(logging.DEBUG, logger="crawl_flow.rules"):
        run_source(api_source, db, raw_store, limit=1, rule_engine=engine)

    assert "rule_trace" in caplog.text
    assert "field=title" in caplog.text
    assert "before='  坏标题  '" in caplog.text
    assert "after='坏标题'" in caplog.text


def test_engine_exception_degrades_to_default_cleaning(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    install_adapter: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    install_adapter({"d-1": _dirty_item()})
    engine = _engine(
        {"sources": {"demo_api": {"fields": {"title": {"rules": [{"name": "strip"}]}}}}}
    )

    def _boom(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("模拟引擎故障")

    setattr(engine, "process", _boom)
    with caplog.at_level(logging.WARNING, logger="crawl_flow.pipeline"):
        summary = run_source(api_source, db, raw_store, limit=1, rule_engine=engine)

    # 主流程不中断，降级为默认清洗（normalize 仍会裁剪标题空白）
    assert summary.ok == 1
    with Session(db.engine) as session:
        row = session.execute(select(RecordRow)).scalar_one()
    assert row.title == "坏标题"
    assert row.quality_score is None
    assert row.low_quality is False
    assert "rule engine degraded" in caplog.text


def test_init_adds_quality_columns_to_legacy_database(tmp_path: Path) -> None:
    db = Database(tmp_path / "legacy.db")
    try:
        with db.engine.begin() as conn:
            conn.execute(
                text(
                    "CREATE TABLE records ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "source VARCHAR(128) NOT NULL, external_id VARCHAR(256) NOT NULL, "
                    "title VARCHAR(512) NOT NULL, category VARCHAR(128), "
                    "tags JSON NOT NULL, published_at DATETIME, content TEXT NOT NULL, "
                    "url VARCHAR(2048) NOT NULL, fetched_at DATETIME NOT NULL, "
                    "updated_at DATETIME NOT NULL)"
                )
            )
        db.init()
        with db.engine.connect() as conn:
            columns = {row[1] for row in conn.execute(text("PRAGMA table_info(records)"))}
        assert {"quality_score", "low_quality"} <= columns
    finally:
        db.close()


# ---------------------------------------------------------------------------
# CLI：启动时校验配置；配置错误时降级而非崩溃
# ---------------------------------------------------------------------------


def _run_args(config: Path, db_path: Path, raw_dir: Path, rules: Path, *extra: str) -> list[str]:
    return [
        "run",
        "--config",
        str(config),
        "--rules",
        str(rules),
        "--db",
        str(db_path),
        "--raw-dir",
        str(raw_dir),
        *extra,
    ]


def test_cli_invalid_rules_warns_and_falls_back(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    rules_path = tmp_path / "broken-rules.yaml"
    rules_path.write_text("sources: [未闭合\n  bad: : :", encoding="utf-8")
    db_path = tmp_path / "fallback.db"

    result = runner.invoke(
        app,
        _run_args(config_path, db_path, tmp_path / "raw", rules_path, "--source", "demo_api", "--limit", "2"),
    )

    assert result.exit_code == 0, result.output
    assert "规则配置错误" in result.output
    assert "降级为默认清洗" in result.output
    # 降级后仍正常入库
    database = Database(db_path)
    database.init()
    try:
        assert database.count_records("demo_api") == 2
    finally:
        database.close()


def test_cli_rules_check_valid() -> None:
    result = runner.invoke(app, ["rules", "check", "--config", "config/rules.yaml"])
    assert result.exit_code == 0, result.output
    assert "规则配置有效" in result.output
    assert "source=demo_api" in result.output


def test_cli_rules_check_invalid_exits_2(tmp_path: Path) -> None:
    rules_path = tmp_path / "broken.yaml"
    rules_path.write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "demo_api": {
                        "fields": {
                            "title": {"rules": [{"name": "truncate", "params": {"maximum": 10}}]}
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    result = runner.invoke(app, ["rules", "check", "--config", str(rules_path)])
    assert result.exit_code == 2, result.output
    assert "规则配置错误" in result.output


def test_cli_rules_check_missing_file_exits_2(tmp_path: Path) -> None:
    result = runner.invoke(app, ["rules", "check", "--config", str(tmp_path / "nope.yaml")])
    assert result.exit_code == 2, result.output
    assert "规则配置文件不存在" in result.output


def test_cli_run_with_rules_marks_low_quality(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    rules_path = tmp_path / "rules.yaml"
    rules_path.write_text(
        yaml.safe_dump(
            {
                "sources": {
                    "demo_api": {
                        "threshold": 60,
                        "on_quality_failure": "mark",
                        "fields": {
                            "category": {
                                "rules": [
                                    {
                                        "name": "enum",
                                        "params": {"values": ["不存在的分类"]},
                                        "weight": 50,
                                    }
                                ]
                            }
                        },
                    }
                }
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    db_path = tmp_path / "rules.db"

    result = runner.invoke(
        app,
        _run_args(config_path, db_path, tmp_path / "raw", rules_path, "--source", "demo_api", "--limit", "2"),
    )

    assert result.exit_code == 0, result.output
    assert "low_quality=2" in result.output

    database = Database(db_path)
    database.init()
    try:
        with Session(database.engine) as session:
            rows = list(session.execute(select(RecordRow)).scalars())
    finally:
        database.close()
    assert len(rows) == 2
    assert all(row.low_quality for row in rows)
    assert all(row.quality_score == 50 for row in rows)
