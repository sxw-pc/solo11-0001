"""dedup CLI 冒烟：run / stats / groups 与退出码。"""

from __future__ import annotations

from pathlib import Path

import yaml
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.models import Record
from crawl_flow.storage.db import Database

runner = CliRunner()

BASE = (
    "近日，某市发展改革委发布最新一轮新能源汽车购置补贴政策，明确对符合条件的购车用户"
    "给予每辆最高两万元的一次性补贴，政策自下个月一日起正式实施，有效期至今年年底。"
)
TITLE = "某市发布新能源汽车购置补贴政策"


def _seed_records(db_path: Path) -> None:
    database = Database(db_path)
    database.init()
    try:
        for source, external_id in (("src_a", "1"), ("src_b", "2")):
            database.upsert_record(
                Record(
                    source=source,
                    external_id=external_id,
                    title=TITLE,
                    content=BASE,
                    url=f"http://example.com/{source}/{external_id}",
                )
            )
    finally:
        database.close()


def test_dedup_run_stats_and_groups(tmp_path: Path) -> None:
    db_path = tmp_path / "cli.db"
    _seed_records(db_path)

    result = runner.invoke(app, ["dedup", "run", "--db", str(db_path)])
    assert result.exit_code == 0, result.output
    assert "computed=2" in result.output
    assert "groups=1" in result.output
    assert "仅标记" in result.output

    stats = runner.invoke(app, ["dedup", "stats", "--db", str(db_path)])
    assert stats.exit_code == 0, stats.output
    assert "fingerprints=2" in stats.output
    assert "groups=1" in stats.output
    assert "marked_records=2" in stats.output

    groups = runner.invoke(app, ["dedup", "groups", "--db", str(db_path)])
    assert groups.exit_code == 0, groups.output
    assert "match_type=exact" in groups.output
    assert "source=src_a" in groups.output
    assert "source=src_b" in groups.output


def test_dedup_run_with_custom_config(tmp_path: Path) -> None:
    db_path = tmp_path / "cfg.db"
    _seed_records(db_path)
    config_path = tmp_path / "dedup.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"rule_version": "t1", "min_content_length": 5, "similarity": {"threshold": 0.9}},
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    result = runner.invoke(
        app, ["dedup", "run", "--db", str(db_path), "--config", str(config_path)]
    )
    assert result.exit_code == 0, result.output
    assert "rule_version=t1+" in result.output


def test_dedup_run_missing_db_exits_1(tmp_path: Path) -> None:
    result = runner.invoke(app, ["dedup", "run", "--db", str(tmp_path / "none.db")])
    assert result.exit_code == 1
    assert "数据库不存在" in result.output


def test_dedup_run_bad_config_exits_2(tmp_path: Path) -> None:
    db_path = tmp_path / "bad.db"
    _seed_records(db_path)
    config_path = tmp_path / "dedup.yaml"
    config_path.write_text("similarity:\n  threshold: 1.5\n", encoding="utf-8")
    result = runner.invoke(
        app, ["dedup", "run", "--db", str(db_path), "--config", str(config_path)]
    )
    assert result.exit_code == 2
    assert "配置错误" in result.output


def test_dedup_run_missing_config_file_exits_2(tmp_path: Path) -> None:
    db_path = tmp_path / "miss.db"
    _seed_records(db_path)
    result = runner.invoke(
        app,
        ["dedup", "run", "--db", str(db_path), "--config", str(tmp_path / "nope.yaml")],
    )
    assert result.exit_code == 2
    assert "配置错误" in result.output
