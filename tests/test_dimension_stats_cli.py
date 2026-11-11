"""维度统计 CLI：参数组合、CSV 导出（中文/特殊字符）、路径安全与空结果。"""

from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.errors import StoreError
from crawl_flow.models import RawRecord, Record
from crawl_flow.storage.db import UNCATEGORIZED_LABEL
from crawl_flow.storage.raw_store import safe_output_path

runner = CliRunner()
BASE_TIME = datetime(2024, 3, 1, 9, 0, tzinfo=timezone.utc)


def _seed_db(db_path: Path) -> None:
    from crawl_flow.storage.db import Database

    database = Database(db_path)
    database.init()
    rows = [
        ("s1", "a1", "技术", ["数据库", "性能"], 0, "failed"),
        ("s1", "a2", "技术", ["性能", "基线"], 1, "ok"),
        ("s1", "a3", "生活", ["数据库,带逗号", '引号"x"'], 2, None),
        ("s1", "a4", None, [], 3, None),
        ("s2", "b1", "技术", ["测试"], 5, "failed"),
    ]
    for source, external_id, category, tags, offset, _ in rows:
        database.upsert_record(
            Record(
                source=source,
                external_id=external_id,
                title=f"标题-{external_id}",
                category=category,
                tags=tags,
                fetched_at=BASE_TIME + timedelta(days=offset),
            )
        )
    for source, external_id, _, _, _, status in rows:
        if status is None:
            continue
        database.upsert_raw(
            RawRecord(
                source=source,
                external_id=external_id,
                url=f"https://example.test/{external_id}",
                http_status=500 if status == "failed" else 200,
                fetched_at=BASE_TIME,
                status=status,
            )
        )
    database.close()


@pytest.fixture
def seeded_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "dim.db"
    _seed_db(db_path)
    return db_path


def test_legacy_stats_output_unchanged(seeded_db: Path) -> None:
    result = runner.invoke(app, ["db", "stats", "--db", str(seeded_db)])

    assert result.exit_code == 0, result.output
    lines = result.output.splitlines()
    assert lines[0].startswith("source")
    assert "TOTAL" in result.output
    # 旧表头不包含新增维度列
    assert "failed_pct" not in result.output


def test_category_dimension_table_output(seeded_db: Path) -> None:
    result = runner.invoke(
        app, ["db", "stats", "--db", str(seeded_db), "--dim", "category"]
    )

    assert result.exit_code == 0, result.output
    assert "技术" in result.output
    assert "生活" in result.output
    assert UNCATEGORIZED_LABEL in result.output
    assert "failed_pct" in result.output
    assert "TOTAL" in result.output
    # 技术桶 2 条失败 raw / 3 条关联 raw
    assert "66.67%" in result.output


def test_tag_dimension_with_source_and_time_filters(seeded_db: Path) -> None:
    result = runner.invoke(
        app,
        [
            "db", "stats", "--db", str(seeded_db), "--dim", "tag",
            "--source", "s1",
            "--start", "2024-03-01T00:00:00",
            "--end", "2024-03-04T00:00:00",
        ],
    )

    assert result.exit_code == 0, result.output
    # 区间覆盖 a1(day0)/a2(day1)/a3(day2)，a4(day3 无标签) 不计标签
    assert "数据库,带逗号" in result.output
    assert '引号"x"' in result.output
    # s2/b1 在 day5，超出区间；性能来自 s1 的 a1/a2，仍在区间内
    assert "测试" not in result.output


def test_invalid_dimension_exits_2(seeded_db: Path) -> None:
    result = runner.invoke(
        app, ["db", "stats", "--db", str(seeded_db), "--dim", "author"]
    )

    assert result.exit_code == 2
    assert "不支持的统计维度" in result.output


def test_filters_without_dimension_exits_2(seeded_db: Path) -> None:
    result = runner.invoke(
        app, ["db", "stats", "--db", str(seeded_db), "--source", "s1"]
    )

    assert result.exit_code == 2
    assert "--dim" in result.output


def test_invalid_time_range_exits_2(seeded_db: Path) -> None:
    result = runner.invoke(
        app,
        [
            "db", "stats", "--db", str(seeded_db), "--dim", "tag",
            "--start", "2024-03-10T00:00:00",
            "--end", "2024-03-01T00:00:00",
        ],
    )

    assert result.exit_code == 2
    assert "起点不能晚于终点" in result.output


def test_no_match_prints_hint_and_exits_zero(seeded_db: Path) -> None:
    result = runner.invoke(
        app,
        ["db", "stats", "--db", str(seeded_db), "--dim", "category", "-s", "ghost"],
    )

    assert result.exit_code == 0
    assert "无匹配数据" in result.output
    assert "TOTAL" not in result.output


def test_csv_export_handles_chinese_and_special_chars(
    seeded_db: Path, tmp_path: Path
) -> None:
    csv_path = tmp_path / "导出" / "统计,报表.csv"
    result = runner.invoke(
        app,
        [
            "db", "stats", "--db", str(seeded_db), "--dim", "tag",
            "--output", str(csv_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "已导出 CSV" in result.output
    assert csv_path.exists()

    raw = csv_path.read_bytes()
    # utf-8-sig BOM，Excel 打开中文不乱码
    assert raw[:3] == b"\xef\xbb\xbf"

    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))

    assert rows[0] == [
        "dimension",
        "dimension_value",
        "record_count",
        "last_updated_at",
        "raw_total",
        "raw_failed",
        "raw_failed_ratio",
    ]
    values = {row[1]: row for row in rows[1:]}
    # 中文原样保留
    assert "性能" in values
    # 含逗号的字段被引号转义后仍能完整还原
    assert "数据库,带逗号" in values
    # 含双引号的字段同样可还原
    assert '引号"x"' in values
    for row in rows[1:]:
        # 占比为 0~1 的四位小数字符串
        ratio = float(row[6])
        assert 0.0 <= ratio <= 1.0
        # 时间列非空
        assert row[3]


def test_csv_export_empty_result_writes_header_only(
    seeded_db: Path, tmp_path: Path
) -> None:
    csv_path = tmp_path / "empty.csv"
    result = runner.invoke(
        app,
        [
            "db", "stats", "--db", str(seeded_db), "--dim", "tag",
            "-s", "ghost", "--output", str(csv_path),
        ],
    )

    assert result.exit_code == 0
    assert "无匹配数据" in result.output
    assert csv_path.exists()
    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.reader(handle))
    assert len(rows) == 1  # 仅表头


def test_traversal_path_is_neutralized(tmp_path: Path, monkeypatch: Any) -> None:
    target = tmp_path / "work"
    target.mkdir()
    monkeypatch.chdir(target)

    neutralized = safe_output_path("out/../../evil.csv")
    # 解析后仍被锚定在工作目录之下，无法逃逸
    assert neutralized.parts[0] != ".."
    resolved = (target / neutralized).resolve()
    assert target.resolve() in resolved.parents

    absolute = safe_output_path(str(tmp_path / "a" / ".." / ".." / "etc" / "evil.csv"))
    assert ".." not in absolute.parts
    assert absolute == tmp_path / "a" / "_" / "_" / "etc" / "evil.csv"


def test_traversal_export_does_not_escape_output_root(
    seeded_db: Path, tmp_path: Path, monkeypatch: Any
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    monkeypatch.chdir(workdir)
    sentinel = workdir.parent / "evil.csv"
    assert not sentinel.exists()

    result = runner.invoke(
        app,
        [
            "db", "stats", "--db", str(seeded_db), "--dim", "tag",
            "--output", "../../evil.csv",
        ],
    )

    assert result.exit_code == 0, result.output
    assert not sentinel.exists()
    # 文件被清洗后写在工作目录内（每个 ``..`` 分量各清洗为一个 ``_`` 目录）
    written = list(workdir.rglob("evil.csv"))
    assert written == [workdir / "_" / "_" / "evil.csv"]


def test_safe_output_path_rejects_dot_only_inputs() -> None:
    for bad in (".", "..", "/", "//"):
        with pytest.raises(StoreError, match="非法导出路径"):
            safe_output_path(bad)


def test_missing_database_still_exits_1(tmp_path: Path) -> None:
    result = runner.invoke(
        app, ["db", "stats", "--db", str(tmp_path / "missing.db"), "--dim", "tag"]
    )

    assert result.exit_code == 1
    assert "数据库不存在" in result.output
