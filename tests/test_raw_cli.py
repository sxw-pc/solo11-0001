"""raw search / raw export 命令行：筛选、分页、导出模式、异常文件与退出码。"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.config import SourceConfig
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

runner = CliRunner()


@pytest.fixture(scope="session")
def golden_seed(
    tmp_path_factory: pytest.TempPathFactory, mock_base_url: str
) -> Path:
    """整组测试只跑一次 mock 采集，生成 golden 数据集（23 条 raw）。"""

    def _make(name: str, source_type: str = "api", **overrides: Any) -> SourceConfig:
        payload: dict[str, Any] = {
            "name": name,
            "type": source_type,
            "base_url": mock_base_url,
        }
        payload.update(overrides)
        return SourceConfig.model_validate(payload)

    root = tmp_path_factory.mktemp("golden")
    db_path = root / "search.db"
    raw_dir = root / "raw"
    database = Database(db_path)
    database.init()
    raw_store = RawStore(raw_dir)
    try:
        run_source(_make("demo_api"), database, raw_store, limit=15)
        run_source(
            _make("demo_api_flaky", detail_path="/api/items/{id}?fail=1"),
            database,
            raw_store,
            limit=3,
        )
        run_source(_make("demo_html", "html"), database, raw_store, limit=5)
    finally:
        database.close()
    return root


@pytest.fixture
def dataset(tmp_path: Path, golden_seed: Path) -> tuple[Path, Path, Path]:
    """每个用例复制一份 golden 数据，坏文件改动互不影响。

    复制后把 raw_records 里的绝对路径前缀改写成新目录，否则库里的
    raw_path 仍指向 golden 原件。
    """

    root = tmp_path / "work"
    shutil.copytree(golden_seed, root)
    db_path = root / "search.db"
    raw_dir = root / "raw"
    database = Database(db_path)
    try:
        with database.engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE raw_records SET raw_path = REPLACE(raw_path, ?, ?)",
                (str(golden_seed / "raw"), str(raw_dir)),
            )
    finally:
        database.close()
    return db_path, raw_dir, root


def _search(db_path: Path, raw_dir: Path, *extra: str) -> Any:
    return runner.invoke(
        app,
        ["raw", "search", "--db", str(db_path), "--raw-dir", str(raw_dir), *extra],
    )


def test_search_all_mock_data_and_default_limit(dataset: tuple[Path, Path, Path]) -> None:
    db_path, raw_dir, _ = dataset
    result = _search(db_path, raw_dir)
    assert result.exit_code == 0, result.output
    assert "共 23 条满足条件" in result.output
    assert "demo_api" in result.output and "demo_html" in result.output


def test_search_filters_source_status_keyword_and_id(
    dataset: tuple[Path, Path, Path],
) -> None:
    db_path, raw_dir, _ = dataset

    api = _search(db_path, raw_dir, "-s", "demo_api", "--format", "json")
    assert api.exit_code == 0
    data = json.loads(api.output)
    assert data["total"] == 15
    assert all(row["source"] == "demo_api" for row in data["results"])

    failed = _search(db_path, raw_dir, "--status", "failed", "--format", "json")
    failed_data = json.loads(failed.output)
    assert failed_data["total"] == 3
    assert all(row["status"] == "failed" for row in failed_data["results"])

    errors = _search(db_path, raw_dir, "--error-keyword", "HTTP 500", "--format", "json")
    assert json.loads(errors.output)["total"] == 3

    one = _search(db_path, raw_dir, "--id", "api-2", "-s", "demo_api", "--format", "json")
    one_data = json.loads(one.output)
    assert one_data["total"] == 1
    assert one_data["results"][0]["external_id"] == "api-2"
    assert one_data["results"][0]["raw_path"].endswith("api-2.json")


def test_search_limit_and_offset_paginate(dataset: tuple[Path, Path, Path]) -> None:
    db_path, raw_dir, _ = dataset
    first = _search(db_path, raw_dir, "-s", "demo_api", "--limit", "10", "--format", "json")
    first_data = json.loads(first.output)
    assert first_data["total"] == 15 and len(first_data["results"]) == 10

    second = _search(
        db_path,
        raw_dir,
        "-s",
        "demo_api",
        "--limit",
        "10",
        "--offset",
        "10",
        "--format",
        "json",
    )
    second_data = json.loads(second.output)
    assert len(second_data["results"]) == 5
    first_ids = {row["external_id"] for row in first_data["results"]}
    second_ids = {row["external_id"] for row in second_data["results"]}
    assert first_ids.isdisjoint(second_ids)


def test_search_time_range(dataset: tuple[Path, Path, Path]) -> None:
    db_path, raw_dir, _ = dataset
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    hit = _search(db_path, raw_dir, "--from", today, "--to", today, "--format", "json")
    assert json.loads(hit.output)["total"] == 23

    miss = _search(db_path, raw_dir, "--from", "2099-01-01", "--format", "json")
    assert json.loads(miss.output)["total"] == 0

    bad = _search(db_path, raw_dir, "--from", "not-a-date")
    assert bad.exit_code == 2
    assert "时间格式" in bad.output


def test_search_body_keyword_loads_raw_files(dataset: tuple[Path, Path, Path]) -> None:
    db_path, raw_dir, _ = dataset
    result = _search(
        db_path,
        raw_dir,
        "-s",
        "demo_api",
        "--body-keyword",
        "数据库",
        "--format",
        "json",
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert [row["external_id"] for row in data["results"]] == ["api-2"]
    assert data["body_scanned"] == 15


def test_search_body_keyword_respects_max_scan(dataset: tuple[Path, Path, Path]) -> None:
    db_path, raw_dir, _ = dataset
    result = _search(
        db_path,
        raw_dir,
        "-s",
        "demo_api",
        "--body-keyword",
        "不存在的关键词",
        "--max-scan",
        "3",
    )
    assert result.exit_code == 0
    assert "正文扫描 3 个 raw 文件" in result.output
    assert "扫描上限" in result.output


def test_search_corrupt_raw_is_warning_not_failure(
    dataset: tuple[Path, Path, Path],
) -> None:
    db_path, raw_dir, _ = dataset
    target = next((raw_dir / "demo_api").rglob("api-1.json"))
    target.write_text("{损坏的 json", encoding="utf-8")

    result = _search(
        db_path, raw_dir, "--id", "api-1", "-s", "demo_api", "--body-keyword", "x"
    )
    assert result.exit_code == 0
    assert "警告" in result.output and "JSON 损坏" in result.output
    assert "返回 0 条" in result.output

    # 不带正文检索时完全不读文件，损坏文件不影响元信息检索
    meta = _search(db_path, raw_dir, "--id", "api-1", "-s", "demo_api")
    assert meta.exit_code == 0
    assert "返回 1 条" in meta.output


def test_search_missing_and_unreadable_raw_are_isolated(
    dataset: tuple[Path, Path, Path],
) -> None:
    db_path, raw_dir, _ = dataset
    target = next((raw_dir / "demo_api").rglob("api-2.json"))
    target.chmod(0o000)
    try:
        denied = _search(
            db_path,
            raw_dir,
            "--id",
            "api-2",
            "-s",
            "demo_api",
            "--body-keyword",
            "x",
        )
        assert denied.exit_code == 0
        # root 下 chmod 000 仍可读，其余环境应观察到权限不足告警
        if not Path("/proc/1/root").exists():
            assert "权限不足" in denied.output
    finally:
        target.chmod(0o644)

    target.unlink()
    missing = _search(
        db_path,
        raw_dir,
        "--id",
        "api-2",
        "-s",
        "demo_api",
        "--body-keyword",
        "x",
    )
    assert missing.exit_code == 0
    assert "raw 文件不存在" in missing.output


def test_export_default_is_metadata_only(dataset: tuple[Path, Path, Path]) -> None:
    db_path, raw_dir, root = dataset
    output = root / "export" / "meta.json"
    result = runner.invoke(
        app,
        [
            "raw",
            "export",
            "-o",
            str(output),
            "-s",
            "demo_api",
            "--limit",
            "5",
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "仅元信息" in result.output
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["with_content"] is False
    assert data["exported"] == 5 and data["total"] == 15
    assert "content" not in data["results"][0]
    assert set(data["results"][0]) == {
        "source",
        "external_id",
        "url",
        "http_status",
        "fetched_at",
        "status",
        "error",
        "raw_path",
    }


def test_export_with_content_embeds_full_payload(
    dataset: tuple[Path, Path, Path],
) -> None:
    db_path, raw_dir, root = dataset
    output = root / "full.json"
    result = runner.invoke(
        app,
        [
            "raw",
            "export",
            "-o",
            str(output),
            "-s",
            "demo_api",
            "--limit",
            "2",
            "--with-content",
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["with_content"] is True
    for item in data["results"]:
        assert isinstance(item["content"], dict)
        assert "body" in item["content"] and "headers" in item["content"]


def test_export_with_content_records_bad_file_inline(
    dataset: tuple[Path, Path, Path],
) -> None:
    db_path, raw_dir, root = dataset
    target = next((raw_dir / "demo_api").rglob("api-3.json"))
    target.write_text("", encoding="utf-8")  # 空文件
    output = root / "bad.json"
    result = runner.invoke(
        app,
        [
            "raw",
            "export",
            "-o",
            str(output),
            "--id",
            "api-3",
            "-s",
            "demo_api",
            "--with-content",
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
        ],
    )
    assert result.exit_code == 0
    assert "警告" in result.output
    data = json.loads(output.read_text(encoding="utf-8"))
    item = data["results"][0]
    assert item["content"] is None
    assert "为空" in item["content_error"]


def test_export_atomic_temp_file_cleaned_up(dataset: tuple[Path, Path, Path]) -> None:
    db_path, raw_dir, root = dataset
    output = root / "atomic.json"
    result = runner.invoke(
        app,
        [
            "raw",
            "export",
            "-o",
            str(output),
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
        ],
    )
    assert result.exit_code == 0
    assert output.is_file()
    assert not list(output.parent.glob("*.tmp"))


def test_search_exit_codes(dataset: tuple[Path, Path, Path], tmp_path: Path) -> None:
    db_path, raw_dir, _ = dataset

    bad_status = _search(db_path, raw_dir, "--status", "bogus")
    assert bad_status.exit_code == 2
    assert "只允许 ok 或 failed" in bad_status.output

    bad_format = _search(db_path, raw_dir, "--format", "xml")
    assert bad_format.exit_code == 2

    missing = runner.invoke(
        app,
        ["raw", "search", "--db", str(tmp_path / "absent.db"), "--raw-dir", str(raw_dir)],
    )
    assert missing.exit_code == 1
    assert "数据库不存在" in missing.output


def test_dry_run_results_not_searchable(
    tmp_path: Path, write_config: Any, source_payload: Any
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "dry.db"
    raw_dir = tmp_path / "dry-raw"
    run = runner.invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "5",
            "--dry-run",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(raw_dir),
        ],
    )
    assert run.exit_code == 0, run.output

    # dry-run 会初始化空库但不写任何行，也不创建 raw 目录
    assert db_path.exists()
    search = runner.invoke(
        app,
        ["raw", "search", "--db", str(db_path), "--raw-dir", str(raw_dir)],
    )
    assert search.exit_code == 0
    assert "共 0 条满足条件" in search.output

    empty = _search(db_path, raw_dir, "--format", "json")
    assert json.loads(empty.output)["total"] == 0
    assert not raw_dir.exists()
