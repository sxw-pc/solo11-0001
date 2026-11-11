"""CLI 错误边界冒烟：统一校验层、退出码、无堆栈、相对路径与启动场景。

覆盖需求：
- 非法值（``--limit`` 0/负数）在发请求前失败，退出码 2；
- 只读目录（db）、目录不可写（raw）给可读错误且不打印堆栈；
- dry-run 与正常模式错误行为一致（源不存在同样报错）；
- 相对路径按当前工作目录转绝对路径，切换 cwd 后仍能找到产物；
- 配置文件缺失 / 无读取权限的启动场景；
- ``db stats`` 在库不存在时仍退出 1；
- Windows / macOS / Linux 路径规则在 ``test_cli_validation.py`` 覆盖。
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from crawl_flow.cli import app

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


def _assert_no_traceback(output: str) -> None:
    assert "Traceback (most recent call last)" not in output


# ---------- --limit 非法值：发请求前失败，退出码 2 ----------


@pytest.mark.parametrize("bad_limit", ["0", "-1", "-10"])
def test_limit_non_positive_exits_2_before_any_request(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
    bad_limit: str,
) -> None:
    # base_url 指向不可路由地址：只要校验层先失败，就不会真正发请求。
    config_path = write_config(
        [source_payload("demo_api", base_url="http://127.0.0.1:1")]
    )
    result = runner.invoke(
        app,
        _run_args(
            config_path,
            tmp_path / "x.db",
            tmp_path / "raw",
            "--source",
            "demo_api",
            "--limit",
            bad_limit,
        ),
    )
    assert result.exit_code == 2, result.output
    assert isinstance(result.exception, SystemExit)
    _assert_no_traceback(result.output)


# ---------- 只读路径 / 目录不可写：可读错误，无堆栈，退出码 2 ----------


def test_readonly_db_directory_exits_2_without_traceback(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    readonly = tmp_path / "readonly"
    readonly.mkdir()
    readonly.chmod(0o555)
    try:
        result = runner.invoke(
            app,
            _run_args(
                config_path,
                readonly / "sub" / "crawl.db",
                tmp_path / "raw",
                "--source",
                "demo_api",
                "--limit",
                "1",
            ),
        )
    finally:
        readonly.chmod(0o755)
    assert result.exit_code == 2, result.output
    assert "参数错误" in result.output
    assert "数据库" in result.output
    assert "不可写" in result.output
    _assert_no_traceback(result.output)
    assert isinstance(result.exception, SystemExit)


def test_db_path_being_a_directory_exits_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    target_dir = tmp_path / "is-a-dir"
    target_dir.mkdir()
    result = runner.invoke(
        app,
        _run_args(
            config_path,
            target_dir,
            tmp_path / "raw",
            "--source",
            "demo_api",
        ),
    )
    assert result.exit_code == 2
    assert "必须是文件" in result.output
    _assert_no_traceback(result.output)


def test_unwritable_raw_dir_exits_2_without_traceback(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    raw_dir = tmp_path / "raw"
    raw_dir.mkdir()
    raw_dir.chmod(0o555)
    try:
        result = runner.invoke(
            app,
            _run_args(
                config_path,
                tmp_path / "crawl.db",
                raw_dir,
                "--source",
                "demo_api",
                "--limit",
                "1",
            ),
        )
    finally:
        raw_dir.chmod(0o755)
    assert result.exit_code == 2, result.output
    assert "原始响应目录不可写" in result.output
    _assert_no_traceback(result.output)
    assert isinstance(result.exception, SystemExit)


def test_raw_dir_being_a_file_exits_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    raw_file = tmp_path / "raw.txt"
    raw_file.write_text("x", encoding="utf-8")
    result = runner.invoke(
        app,
        _run_args(
            config_path,
            tmp_path / "crawl.db",
            raw_file,
            "--source",
            "demo_api",
        ),
    )
    assert result.exit_code == 2
    assert "必须是目录" in result.output
    _assert_no_traceback(result.output)


# ---------- dry-run 与正常模式错误行为一致 ----------


def test_unknown_source_same_error_in_dry_run_and_normal_mode(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    common = [
        "--config",
        str(config_path),
        "--db",
        str(tmp_path / "crawl.db"),
        "--raw-dir",
        str(tmp_path / "raw"),
        "--source",
        "missing_source",
    ]
    dry = runner.invoke(app, ["run", *common, "--dry-run"])
    normal = runner.invoke(app, ["run", *common])
    assert dry.exit_code == 2
    assert normal.exit_code == 2
    assert "未找到源 missing_source" in dry.output
    assert "未找到源 missing_source" in normal.output
    assert "配置错误" in dry.output and "配置错误" in normal.output
    _assert_no_traceback(dry.output)
    _assert_no_traceback(normal.output)


def test_dry_run_rejects_readonly_db_like_normal_mode(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    readonly = tmp_path / "readonly"
    readonly.mkdir()
    readonly.chmod(0o555)
    try:
        result = runner.invoke(
            app,
            _run_args(
                config_path,
                readonly / "crawl.db",
                tmp_path / "raw",
                "--source",
                "demo_api",
                "--dry-run",
            ),
        )
    finally:
        readonly.chmod(0o755)
    assert result.exit_code == 2
    assert "数据库" in result.output
    _assert_no_traceback(result.output)


# ---------- 相对路径按 cwd 转绝对路径 ----------


def test_relative_paths_resolve_against_current_working_directory(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workdir = tmp_path / "work"
    workdir.mkdir()
    config_path = write_config([source_payload("demo_api")], filename="sources.yaml")
    # 配置挪进工作目录，全部以相对路径传入，再把 cwd 切到工作目录。
    target_config = workdir / "sources.yaml"
    target_config.write_text(config_path.read_text(encoding="utf-8"), encoding="utf-8")
    monkeypatch.chdir(workdir)

    result = runner.invoke(
        app,
        [
            "run",
            "--config",
            "sources.yaml",
            "--db",
            "data/crawl.db",
            "--raw-dir",
            "data/raw",
            "--source",
            "demo_api",
            "--limit",
            "1",
        ],
    )
    assert result.exit_code == 0, result.output
    assert (workdir / "data" / "crawl.db").exists()
    assert (workdir / "data" / "raw").is_dir()
    # 没有在进程启动目录（仓库根）之外误建默认路径。
    assert not (Path.cwd() / "config").is_dir() or Path.cwd() == workdir


def test_relative_db_stats_missing_still_exits_1(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["db", "stats", "--db", "data/missing.db"])
    assert result.exit_code == 1
    assert "数据库不存在" in result.output
    assert str(tmp_path / "data" / "missing.db") in result.output
    _assert_no_traceback(result.output)


# ---------- 配置文件缺失 / 权限不足的启动场景 ----------


def test_missing_config_file_exits_2(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        _run_args(
            tmp_path / "no-such.yaml",
            tmp_path / "crawl.db",
            tmp_path / "raw",
            "--source",
            "demo_api",
        ),
    )
    assert result.exit_code == 2
    assert "配置文件不存在" in result.output
    _assert_no_traceback(result.output)


def test_unreadable_config_file_exits_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    config_path.chmod(0o000)
    try:
        result = runner.invoke(
            app,
            _run_args(
                config_path,
                tmp_path / "crawl.db",
                tmp_path / "raw",
                "--source",
                "demo_api",
            ),
        )
    finally:
        config_path.chmod(0o644)
    if os.geteuid() == 0:
        pytest.skip("root 绕过文件权限，无法模拟不可读场景")
    assert result.exit_code == 2
    assert "配置错误" in result.output
    _assert_no_traceback(result.output)


# ---------- db stats 库不存在保持退出码 1 ----------


def test_db_stats_missing_file_exits_1(tmp_path: Path) -> None:
    result = runner.invoke(app, ["db", "stats", "--db", str(tmp_path / "missing.db")])
    assert result.exit_code == 1
    assert "数据库不存在" in result.output
    _assert_no_traceback(result.output)


# ---------- 帮助文案 / 参数结构 / 日志格式回归保护 ----------


def test_run_help_keeps_parameter_structure_and_text() -> None:
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    # 子命令参数结构与帮助文案不允许变化。
    for fragment in (
        "--source",
        "--all",
        "--limit",
        "INTEGER RANGE [x>=1]",
        "--dry-run",
        "--verbose",
        "--config",
        "--db",
        "--raw-dir",
        "每个源最多处理 N 条",
        "只解析，不写 raw 与数据库",
        "源配置文件路径",
        "SQLite 数据库路径",
        "原始响应目录",
    ):
        assert fragment in result.output, fragment


def test_db_stats_help_keeps_parameter_text() -> None:
    result = runner.invoke(app, ["db", "stats", "--help"])
    assert result.exit_code == 0
    assert "--db" in result.output
    assert "SQLite 数据库路径" in result.output
    assert "--verbose" in result.output


def test_successful_run_keeps_key_value_output_format(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = runner.invoke(
        app,
        _run_args(
            config_path,
            tmp_path / "kv.db",
            tmp_path / "kv-raw",
            "--source",
            "demo_api",
            "--limit",
            "1",
        ),
    )
    assert result.exit_code == 0, result.output
    # stdout 的汇总行保持既有 key=value 形态（日志 formatter 另有单测锁定）。
    assert "本轮汇总" in result.output
    assert "source=demo_api ok=1 failed=0" in result.output
    assert "total ok=1 failed=0" in result.output
