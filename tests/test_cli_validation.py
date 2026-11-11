"""统一 CLI 校验层单元测试。

路径规则用 ``PurePosixPath`` / ``PureWindowsPath`` 在任意平台上同时覆盖
macOS / Linux（POSIX）与 Windows 语义；真实文件系统的权限行为在
``test_cli_errors.py`` 里通过 CLI 冒烟覆盖。
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath, PureWindowsPath

import pytest

from crawl_flow.cli_validation import (
    _join_absolute,
    as_absolute_path,
    validate_db_path,
    validate_limit,
    validate_raw_dir,
)
from crawl_flow.errors import CliArgumentError


# ---------- --limit ----------


@pytest.mark.parametrize("value", [0, -1, -100])
def test_validate_limit_rejects_non_positive(value: int) -> None:
    with pytest.raises(CliArgumentError) as exc_info:
        validate_limit(value)
    assert str(value) in str(exc_info.value)


@pytest.mark.parametrize("value", [None, 1, 2, 999])
def test_validate_limit_accepts_none_or_positive(value: int | None) -> None:
    assert validate_limit(value) == value


# ---------- 相对路径 → 绝对路径：三套平台规则 ----------


def test_join_absolute_posix_rules() -> None:
    cwd = PurePosixPath("/home/alice/work")
    assert _join_absolute(PurePosixPath("data/raw"), cwd) == PurePosixPath(
        "/home/alice/work/data/raw"
    )
    assert _join_absolute(PurePosixPath("../shared/raw"), cwd) == PurePosixPath(
        "/home/alice/work/../shared/raw"
    )
    # 绝对路径原样返回，不与 cwd 拼接。
    assert _join_absolute(PurePosixPath("/var/tmp/crawl.db"), cwd) == PurePosixPath(
        "/var/tmp/crawl.db"
    )
    # 根目录与点号。
    assert _join_absolute(PurePosixPath("."), cwd) == cwd


def test_join_absolute_windows_rules() -> None:
    cwd = PureWindowsPath(r"C:\Users\alice\work")
    # 相对路径按 Windows 反斜杠拼接。
    assert _join_absolute(PureWindowsPath(r"data\raw"), cwd) == PureWindowsPath(
        r"C:\Users\alice\work\data\raw"
    )
    assert _join_absolute(PureWindowsPath("data/raw"), cwd) == PureWindowsPath(
        r"C:\Users\alice\work\data\raw"
    )
    # 盘符绝对路径与 UNC 路径原样返回。
    assert _join_absolute(PureWindowsPath(r"D:\crawl\crawl.db"), cwd) == PureWindowsPath(
        r"D:\crawl\crawl.db"
    )
    assert _join_absolute(PureWindowsPath(r"\\server\share\raw"), cwd) == PureWindowsPath(
        r"\\server\share\raw"
    )


def test_as_absolute_path_uses_current_working_directory(tmp_path: Path) -> None:
    raw = tmp_path / "data" / "raw"
    resolved = as_absolute_path("data/raw", cwd=tmp_path)
    assert resolved.is_absolute()
    assert resolved == raw
    # 已绝对化的路径保持不变。
    assert as_absolute_path(raw, cwd=tmp_path) == raw


def test_as_absolute_path_expands_user(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    resolved = as_absolute_path("~/crawl/raw", cwd=tmp_path)
    assert resolved.is_absolute()
    assert str(resolved).startswith(str(tmp_path))


# ---------- 数据库路径 ----------


def test_validate_db_path_readonly_ancestor(tmp_path: Path) -> None:
    readonly = tmp_path / "readonly"
    readonly.mkdir()
    readonly.chmod(0o555)
    try:
        with pytest.raises(CliArgumentError) as exc_info:
            validate_db_path(readonly / "sub" / "crawl.db")
        message = str(exc_info.value)
        assert "数据库" in message
        assert "不可写" in message
    finally:
        readonly.chmod(0o755)


def test_validate_db_path_existing_directory_rejected(tmp_path: Path) -> None:
    target = tmp_path / "a-directory"
    target.mkdir()
    with pytest.raises(CliArgumentError) as exc_info:
        validate_db_path(target)
    assert "必须是文件" in str(exc_info.value)


def test_validate_db_path_existing_file_requires_writable_parent(tmp_path: Path) -> None:
    db_file = tmp_path / "crawl.db"
    db_file.write_text("", encoding="utf-8")
    tmp_path.chmod(0o555)
    try:
        with pytest.raises(CliArgumentError) as exc_info:
            validate_db_path(db_file)
        assert "不可写" in str(exc_info.value)
    finally:
        tmp_path.chmod(0o755)


def test_validate_db_path_writable_returns_absolute(tmp_path: Path) -> None:
    resolved = validate_db_path(tmp_path / "nested" / "crawl.db")
    assert resolved.is_absolute()
    assert resolved == (tmp_path / "nested" / "crawl.db").resolve() or resolved == (
        tmp_path / "nested" / "crawl.db"
    )


def test_validate_db_path_passes_through_memory() -> None:
    assert validate_db_path(":memory:") == Path(":memory:")


# ---------- raw 目录 ----------


def test_validate_raw_dir_readonly_existing_dir(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    raw.chmod(0o555)
    try:
        with pytest.raises(CliArgumentError) as exc_info:
            validate_raw_dir(raw)
        assert "原始响应目录不可写" in str(exc_info.value)
    finally:
        raw.chmod(0o755)


def test_validate_raw_dir_when_path_is_file(tmp_path: Path) -> None:
    target = tmp_path / "raw.txt"
    target.write_text("x", encoding="utf-8")
    with pytest.raises(CliArgumentError) as exc_info:
        validate_raw_dir(target)
    assert "必须是目录" in str(exc_info.value)


def test_validate_raw_dir_missing_under_readonly_ancestor(tmp_path: Path) -> None:
    readonly = tmp_path / "readonly"
    readonly.mkdir()
    readonly.chmod(0o555)
    try:
        with pytest.raises(CliArgumentError) as exc_info:
            validate_raw_dir(readonly / "raw")
        assert "不可写" in str(exc_info.value)
    finally:
        readonly.chmod(0o755)


def test_validate_raw_dir_creatable_returns_absolute(tmp_path: Path) -> None:
    resolved = validate_raw_dir(tmp_path / "new" / "raw")
    assert resolved.is_absolute()
    assert not resolved.exists()  # 只校验，不提前创建
