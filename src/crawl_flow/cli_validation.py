"""CLI 边界的统一参数校验层。

所有在发出任何网络请求之前就能判定的非法输入都在这里失败：

- 非法标量值（如 ``--limit`` 为 0 或负数）；
- 相对路径统一按当前工作目录转成绝对路径（Windows / macOS / Linux
  都通过 :mod:`pathlib` 的平台规则处理）；
- 数据库路径、原始响应目录在打开前做可读/可写探测；
- 配置文件存在性仍由 :func:`crawl_flow.config.load_sources` 负责，
  这里只做路径绝对化，保持「配置错误」出口不变。

校验失败统一抛 :class:`~crawl_flow.errors.CliArgumentError`，由 CLI 边界
打印可读信息、不输出堆栈并以退出码 2 结束。
"""

from __future__ import annotations

import os
from pathlib import Path

from crawl_flow.errors import CliArgumentError


def as_absolute_path(path: str | Path, *, cwd: Path | None = None) -> Path:
    """把路径按当前工作目录转成绝对路径。

    - 绝对路径（含 Windows 盘符 ``C:\\...`` 与 UNC 路径）原样返回；
    - 相对路径拼接当前工作目录（``os.getcwd()``），不解析软链、不要求存在；
    - 展开 ``~`` / ``~user``。

    纯路径计算拆在 :func:`_join_absolute` 里，便于用
    :class:`pathlib.PurePosixPath` / :class:`pathlib.PureWindowsPath`
    在任意平台上验证三套路径规则。
    """

    candidate = Path(path).expanduser()
    base = Path(cwd) if cwd is not None else Path.cwd()
    return _join_absolute(candidate, base)


def _join_absolute(path: Path, cwd: Path) -> Path:
    if path.is_absolute():
        return path
    return cwd / path


def validate_limit(limit: int | None) -> int | None:
    """``--limit`` 必须缺省或为正整数；0 / 负数在发请求前失败。"""

    if limit is not None and limit < 1:
        raise CliArgumentError(f"--limit 必须是大于等于 1 的正整数，收到: {limit}")
    return limit


def validate_config_path(path: str | Path) -> Path:
    """配置路径只做绝对化；存在性与读取权限留给配置加载层报错。"""

    return as_absolute_path(path)


def validate_db_path(path: str | Path) -> Path:
    """校验 SQLite 数据库路径并返回绝对路径。

    - 已存在且是文件：其父目录必须可写；
    - 已存在但是目录：无法作为数据库文件，直接失败；
    - 不存在：沿路径向上找到最近的已存在祖先，必须是可写目录。
    """

    if str(path) == ":memory:":
        # SQLite 内存库是内部能力，无文件系统路径可探测，直接透传。
        return Path(":memory:")
    db_path = as_absolute_path(path)
    if db_path.exists():
        if db_path.is_dir():
            raise CliArgumentError(f"数据库路径必须是文件，当前是目录: {db_path}")
        _ensure_writable_parent(db_path, kind="数据库")
        return db_path

    ancestor = _nearest_existing_ancestor(db_path)
    if not ancestor.is_dir():
        raise CliArgumentError(f"数据库路径的父级不是目录: {db_path}")
    _ensure_writable(ancestor, f"数据库所在目录不可写: {db_path}（目录 {ancestor} 无写权限）")
    return db_path


def validate_raw_dir(path: str | Path) -> Path:
    """校验原始响应目录并返回绝对路径。

    - 已存在：必须是目录且可写；
    - 不存在：最近的已存在祖先必须是可写目录，运行时再逐级创建。
    """

    raw_path = as_absolute_path(path)
    if raw_path.exists():
        if not raw_path.is_dir():
            raise CliArgumentError(f"原始响应目录必须是目录，当前是文件: {raw_path}")
        _ensure_writable(raw_path, f"原始响应目录不可写: {raw_path}（无写权限）")
        return raw_path

    ancestor = _nearest_existing_ancestor(raw_path)
    if not ancestor.is_dir():
        raise CliArgumentError(f"原始响应目录的父级不是目录: {raw_path}")
    _ensure_writable(
        ancestor,
        f"原始响应目录不可写: {raw_path}（目录 {ancestor} 无写权限）",
    )
    return raw_path


def _nearest_existing_ancestor(path: Path) -> Path:
    ancestor = path.parent
    while not ancestor.exists() and ancestor != ancestor.parent:
        ancestor = ancestor.parent
    return ancestor


def _ensure_writable_parent(file_path: Path, *, kind: str) -> None:
    _ensure_writable(
        file_path.parent,
        f"{kind}所在目录不可写: {file_path}（目录 {file_path.parent} 无写权限）",
    )


def _ensure_writable(directory: Path, message: str) -> None:
    if not os.access(directory, os.W_OK):
        raise CliArgumentError(message)
