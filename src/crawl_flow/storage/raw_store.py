"""原始响应落盘：``data/raw/{source}/{YYYY-MM-DD}/{external_id}.json``。"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from crawl_flow.errors import StoreError

SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")
MAX_ID_LENGTH = 120


def safe_external_id(external_id: str) -> str:
    """把 external_id 转成安全的文件名片段。"""

    candidate = SAFE_NAME.sub("_", external_id).strip("._")
    return (candidate or "unknown")[:MAX_ID_LENGTH]


# 路径分量中的路径分隔符、NUL 与 ``..`` 穿越片段；允许中文等 Unicode 字符。
_UNSAFE_PATH_PART = re.compile(r"[\\/\x00]|(?:^|[^.])\.\.(?![.])")


def safe_path_part(part: str) -> str:
    """清洗单个路径分量：禁止分隔符、NUL 与 ``..``，保留中文等合法字符。"""

    candidate = _UNSAFE_PATH_PART.sub("_", part)
    cleaned = candidate.lstrip(".") or "_"
    return cleaned[:255]


def safe_output_path(path: str | Path) -> Path:
    """把用户提供的导出路径规范化为安全路径，防止目录穿越。

    与 :class:`RawStore` 落盘遵循同一套安全原则：路径分量逐个清洗，
    ``..``、``/``、``\\``、NUL 等可用于穿越的字符替换为 ``_``，
    前导 ``.`` 去除，空分量以 ``_`` 占位；绝对路径保留其根锚点，
    相对路径相对当前工作目录解析，清洗结果不可能指向锚点之外。
    与落盘文件名不同，导出路径允许中文等 Unicode 字符。
    """

    candidate = Path(path)
    parts = [part for part in candidate.parts if part not in ("", "/", "\\")]
    cleaned = [safe_path_part(part) for part in parts]
    if not cleaned or all(not part.strip("._") for part in cleaned):
        raise StoreError(f"非法导出路径: {path}")
    if candidate.is_absolute():
        return Path(candidate.anchor).joinpath(*cleaned)
    return Path(*cleaned)


class RawStore:
    """负责把原始响应写成 JSON 文件，不读取数据库。"""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def save(
        self,
        *,
        source: str,
        external_id: str,
        payload: dict[str, Any],
        fetched_at: datetime,
    ) -> str:
        """原子写入一份原始响应，返回文件路径；失败抛 :class:`StoreError`。"""

        directory = self.root / source / fetched_at.strftime("%Y-%m-%d")
        path = directory / f"{safe_external_id(external_id)}.json"
        temp_path = path.with_name(f"{path.name}.tmp")
        try:
            directory.mkdir(parents=True, exist_ok=True)
            temp_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temp_path.replace(path)
        except OSError as exc:
            raise StoreError(f"原始响应写入失败 path={path} error={exc}") from exc
        return str(path)
