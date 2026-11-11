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
FALLBACK_NAME = "unknown"


def safe_external_id(external_id: str) -> str:
    """把 external_id 转成安全的文件名片段。

    只保留 ``[A-Za-z0-9._-]``，其余字符（含路径分隔符、空字节）折叠为
    ``_``；去掉首尾的 ``.`` / ``_``，结果为空（例如 ``..``）时回退为
    ``unknown``，并截断到 120 字符。返回值一定不含路径分隔符，也不会
    是 ``.`` / ``..``，可以安全地作为单个文件名片段使用。
    """

    candidate = SAFE_NAME.sub("_", external_id).strip("._")
    return (candidate or FALLBACK_NAME)[:MAX_ID_LENGTH]


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
        """原子写入一份原始响应，返回文件路径；失败抛 :class:`StoreError`。

        目录不存在时逐级创建；写入走「临时文件 + 原子替换」，重复采集覆盖
        同名文件时不会出现半读取状态；任何失败都会清理临时文件，不在目录
        中留下写了一半的残留。
        """

        directory = self.root / source / fetched_at.strftime("%Y-%m-%d")
        path = directory / f"{safe_external_id(external_id)}.json"
        self._ensure_contained(path)
        temp_path = path.with_name(f"{path.name}.tmp")
        try:
            directory.mkdir(parents=True, exist_ok=True)
            temp_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temp_path.replace(path)
        except OSError as exc:
            self._cleanup_temp(temp_path)
            raise StoreError(f"原始响应写入失败 path={path} error={exc}") from exc
        return str(path)

    def _ensure_contained(self, path: Path) -> None:
        """兜底校验：最终路径必须位于 raw 根目录内，杜绝路径跳转越权。"""

        try:
            contained = path.resolve().is_relative_to(self.root.resolve())
        except (OSError, RuntimeError) as exc:
            raise StoreError(f"raw 路径解析失败 path={path} error={exc}") from exc
        if not contained:
            raise StoreError(f"raw 路径越界 path={path} root={self.root}")

    @staticmethod
    def _cleanup_temp(temp_path: Path) -> None:
        """尽力删除残留临时文件；清理失败不掩盖原始异常。"""

        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
