"""原始响应落盘：``data/raw/{source}/{YYYY-MM-DD}/{external_id}.json``。"""

from __future__ import annotations

import json
import re
import threading
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


class RawStore:
    """负责把原始响应写成 JSON 文件，不读取数据库。"""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        # 并发采集时由 ThreadSafeRawStore 复用本锁，串行化目录创建与写入。
        # 用 RLock：包装层与本方法可能在同一线程内重入获取。
        self.lock = threading.RLock()

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
            with self.lock:
                directory.mkdir(parents=True, exist_ok=True)
                temp_path.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
                temp_path.replace(path)
        except OSError as exc:
            raise StoreError(f"原始响应写入失败 path={path} error={exc}") from exc
        return str(path)
