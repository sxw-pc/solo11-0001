"""游标持久化：每个采集源一个 JSON 文件，临时文件 + 原子替换。

设计要点：

- **无外部服务**：纯本地文件，默认落在 ``data/cursors/{source}.json``。
- **原子写入**：先写同目录临时文件并 ``flush + fsync``，再 ``os.replace``
  原子替换，任何时刻读到的都是完整文件，不会写出半截 JSON。
- **单源隔离**：一源一文件，某个文件损坏只影响对应源，其余源不受牵连。
- **不吞异常**：读/写问题统一抛 :class:`~crawl_flow.errors.CursorError`，
  由 :class:`~crawl_flow.cursor.manager.CursorManager` 决定降级策略。
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any

from crawl_flow.errors import CursorError

# 与源名校验保持同一字符集，文件名天然安全；额外防御性再清洗一次。
SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")
CURSOR_FILE_SUFFIX = ".json"
CURSOR_VERSION = 1


class CursorStore:
    """按源名读写游标文件的本地存储，不理解游标语义。"""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def path_for(self, source_name: str) -> Path:
        """返回某源的游标文件路径（不要求文件已存在）。"""

        safe_name = SAFE_NAME.sub("_", source_name)
        return self.root / f"{safe_name}{CURSOR_FILE_SUFFIX}"

    def exists(self, source_name: str) -> bool:
        return self.path_for(source_name).is_file()

    def load(self, source_name: str) -> dict[str, Any] | None:
        """读取游标载荷；文件不存在返回 ``None``（首次运行语义）。

        文件不可读、JSON 非法或顶层不是对象时抛 :class:`CursorError`，
        交由上层降级为全量采集。
        """

        path = self.path_for(source_name)
        if not path.is_file():
            return None
        try:
            raw = path.read_text(encoding="utf-8")
        except OSError as exc:
            raise CursorError(f"游标文件读取失败 source={source_name} error={exc}") from exc
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CursorError(
                f"游标文件损坏（JSON 解析失败）source={source_name} error={exc.msg}"
            ) from exc
        if not isinstance(payload, dict):
            raise CursorError(f"游标文件损坏（顶层不是 JSON 对象）source={source_name}")
        return payload

    def save(self, source_name: str, rule: str, state_payload: dict[str, Any]) -> Path:
        """原子写入游标文件，返回落盘路径；失败抛 :class:`CursorError`。"""

        path = self.path_for(source_name)
        envelope = {
            "version": CURSOR_VERSION,
            "source": source_name,
            "rule": rule,
            "state": state_payload,
        }
        temp_path = path.with_name(f"{path.name}.tmp")
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            with open(temp_path, "w", encoding="utf-8") as handle:
                json.dump(envelope, handle, ensure_ascii=False, indent=2, sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
            # fsync 目录，保证替换条目本身落盘，崩溃后目录项不丢失。
            self._fsync_directory(path.parent)
        except OSError as exc:
            raise CursorError(f"游标文件写入失败 source={source_name} error={exc}") from exc
        return path

    def reset(self, source_name: str) -> bool:
        """删除某源的游标文件；文件不存在返回 ``False``，删除失败抛 CursorError。"""

        path = self.path_for(source_name)
        if not path.is_file():
            return False
        try:
            path.unlink()
        except OSError as exc:
            raise CursorError(f"游标文件删除失败 source={source_name} error={exc}") from exc
        return True

    def list_sources(self) -> list[str]:
        """列出当前游标目录中已持久化游标的源名（按名排序）。"""

        if not self.root.is_dir():
            return []
        names = [
            path.stem
            for path in self.root.glob(f"*{CURSOR_FILE_SUFFIX}")
            if path.is_file()
        ]
        return sorted(names)

    @staticmethod
    def _fsync_directory(directory: Path) -> None:
        # Windows 不支持对目录句柄 fsync，忽略对应错误即可，文件替换本身已原子。
        if not hasattr(os, "O_DIRECTORY"):
            return
        try:
            dir_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        except OSError:
            return
        try:
            os.fsync(dir_fd)
        except OSError:
            pass
        finally:
            os.close(dir_fd)
