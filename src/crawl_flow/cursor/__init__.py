"""增量断点（游标）模块：规则 + 原子持久化 + 管理器。

模块对外暴露：

- :class:`CursorRule` / :class:`ItemCursorContext` / :class:`CursorState`：游标规则抽象。
- :class:`CompletedIdsRule` / :class:`ListPositionRule`：两种内置规则。
- :class:`CursorStore`：按源一个 JSON 文件、临时文件 + 原子替换的持久化。
- :class:`CursorManager`：管道侧的游标协作者，损坏自动降级、单源隔离。
- :class:`CursorError`：游标自身的异常体系。
"""

from __future__ import annotations

from crawl_flow.cursor.manager import CursorManager
from crawl_flow.cursor.rules import (
    CompletedIdsRule,
    CursorRule,
    CursorState,
    ItemCursorContext,
    ListPositionRule,
    build_rule,
)
from crawl_flow.cursor.store import CursorStore
from crawl_flow.errors import CursorError

__all__ = [
    "CompletedIdsRule",
    "CursorError",
    "CursorManager",
    "CursorRule",
    "CursorState",
    "CursorStore",
    "ItemCursorContext",
    "ListPositionRule",
    "build_rule",
]
