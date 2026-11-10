"""游标规则：决定「游标记什么」以及「条目是否已完成」。

两种内置规则都是对「按列表顺序枚举到的条目」的断点表达：

- :class:`CompletedIdsRule`（默认）：记录已完成条目的 ``external_id`` 集合，
  按 ID 跳过。列表条目增删不影响已完成 ID 的判定，语义最稳，适合长期周期采集。
- :class:`ListPositionRule`：记录已枚举的条目总数（列表位置偏移），按位置跳过。
  只依赖位置、不依赖 ID；列表顺序发生变化时位置语义会漂移，适合顺序稳定的源。

规则只负责内存状态与 JSON 载荷之间的转换，不触碰文件系统，持久化由
:class:`~crawl_flow.cursor.store.CursorStore` 统一完成（便于替换存储后端）。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

from crawl_flow.errors import CursorError

# 规则名 -> 规则类，配置 ``cursor.rule`` 的取值注册表。
RULE_REGISTRY: dict[str, type["CursorRule"]] = {}


@dataclass(frozen=True)
class ItemCursorContext:
    """条目在当前列表枚举中的游标上下文。

    ``position`` 是该条目在「本次全量列表枚举」中的 0 基序号，跨页连续计数，
    与翻页无关；``external_id`` 是条目的源内唯一 ID。
    """

    external_id: str
    position: int


@dataclass
class CursorState:
    """游标运行时状态基类，具体字段由子类规则定义。"""

    rule: str

    @abstractmethod
    def to_payload(self) -> dict[str, Any]:
        """序列化为可 JSON 化的游标载荷（不含 ``rule`` 信封）。"""


class CursorRule(ABC):
    """游标规则接口：初始化、跳过判定、条目完成推进、序列化/反序列化。"""

    name: str = ""

    def __init__(self, state: CursorState) -> None:
        self.state = state

    # ---- 子类必须实现 ----
    @classmethod
    @abstractmethod
    def initial_state(cls) -> CursorState:
        """首次运行（无游标文件）时的初始状态。"""

    @classmethod
    @abstractmethod
    def load_state(cls, payload: dict[str, Any]) -> CursorState:
        """从游标文件载荷恢复状态；载荷非法时抛 :class:`CursorError`。"""

    @abstractmethod
    def is_completed(self, context: ItemCursorContext) -> bool:
        """判断条目是否已被历史运行完成（应跳过、不再发起详情请求）。"""

    @abstractmethod
    def mark_completed(self, context: ItemCursorContext) -> None:
        """条目全流程处理完成后推进游标（仅改内存，落盘由管理器负责）。"""

    @abstractmethod
    def completed_count(self) -> int:
        """游标记录的已完成条目数量（用于日志与跳过数量核对）。"""

    @classmethod
    def register(cls) -> None:
        if cls.name:
            RULE_REGISTRY[cls.name] = cls


@dataclass
class CompletedIdsState(CursorState):
    """``completed_ids`` 规则的状态：已完成条目的 ID 集合。"""

    completed_ids: set[str] = field(default_factory=set)

    def to_payload(self) -> dict[str, Any]:
        # 排序后落盘，保证同一状态字节稳定，便于 diff 与排查。
        return {"completed_ids": sorted(self.completed_ids)}


class CompletedIdsRule(CursorRule):
    """按 ``external_id`` 记录断点：完成一个记一个，重启按 ID 跳过。"""

    name = "completed_ids"

    @classmethod
    def initial_state(cls) -> CompletedIdsState:
        return CompletedIdsState(rule=cls.name)

    @classmethod
    def load_state(cls, payload: dict[str, Any]) -> CompletedIdsState:
        raw = payload.get("completed_ids")
        if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
            raise CursorError("游标载荷 completed_ids 必须是字符串数组")
        return CompletedIdsState(rule=cls.name, completed_ids=set(raw))

    def is_completed(self, context: ItemCursorContext) -> bool:
        return context.external_id in self.state.completed_ids

    def mark_completed(self, context: ItemCursorContext) -> None:
        self.state.completed_ids.add(context.external_id)

    def completed_count(self) -> int:
        return len(self.state.completed_ids)


@dataclass
class ListPositionState(CursorState):
    """``list_position`` 规则的状态：已完成的列表枚举条目数（位置偏移）。"""

    completed_count: int = 0

    def to_payload(self) -> dict[str, Any]:
        return {"completed_count": self.completed_count}


class ListPositionRule(CursorRule):
    """按列表位置记录断点：前 ``completed_count`` 个枚举条目视为已完成。"""

    name = "list_position"

    @classmethod
    def initial_state(cls) -> ListPositionState:
        return ListPositionState(rule=cls.name)

    @classmethod
    def load_state(cls, payload: dict[str, Any]) -> ListPositionState:
        raw = payload.get("completed_count")
        # bool 是 int 的子类，显式排除，避免 true/false 被当成 1/0。
        if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
            raise CursorError("游标载荷 completed_count 必须是非负整数")
        return ListPositionState(rule=cls.name, completed_count=raw)

    def is_completed(self, context: ItemCursorContext) -> bool:
        return context.position < self.state.completed_count

    def mark_completed(self, context: ItemCursorContext) -> None:
        # 条目按列表顺序串行处理，完成位置即枚举偏移；取 max 防御乱序调用。
        self.state.completed_count = max(
            self.state.completed_count, context.position + 1
        )

    def completed_count(self) -> int:
        return self.state.completed_count


CompletedIdsRule.register()
ListPositionRule.register()


def build_rule(rule_name: str, payload: dict[str, Any] | None = None) -> CursorRule:
    """按规则名构造规则实例。

    ``payload`` 为 ``None`` 时返回初始游标（首次运行）；否则从载荷恢复。
    未知规则名或载荷与规则不匹配时抛 :class:`CursorError`。
    """

    rule_cls = RULE_REGISTRY.get(rule_name)
    if rule_cls is None:
        available = ", ".join(sorted(RULE_REGISTRY)) or "<无已注册规则>"
        raise CursorError(f"未知的游标规则: {rule_name}，可用规则: {available}")
    state = (
        rule_cls.initial_state()
        if payload is None
        else rule_cls.load_state(payload)
    )
    return rule_cls(state)
