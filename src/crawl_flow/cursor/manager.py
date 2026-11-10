"""游标管理器：管道侧的游标协作者。

职责边界（刻意保持窄接口，避免侵入采集主流程）：

- ``start()``：加载/初始化游标。游标文件损坏、规则与配置不一致等任何问题
  都在本模块内部消化为 WARNING + 降级（``active=False``），管道随即回退到
  全量采集，**不向上抛异常、不影响任务与其他源**。
- ``should_skip()``：条目枚举到时判断是否已完成；降级或 dry-run 时恒为 False。
- ``mark_processed()``：条目**全流程处理完成后**由管道调用，推进并原子落盘；
  落盘失败同样降级而不是中断采集。
- ``reset()``：手动清空游标，回到全量采集模式。

``dry_run`` 下游标完全旁路：不读、不写、不跳过，保证演练不产生任何副作用。
"""

from __future__ import annotations

import logging
from typing import Any

from crawl_flow.cursor.rules import (
    CursorRule,
    ItemCursorContext,
    build_rule,
)
from crawl_flow.cursor.store import CursorStore
from crawl_flow.errors import CursorError

LOGGER_NAME = "crawl_flow.cursor"
CURSOR_VERSION = 1


class CursorManager:
    """单个采集源的游标生命周期管理；一源一个实例，互不共享状态。"""

    def __init__(
        self,
        *,
        source_name: str,
        rule_name: str,
        store: CursorStore,
        logger: logging.Logger | None = None,
        dry_run: bool = False,
    ) -> None:
        self.source_name = source_name
        self.rule_name = rule_name
        self.store = store
        self.log = logger or logging.getLogger(LOGGER_NAME)
        self.dry_run = dry_run
        self._rule: CursorRule | None = None
        self._started = False
        self.active = False
        """游标是否生效；False 时管道按全量模式运行。"""
        self.degraded = False
        """是否因游标异常降级到全量模式。"""

    # ---- 生命周期 ----
    def start(self) -> None:
        """加载已有游标或首次初始化；任何游标异常都降级而非抛出。"""

        if self._started:
            return
        self._started = True

        if self.dry_run:
            self.log.info(
                "cursor bypass source=%s reason=dry_run", self.source_name
            )
            return

        try:
            payload = self.store.load(self.source_name)
            if payload is None:
                # 首次运行：先在内存初始化，游标文件延迟到首个条目完成时落盘，
                # 这样空列表场景不会留下任何游标文件（游标不会空转推进）。
                self._rule = build_rule(self.rule_name)
                self.active = True
                self.log.info(
                    "cursor init source=%s rule=%s",
                    self.source_name,
                    self.rule_name,
                )
                return
            self._rule = self._restore(payload)
            self.active = True
            self.log.info(
                "cursor loaded source=%s rule=%s completed=%d",
                self.source_name,
                self._rule.name,
                self.completed_count,
            )
        except CursorError as exc:
            self._degrade(str(exc))

    def reset(self) -> bool:
        """删除游标文件，清空断点；返回是否删除了已有文件。"""

        removed = self.store.reset(self.source_name)
        self._rule = None
        self.active = False
        self.degraded = False
        self._started = False
        return removed

    # ---- 管道调用 ----
    def should_skip(self, external_id: str, position: int) -> bool:
        """条目是否已在历史运行中完成（完成则跳过，不再发起详情请求）。"""

        if not self.active or self._rule is None:
            return False
        context = ItemCursorContext(external_id=external_id, position=position)
        return self._rule.is_completed(context)

    def mark_processed(self, external_id: str, position: int) -> None:
        """条目全流程处理完成后推进游标并原子落盘。

        仅更新内存与本地文件；任何失败都降级为全量模式，不向管道抛异常。
        """

        if not self.active or self._rule is None or self.dry_run:
            return
        context = ItemCursorContext(external_id=external_id, position=position)
        try:
            self._rule.mark_completed(context)
            self.store.save(
                self.source_name,
                self._rule.name,
                self._rule.state.to_payload(),
            )
        except CursorError as exc:
            self._degrade(str(exc))

    @property
    def completed_count(self) -> int:
        """游标当前记录的已完成条目数量。"""

        if self._rule is None:
            return 0
        return self._rule.completed_count()

    # ---- 内部 ----
    def _restore(self, payload: dict[str, Any]) -> CursorRule:
        """校验游标信封并恢复规则；结构损坏抛 CursorError，规则变更自动重建。"""

        if not isinstance(payload.get("state"), dict):
            raise CursorError("游标文件损坏：缺少 state 对象")
        stored_rule = payload.get("rule")
        if not isinstance(stored_rule, str) or not stored_rule:
            raise CursorError("游标文件损坏：缺少 rule 字段")
        version = payload.get("version")
        if not isinstance(version, int) or isinstance(version, bool):
            raise CursorError("游标文件损坏：缺少合法 version 字段")
        if version > CURSOR_VERSION:
            raise CursorError(
                f"游标文件版本不受支持 version={version} supported<={CURSOR_VERSION}"
            )

        if stored_rule != self.rule_name:
            # 配置中的规则被人为调整：旧游标语义不再适用，告警后自动重建，
            # 本轮等价于一次全量采集，且后续运行立即恢复增量能力。
            self.log.warning(
                "cursor rule changed source=%s old_rule=%s new_rule=%s "
                "action=reinitialize mode=full_crawl",
                self.source_name,
                stored_rule,
                self.rule_name,
            )
            return build_rule(self.rule_name)

        return build_rule(stored_rule, payload["state"])

    def _degrade(self, reason: str) -> None:
        self.active = False
        self.degraded = True
        self._rule = None
        self.log.warning(
            "cursor degraded source=%s reason=%s action=full_crawl",
            self.source_name,
            reason,
        )
