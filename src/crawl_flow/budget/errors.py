"""预算账本相关异常。"""

from __future__ import annotations

from crawl_flow.errors import CrawlFlowError


class BudgetError(CrawlFlowError):
    """预算模块可预期异常的基类。"""


class BudgetExceededError(BudgetError):
    """某源当日额度触顶，后续请求必须在发出前熔断。"""

    def __init__(
        self,
        source: str,
        reason: str,
        *,
        metric: str,
        used: int,
        limit: int,
        usage_date: str,
    ) -> None:
        super().__init__(
            f"source={source} date={usage_date} metric={metric} "
            f"used={used} limit={limit} reason={reason}"
        )
        self.source = source
        self.reason = reason
        self.metric = metric
        self.used = used
        self.limit = limit
        self.usage_date = usage_date


class BudgetCorruptError(BudgetError):
    """账本文件损坏或结构不符：宁可拒绝启动也不能静默丢账。"""
