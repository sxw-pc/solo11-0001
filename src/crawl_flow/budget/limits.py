"""源级每日预算上限的取值对象。"""

from __future__ import annotations

from dataclasses import dataclass

from crawl_flow.config import SourceConfig


@dataclass(frozen=True)
class BudgetLimits:
    """单个源的每日额度，``None`` 表示该维度完全不限制。"""

    source: str
    daily_request_limit: int | None
    daily_bytes_limit: int | None

    @classmethod
    def from_source(cls, source: SourceConfig) -> "BudgetLimits":
        return cls(
            source=source.name,
            daily_request_limit=source.daily_request_limit,
            daily_bytes_limit=source.daily_bytes_limit,
        )

    @property
    def unlimited(self) -> bool:
        """两个维度都没配上限时，账本只计数、永不熔断。"""

        return self.daily_request_limit is None and self.daily_bytes_limit is None
