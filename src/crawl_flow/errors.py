"""crawl-flow 的异常体系。

所有业务异常都继承 :class:`CrawlFlowError`，调用方可以按层捕获，
避免在管道代码里散落 ``except Exception``。
"""

from __future__ import annotations


class CrawlFlowError(Exception):
    """crawl-flow 所有可预期异常的基类。"""


class ConfigError(CrawlFlowError):
    """配置缺失、格式错误或校验失败。"""


class FetchError(CrawlFlowError):
    """网络请求失败：连接异常、超时、非预期 HTTP 状态。"""

    def __init__(self, message: str, *, url: str = "", status_code: int | None = None) -> None:
        super().__init__(message)
        self.url = url
        self.status_code = status_code


class ParseError(CrawlFlowError):
    """响应内容不符合适配器预期。"""

    def __init__(self, source: str, external_id: str, reason: str) -> None:
        message = f"source={source} external_id={external_id} reason={reason}"
        super().__init__(message)
        self.source = source
        self.external_id = external_id
        self.reason = reason


class StoreError(CrawlFlowError):
    """原始响应落盘或数据库写入失败。"""


class BudgetError(CrawlFlowError):
    """请求预算账本相关异常的基类。"""


class BudgetExceeded(BudgetError):
    """源当日预算额度触顶，触发熔断：该源停止后续抓取（不影响其他源）。"""

    def __init__(
        self,
        source: str,
        *,
        kind: str,
        limit: int,
        used: int,
        day: str,
    ) -> None:
        self.source = source
        self.kind = kind  # "requests" 或 "bytes"
        self.limit = limit
        self.used = used
        self.day = day
        label = "请求次数" if kind == "requests" else "响应字节"
        super().__init__(
            f"源 {source} 当日{label}额度触顶: day={day} 已用={used} 上限={limit}，"
            f"该源后续抓取已熔断"
        )


class BudgetLedgerError(BudgetError):
    """预算账本损坏或读写失败；为避免静默丢账，拒绝继续运行。"""
