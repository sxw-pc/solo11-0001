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


class SyncError(CrawlFlowError):
    """外部存储目标投递失败：连接失败、超时、权限不足、网络中断等。

    该异常只在同步层内部使用，永远不会传播到采集主流程。
    """
