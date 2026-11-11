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


class WatchError(CrawlFlowError):
    """布控模块的可预期错误（配置非法等）。"""


class WatchRegexTimeout(WatchError):
    """正则执行超过布控引擎配置的超时时间。"""

    def __init__(self, rule_name: str, timeout_seconds: float) -> None:
        super().__init__(
            f"正则执行超时 rule={rule_name} timeout_seconds={timeout_seconds}"
        )
        self.rule_name = rule_name
        self.timeout_seconds = timeout_seconds
