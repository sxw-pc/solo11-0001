"""礼貌 HTTP 客户端：在现有 :class:`Fetcher` 外包一层共享限速与体积保护。

不替换管道自身的 HTTP 栈，只在自动爬取链路上加约束：

- 每个请求前后经过全局 :class:`RateLimiter`，默认每 2 秒至多 1 次；
- 调用方带优先级，适配校验以低优先级入场，不抢占正常采集；
- 响应体超过 ``MAX_HTML_BYTES`` 直接拒绝，防御超大页面。
"""

from __future__ import annotations

import logging

from crawl_flow.autogen.constants import MAX_HTML_BYTES
from crawl_flow.autogen.errors import GenerationError
from crawl_flow.autogen.limiter import PRIORITY_CRAWL, RateLimiter, get_global_limiter
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult

LOGGER_NAME = "crawl_flow.autogen.polite"


class PoliteFetcher:
    """带共享限速、优先级与体积上限的 GET 客户端。"""

    def __init__(
        self,
        *,
        fetcher: Fetcher | None = None,
        limiter: RateLimiter | None = None,
        max_bytes: int = MAX_HTML_BYTES,
        user_agent_suffix: str = "autogen",
    ) -> None:
        self._owns_fetcher = fetcher is None
        base_ua = f"crawl-flow-autogen/0.1 (+polite; {user_agent_suffix})"
        self._fetcher = fetcher or Fetcher(user_agent=base_ua)
        self._limiter = limiter or get_global_limiter()
        self._max_bytes = max_bytes
        self._log = logging.getLogger(LOGGER_NAME)

    def get(self, url: str, *, priority: int = PRIORITY_CRAWL) -> FetchResult:
        self._limiter.acquire(priority=priority)
        try:
            result = self._fetcher.get(url)
        finally:
            self._limiter.release()
        if len(result.body.encode("utf-8", errors="ignore")) > self._max_bytes:
            raise GenerationError(
                f"响应体超过体积上限 {self._max_bytes} 字节",
                stage="fetch",
                detail=url,
            )
        self._log.debug("polite get url=%s status=%s priority=%s", url, result.status_code, priority)
        return result

    def close(self) -> None:
        if self._owns_fetcher:
            self._fetcher.close()

    def __enter__(self) -> "PoliteFetcher":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
