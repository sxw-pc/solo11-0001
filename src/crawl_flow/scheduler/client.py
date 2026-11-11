"""调度版 Fetcher：与 :class:`crawl_flow.fetch.client.Fetcher` 同接口的代理。

接入方式对采集组件透明——适配器只依赖 ``get()`` / ``close()`` / 上下文
管理器协议，因此把 ``Fetcher`` 换成 ``ScheduledFetcher`` 即可获得并发调度
与限流能力，适配器与管道核心逻辑不需要任何修改。

降级语义：调度器拒绝接单（已降级/关闭）或排队期间触发降级时，自动改为
直接调用底层 :class:`Fetcher` 串行执行，采集任务不中断、不丢请求。
"""

from __future__ import annotations

import logging
from typing import Any

from crawl_flow.errors import SchedulerError
from crawl_flow.fetch.types import FetchResult
from crawl_flow.scheduler.pool import PendingRequest, RequestScheduler

LOGGER_NAME = "crawl_flow.scheduler.client"


class ScheduledFetcher:
    """把 GET 请求转交给 :class:`RequestScheduler` 的可插拔代理。

    每个采集源持有一个实例（``source`` 用于源级并发与配额统计），多个源
    可以共享同一个调度器，从而统一受全局并发、全局 QPS 与域名规则约束。
    """

    def __init__(
        self,
        source: str,
        scheduler: RequestScheduler,
        direct_fetcher: Any,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.source = source
        self._scheduler = scheduler
        self._direct = direct_fetcher
        self._log = logger or logging.getLogger(LOGGER_NAME)
        self._closed = False
        scheduler.register_source(source)

    def get(
        self, url: str, params: dict[str, object] | None = None
    ) -> FetchResult:
        """经调度池发起 GET；任何调度器故障都降级为直连。"""

        try:
            request = self._submit(url, params)
        except SchedulerError:
            return self._fallback(url, params)

        try:
            return self._scheduler.execute(request)
        except SchedulerError:
            self._log.warning(
                "scheduler unavailable, fallback to direct fetch source=%s url=%s",
                self.source,
                url,
            )
            return self._fallback(url, params)

    def _submit(
        self,
        url: str,
        params: dict[str, object] | None,
    ) -> PendingRequest:
        return self._scheduler.submit(self.source, url, params)

    def _fallback(
        self,
        url: str,
        params: dict[str, object] | None,
    ) -> FetchResult:
        """降级路径：绕过调度器，直接串行请求，语义与原 Fetcher 完全一致。"""

        return self._direct.get(url, params)

    def close(self) -> None:
        # 调度器与底层连接由外层（CLI/调用方）统一管理，代理本身不持有资源。
        self._closed = True

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> "ScheduledFetcher":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
