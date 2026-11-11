"""httpx 封装：统一超时、User-Agent、直连重试与代理池自动切换。

- 未配置代理池时完全沿用历史直连逻辑：传输异常做固定次数立即重试，
  HTTP 4xx/5xx 不重试，状态码交由调用方判断；
- 配置代理池后，每个代理只尝试一次，遇到传输异常或可重试状态码
  （403、5xx）切换下一个可用代理，同一条请求最多尝试
  :data:`MAX_PROXY_ATTEMPTS` 个不同代理，全部失败才抛 :class:`FetchError`。
"""

from __future__ import annotations

import logging
import time

import httpx

from crawl_flow.errors import FetchError
from crawl_flow.fetch.proxy_pool import ProxyPool
from crawl_flow.fetch.types import FetchResult

DEFAULT_USER_AGENT = "crawl-flow/0.1 (+https://example.invalid/crawl-flow)"
DEFAULT_TIMEOUT_SECONDS = 10.0

# 同一条详情请求最多尝试的不同代理数量。
MAX_PROXY_ATTEMPTS = 3

LOGGER_NAME = "crawl_flow.fetch"
logger = logging.getLogger(LOGGER_NAME)


def is_retryable_status(status_code: int) -> bool:
    """403 与 5xx 视为代理侧可切换重试的响应。"""

    return status_code == 403 or 500 <= status_code < 600


class Fetcher:
    """轻量 HTTP 客户端，可选挂载 :class:`ProxyPool`。

    直连客户端只对连接级传输异常做固定次数立即重试，不处理退避，也不对
    4xx/5xx 重试；代理模式下重试由代理池轮转承担，每个代理只请求一次。
    """

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        retries: int = 1,
        user_agent: str = DEFAULT_USER_AGENT,
        proxy_pool: ProxyPool | None = None,
    ) -> None:
        self._retries = max(0, retries)
        self._timeout = timeout
        self._proxy_pool = proxy_pool
        self._headers = {"User-Agent": user_agent, "Accept": "*/*"}
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers=self._headers,
        )
        self._proxy_clients: dict[str, httpx.Client] = {}

    def get(self, url: str, params: dict[str, object] | None = None) -> FetchResult:
        """发起 GET，传输层失败抛 :class:`FetchError`，HTTP 错误码正常返回。"""

        if self._proxy_pool is None:
            return self._get_direct(url, params)
        return self._get_with_proxies(url, params)

    def _get_direct(
        self, url: str, params: dict[str, object] | None
    ) -> FetchResult:
        """直连路径：保持代理功能上线前的重试与返回语义。"""

        last_error: Exception | None = None
        for attempt in range(self._retries + 1):
            started = time.perf_counter()
            try:
                response = self._client.get(url, params=params)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt < self._retries:
                    continue
                raise FetchError(
                    f"请求失败 url={url} error={exc.__class__.__name__}: {exc}",
                    url=url,
                ) from exc
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return FetchResult(
                url=str(response.url),
                status_code=response.status_code,
                headers=dict(response.headers),
                body=response.text,
                elapsed_ms=elapsed_ms,
            )
        raise FetchError(f"请求失败 url={url} error={last_error}", url=url)

    def _get_with_proxies(
        self, url: str, params: dict[str, object] | None
    ) -> FetchResult:
        """代理路径：按代理池顺序轮转，失败自动切换，全部失败抛 FetchError。"""

        pool = self._proxy_pool
        assert pool is not None
        candidates = pool.acquire_order()[:MAX_PROXY_ATTEMPTS]
        last_exc: Exception | None = None
        last_status: int | None = None

        for proxy_url in candidates:
            started = time.perf_counter()
            try:
                response = self._client_for(proxy_url).get(url, params=params)
            except (httpx.HTTPError, RuntimeError) as exc:
                # RuntimeError：代理协议缺少底层支持（如未安装 socksio）等
                # 客户端构建/传输问题，统一收敛进 FetchError 体系。
                pool.report_failure(proxy_url)
                last_exc = exc
                last_status = None
                logger.warning(
                    "代理请求失败，切换下一个代理 url=%s proxy=%s error=%s: %s",
                    url,
                    proxy_url,
                    exc.__class__.__name__,
                    exc,
                )
                continue

            elapsed_ms = int((time.perf_counter() - started) * 1000)
            if is_retryable_status(response.status_code):
                pool.report_failure(proxy_url)
                last_exc = None
                last_status = response.status_code
                logger.warning(
                    "代理响应状态不可用，切换下一个代理 url=%s proxy=%s status=%d",
                    url,
                    proxy_url,
                    response.status_code,
                )
                continue

            pool.report_success(proxy_url)
            return FetchResult(
                url=str(response.url),
                status_code=response.status_code,
                headers=dict(response.headers),
                body=response.text,
                elapsed_ms=elapsed_ms,
                proxy_used=proxy_url,
            )

        tried = ",".join(candidates)
        if last_status is not None:
            raise FetchError(
                f"所有代理均请求失败 url={url} attempts={len(candidates)} "
                f"last_status={last_status} proxies=[{tried}]",
                url=url,
                status_code=last_status,
                proxy_used=candidates[-1],
            )
        assert last_exc is not None
        raise FetchError(
            f"所有代理均请求失败 url={url} attempts={len(candidates)} "
            f"error={last_exc.__class__.__name__}: {last_exc} proxies=[{tried}]",
            url=url,
            proxy_used=candidates[-1],
        ) from last_exc

    def _client_for(self, proxy_url: str) -> httpx.Client:
        client = self._proxy_clients.get(proxy_url)
        if client is None:
            client = httpx.Client(
                timeout=self._timeout,
                follow_redirects=True,
                headers=self._headers,
                proxy=proxy_url,
            )
            self._proxy_clients[proxy_url] = client
        return client

    def close(self) -> None:
        self._client.close()
        for client in self._proxy_clients.values():
            client.close()
        self._proxy_clients.clear()

    def __enter__(self) -> "Fetcher":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
