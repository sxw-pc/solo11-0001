"""httpx 封装：统一超时、User-Agent 与一次固定重试。"""

from __future__ import annotations

import time

import httpx

from crawl_flow.errors import FetchError
from crawl_flow.fetch.types import FetchResult

DEFAULT_USER_AGENT = "crawl-flow/0.1 (+https://example.invalid/crawl-flow)"
DEFAULT_TIMEOUT_SECONDS = 10.0


class Fetcher:
    """轻量 HTTP 客户端。

    只对连接级传输异常做一次立即重试，不处理退避，也不对 4xx/5xx 重试；
    HTTP 状态码由调用方判断（列表页 5xx 让整轮失败，详情页 5xx 记失败）。
    """

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        retries: int = 1,
        user_agent: str = DEFAULT_USER_AGENT,
    ) -> None:
        self._retries = max(0, retries)
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": user_agent, "Accept": "*/*"},
        )

    def get(self, url: str, params: dict[str, object] | None = None) -> FetchResult:
        """发起 GET，传输层失败抛 :class:`FetchError`，HTTP 错误码正常返回。"""

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

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "Fetcher":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
