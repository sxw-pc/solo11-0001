"""httpx 封装：统一超时、User-Agent/请求头与一次固定重试。"""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

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
        headers: Mapping[str, str] | None = None,
    ) -> None:
        self._retries = max(0, retries)
        merged_headers: dict[str, str] = {
            "User-Agent": user_agent,
            "Accept": "*/*",
        }
        if headers:
            # 源配置里的请求头覆盖默认值；UA 也可以在这里显式覆盖。
            merged_headers.update({str(key): str(value) for key, value in headers.items()})
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers=merged_headers,
        )

    def get(
        self,
        url: str,
        params: dict[str, object] | None = None,
        *,
        timeout: float | None = None,
    ) -> FetchResult:
        """发起 GET，传输层失败抛 :class:`FetchError`，HTTP 错误码正常返回。

        ``timeout`` 为单请求覆盖值；不传时沿用客户端超时。探测会传入源超时的
        一半，但仍然复用本客户端、UA、请求头与同一套重试规则。
        """

        last_error: Exception | None = None
        for attempt in range(self._retries + 1):
            started = time.perf_counter()
            request_kwargs: dict[str, Any] = {}
            if params is not None:
                request_kwargs["params"] = params
            if timeout is not None:
                request_kwargs["timeout"] = timeout
            try:
                response = self._client.get(url, **request_kwargs)
            except httpx.HTTPError as exc:
                last_error = exc
                if attempt < self._retries:
                    continue
                raise FetchError(
                    f"请求失败 url={url} error={exc.__class__.__name__}: {exc}",
                    url=url,
                    timed_out=isinstance(exc, httpx.TimeoutException),
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
