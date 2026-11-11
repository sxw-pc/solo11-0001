"""httpx 封装：统一超时、User-Agent 与一次固定重试。"""

from __future__ import annotations

import time
from typing import Protocol

import httpx

from crawl_flow.errors import FetchError
from crawl_flow.fetch.types import FetchResult

DEFAULT_USER_AGENT = "crawl-flow/0.1 (+https://example.invalid/crawl-flow)"
DEFAULT_TIMEOUT_SECONDS = 10.0


class RequestMeter(Protocol):
    """记账器协议：Fetcher 只依赖这两个钩子，不依赖预算模块具体实现。"""

    def on_request(self) -> None:
        """每个 HTTP 请求发出前调用；额度触顶时抛 BudgetExceededError。"""; ...

    def on_response(self, byte_count: int) -> None:
        """每个 HTTP 响应完整接收后调用，按实际响应字节记账。"""; ...


class Fetcher:
    """轻量 HTTP 客户端。

    只对连接级传输异常做一次立即重试，不处理退避，也不对 4xx/5xx 重试；
    HTTP 状态码由调用方判断（列表页 5xx 让整轮失败，详情页 5xx 记失败）。

    可选注入 :class:`RequestMeter`：通过 httpx 事件钩子保证**每一个真实
    发出的请求**（含传输重试与 3xx 重定向的每一跳）都先预留额度，每一个
    实际接收的响应体都按字节记账。
    """

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        retries: int = 1,
        user_agent: str = DEFAULT_USER_AGENT,
        meter: RequestMeter | None = None,
    ) -> None:
        self._retries = max(0, retries)
        self._meter = meter
        event_hooks: dict[str, list[object]] = {}
        if meter is not None:
            event_hooks = {
                # request 钩子在请求真正发出前触发；抛异常会直接中断本次调用，
                # 熔断的那次请求不会离开本机。
                "request": [self._on_request_hook],
                # response 钩子在响应头/体读取完成后触发（重定向的每一跳也触发）。
                "response": [self._on_response_hook],
            }
        self._client = httpx.Client(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": user_agent, "Accept": "*/*"},
            event_hooks=event_hooks,
        )

    def _on_request_hook(self, request: httpx.Request) -> None:
        # 钩子签名由 httpx 决定，request 本身不参与记账，记账器按当前绑定源记账。
        self._meter.on_request()  # type: ignore[union-attr]

    def _on_response_hook(self, response: httpx.Response) -> None:
        # 先读完流式响应体；response.content 随后返回解码后的原始字节，
        # 与后续 response.text 共享同一份缓存，不会重复消费响应体。
        response.read()
        self._meter.on_response(len(response.content))  # type: ignore[union-attr]

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
