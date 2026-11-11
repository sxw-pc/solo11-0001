"""HTTP 抓取结果的数据结构。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True)
class FetchResult:
    """一次 GET 请求的完整结果（含失败状态码的响应）。"""

    url: str
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""
    elapsed_ms: int = 0

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 400


class FetchObserver(Protocol):
    """抓取观测钩子：预算记账等横切能力挂在这里。

    每个真实发出的 HTTP 请求（含重试）都会触发一次 ``before_request``；
    每收到一份响应（含 4xx/5xx）触发一次 ``after_response``。
    ``before_request`` 抛出的异常会原样向上传播（用于预算熔断）。
    """

    def before_request(self) -> None:
        """请求即将发出；抛出异常可阻止本次请求。"""
        ...

    def after_response(self, body_bytes: int) -> None:
        """响应已接收；``body_bytes`` 为解码后响应体的字节数。"""
        ...
