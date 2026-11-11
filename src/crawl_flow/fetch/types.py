"""HTTP 抓取结果的数据结构。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FetchResult:
    """一次 GET 请求的完整结果（含失败状态码的响应）。"""

    url: str
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""
    elapsed_ms: int = 0
    proxy_used: str | None = None

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 400
