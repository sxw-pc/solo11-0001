"""HTTP 抓取结果的数据结构。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FetchResult:
    """一次 GET 请求的完整结果（含失败状态码的响应）。

    ``content`` 是原始响应字节（留档用，不转码）；``body`` 是按
    :func:`crawl_flow.fetch.encoding.decode_body` 解码出的统一 Unicode
    文本，``encoding`` / ``encoding_source`` 记录解码依据。
    """

    url: str
    status_code: int
    headers: dict[str, str] = field(default_factory=dict)
    body: str = ""
    content: bytes = b""
    encoding: str = ""
    encoding_source: str = ""
    elapsed_ms: int = 0

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 400

    @property
    def raw_content(self) -> bytes:
        """原始响应字节；手工构造的结果缺 ``content`` 时由 ``body`` 反推 UTF-8。"""

        if self.content or not self.body:
            return self.content
        return self.body.encode("utf-8")
