"""HTTP 抓取结果的数据结构。"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FetchResult:
    """一次 GET 请求的完整结果（含失败状态码的响应）。

    ``content`` 是原始响应字节，留档时原样保存、不做转码；``body`` 是按
    「响应头 charset → BOM → 页面 meta → UTF-8 容错」链解码后的文本。
    ``encoding`` / ``encoding_source`` 记录实际使用的解码器与决策来源，
    便于解释一次解码为何如此。
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
