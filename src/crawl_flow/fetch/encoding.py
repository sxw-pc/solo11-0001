"""响应体解码链：响应头 charset → 页面 meta charset → UTF-8 容错解码。

链路每一步都可解释：:class:`DecodedBody` 记录实际使用的编码与来源
（``header`` / ``meta`` / ``fallback``），随 :class:`FetchResult` 进入
raw 留档，事后能还原「这段文本是按什么编码、依据什么声明解出来的」。

设计约定：

- 前两步用**严格解码**：声明的编码解不动（声明错误、未知编码名）就落到
  下一步，而不是在错误的编码上硬解出乱码。
- 最后一步 UTF-8 用 ``errors="replace"`` 兜底：任何字节序列都能出文本，
  非法字节变为 U+FFFD，绝不抛异常。
- GB2312 是 GBK 的子集，统一按 GBK 解码（与 WHATWG 一致），避免页面里
  出现 GBK 扩展字符（如「〇」）时严格 GB2312 解码失败。
- UTF-8 一律用 ``utf-8-sig`` 解码，自动去掉可能存在的 BOM。
- 只有 HTML 响应（或完全未声明类型）才嗅探 meta charset；JSON 等接口
  响应不嗅探，避免正文里出现的 meta 字样干扰 API 源解码。
"""

from __future__ import annotations

import codecs
import re
from dataclasses import dataclass
from typing import Mapping, NamedTuple

# WHATWG 的 meta 嗅探只看文档开头，这里取前 2048 字节。
META_SNIFF_BYTES = 2048

META_CHARSET_PATTERN = re.compile(
    rb"<meta\s[^>]*?charset\s*=\s*[\"']?\s*([A-Za-z0-9_:\-]+)",
    re.IGNORECASE,
)
CHARSET_PARAM_PATTERN = re.compile(
    r"charset\s*=\s*[\"']?\s*([A-Za-z0-9_:\-]+)",
    re.IGNORECASE,
)

FALLBACK_ENCODING = "utf-8"


@dataclass(frozen=True)
class DecodedBody:
    """一次解码的结果：文本、实际使用的编码、编码来源。"""

    text: str
    encoding: str
    source: str  # "header" | "meta" | "fallback"


class _Codec(NamedTuple):
    """规范化后的编码：实际解码用的 codec 名与对外报告名。"""

    actual: str
    reported: str


def decode_body(content: bytes, headers: Mapping[str, str]) -> DecodedBody:
    """按「响应头 → 页面 meta → UTF-8 容错」的顺序解码响应体。"""

    content_type = _header_value(headers, "content-type")

    # 1. 响应头 Content-Type 的 charset
    header_codec = _resolve_codec(_charset_from_content_type(content_type))
    if header_codec is not None:
        text = _strict_decode(content, header_codec)
        if text is not None:
            return DecodedBody(text, header_codec.reported, "header")
        # 响应头声明错误：不解出乱码，继续向下看页面 meta。

    # 2. 页面 <meta> 声明的 charset（仅 HTML 响应）
    if _is_html(content_type):
        meta_codec = _resolve_codec(_charset_from_meta(content))
        if meta_codec is not None and (
            header_codec is None or meta_codec.actual != header_codec.actual
        ):
            text = _strict_decode(content, meta_codec)
            if text is not None:
                return DecodedBody(text, meta_codec.reported, "meta")

    # 3. UTF-8 容错兜底：非法字节替换为 U+FFFD，不抛异常
    return DecodedBody(
        content.decode("utf-8-sig", errors="replace"),
        FALLBACK_ENCODING,
        "fallback",
    )


def _header_value(headers: Mapping[str, str], name: str) -> str | None:
    """大小写不敏感地取响应头。"""

    for key, value in headers.items():
        if key.lower() == name:
            return value
    return None


def _charset_from_content_type(content_type: str | None) -> str | None:
    if not content_type:
        return None
    match = CHARSET_PARAM_PATTERN.search(content_type)
    return match.group(1) if match else None


def _charset_from_meta(content: bytes) -> str | None:
    """从文档开头的 ``<meta>`` 标签提取 charset（简化版 WHATWG 嗅探）。

    同时覆盖 ``<meta charset="gbk">`` 与
    ``<meta http-equiv="Content-Type" content="text/html; charset=gbk">``。
    """

    match = META_CHARSET_PATTERN.search(content[:META_SNIFF_BYTES])
    if match is None:
        return None
    return match.group(1).decode("ascii", errors="ignore")


def _is_html(content_type: str | None) -> bool:
    if content_type is None:
        return True
    return "html" in content_type.lower()


def _resolve_codec(charset: str | None) -> _Codec | None:
    """把声明的编码名规范化为可用 codec；未知编码名返回 ``None``。"""

    if not charset:
        return None
    name = charset.strip().strip("\"'").strip().lower()
    if not name:
        return None
    try:
        codec = codecs.lookup(name)
    except LookupError:
        return None
    if codec.name in {"gb2312", "gbk"}:
        return _Codec("gbk", "gbk")
    if codec.name == "utf-8":
        # utf-8-sig 与 utf-8 的唯一差别是自动去掉 BOM，对外仍报告 utf-8。
        return _Codec("utf-8-sig", "utf-8")
    return _Codec(codec.name, codec.name)


def _strict_decode(content: bytes, codec: _Codec) -> str | None:
    """严格解码；声明与实际不符（或 codec 不可用）时返回 ``None``。"""

    try:
        return content.decode(codec.actual, errors="strict")
    except (UnicodeDecodeError, LookupError):
        return None
