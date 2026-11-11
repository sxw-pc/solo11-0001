"""响应体解码链：响应头 charset → BOM → 页面 meta → UTF-8 容错。

整条链只依赖原始字节与 ``Content-Type`` 头；任何一步失败（编解码器未知、
严格解码报错）都向后回退，末位 UTF-8 + ``errors="replace"`` 保证解码本身
永远不会抛异常。每一步的决策来源记录在 :class:`DecodedBody` 中，便于排查
「为什么这段中文被解成了这样」。
"""

from __future__ import annotations

import codecs
import re
from dataclasses import dataclass

META_SCAN_BYTES = 2048

_CHARSET_PARAM_RE = re.compile(
    r"charset\s*=\s*[\"']?\s*([A-Za-z0-9._:-]+)",
    re.IGNORECASE,
)
_META_CHARSET_RE = re.compile(
    rb"<meta\s[^>]*?charset\s*=\s*[\"']?\s*([A-Za-z0-9._:-]+)",
    re.IGNORECASE,
)
_HTMLISH_TYPES = ("text/html", "application/xhtml")

# 声明标签 → 实际解码器。GB2312 是 GBK 的子集，部分站点标 gb2312 却混用
# GBK 字符，统一用超集 GBK 解码，避免因个别生僻字触发回退。
_CODEC_OVERRIDES = {"gb2312": "gbk"}


@dataclass(frozen=True)
class DecodedBody:
    """一次解码的结果与决策依据。"""

    text: str
    encoding: str  # 实际使用的解码器规范名，如 utf-8 / gbk
    declared: str | None  # 响应头或 meta 声明的 charset；未声明为 None
    source: str  # 决策来源：header / bom / meta / fallback


def decode_body(content: bytes, content_type: str = "") -> DecodedBody:
    """按「响应头 → BOM → meta → UTF-8 容错」解码响应体。"""

    declared = _charset_from_header(content_type)

    # 1) 响应头 charset
    header_codec = _canonical_codec(declared)
    if header_codec is not None:
        text = _decode_strict(content, header_codec)
        if text is not None:
            return DecodedBody(text, header_codec, declared, "header")

    # 2) BOM（UTF-8 BOM 自身就是无歧义的强信号）
    if content.startswith(codecs.BOM_UTF8):
        text = _decode_strict(content, "utf-8")
        if text is not None:
            return DecodedBody(text, "utf-8", declared, "bom")

    # 3) 页面 meta（只对 HTML 类或未知类型扫描，避免误读 JSON 内容）
    if _is_htmlish(content_type):
        meta = _charset_from_meta(content)
        meta_codec = _canonical_codec(meta)
        if meta_codec is not None and meta_codec != header_codec:
            text = _decode_strict(content, meta_codec)
            if text is not None:
                return DecodedBody(text, meta_codec, meta, "meta")

    # 4) UTF-8 容错兜底：不会失败
    fallback_codec = "utf-8-sig" if content.startswith(codecs.BOM_UTF8) else "utf-8"
    text = content.decode(fallback_codec, errors="replace")
    return DecodedBody(text, "utf-8", declared, "fallback")


def _charset_from_header(content_type: str) -> str | None:
    match = _CHARSET_PARAM_RE.search(content_type or "")
    return match.group(1) if match else None


def _charset_from_meta(content: bytes) -> str | None:
    """在文档前部扫描 ``<meta charset=...>`` / ``http-equiv`` 声明。"""

    match = _META_CHARSET_RE.search(content[:META_SCAN_BYTES])
    if match is None:
        return None
    return match.group(1).decode("ascii", errors="ignore") or None


def _canonical_codec(label: str | None) -> str | None:
    """把声明的 charset 归一化为 Python 解码器名；未知标签返回 ``None``。"""

    if not label:
        return None
    normalized = _CODEC_OVERRIDES.get(label.strip().lower(), label.strip().lower())
    try:
        return codecs.lookup(normalized).name
    except LookupError:
        return None


def _decode_strict(content: bytes, codec: str) -> str | None:
    """严格解码；失败（含 BOM 处理）返回 ``None``，由调用方回退到下一步。"""

    if codec == "utf-8" and content.startswith(codecs.BOM_UTF8):
        codec = "utf-8-sig"
    try:
        return content.decode(codec)
    except UnicodeDecodeError:
        return None


def _is_htmlish(content_type: str) -> bool:
    if not content_type:
        return True
    lowered = content_type.lower()
    return any(token in lowered for token in _HTMLISH_TYPES)
