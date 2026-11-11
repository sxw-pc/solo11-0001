"""编码解码链：响应头 charset → 页面 meta → UTF-8 容错，以及 Fetcher 集成。"""

from __future__ import annotations

import json

from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.encoding import decode_body
from mock_site.app import LAB_CASES, LAB_TITLE

TITLE = "城市内涝监测周报"
UTF8_BOM = b"\xef\xbb\xbf"


def _page(meta_charset: str | None = None) -> str:
    meta = f'<meta charset="{meta_charset}">' if meta_charset else ""
    return (
        f"<html><head>{meta}</head><body>"
        f"<h1 class='title'>{TITLE}</h1><p>二〇二四年六月</p></body></html>"
    )


# ---- 第 1 步：响应头 charset ----


def test_header_gbk_decodes_chinese() -> None:
    body = _page("gbk").encode("gbk")
    decoded = decode_body(body, {"content-type": "text/html; charset=gbk"})
    assert (decoded.encoding, decoded.source) == ("gbk", "header")
    assert TITLE in decoded.text


def test_header_gb2312_is_normalized_to_gbk() -> None:
    # 「〇」是 GBK 扩展字符，严格 GB2312 解不出来；按 GBK 解码才能拿到原文。
    body = "<p>二〇二四年六月</p>".encode("gbk")
    decoded = decode_body(body, {"content-type": "text/html; charset=gb2312"})
    assert (decoded.encoding, decoded.source) == ("gbk", "header")
    assert "二〇二四年六月" in decoded.text


def test_header_utf8_strips_bom() -> None:
    body = UTF8_BOM + _page("utf-8").encode("utf-8")
    decoded = decode_body(body, {"content-type": "text/html; charset=utf-8"})
    assert (decoded.encoding, decoded.source) == ("utf-8", "header")
    assert not decoded.text.startswith("\ufeff")
    assert TITLE in decoded.text


def test_header_name_and_charset_are_case_insensitive() -> None:
    body = _page("gbk").encode("gbk")
    decoded = decode_body(body, {"Content-Type": "Text/HTML; Charset=GBK"})
    assert (decoded.encoding, decoded.source) == ("gbk", "header")


def test_header_wins_over_conflicting_meta() -> None:
    # meta 谎称 utf-8，但响应头 gbk 优先，且正文确实是 GBK 字节。
    body = _page("utf-8").encode("gbk")
    decoded = decode_body(body, {"content-type": "text/html; charset=gbk"})
    assert (decoded.encoding, decoded.source) == ("gbk", "header")
    assert TITLE in decoded.text


# ---- 第 2 步：页面 meta charset（响应头缺失或声明错误时）----


def test_meta_used_when_header_has_no_charset() -> None:
    body = _page("gb2312").encode("gbk")
    decoded = decode_body(body, {"content-type": "text/html"})
    assert (decoded.encoding, decoded.source) == ("gbk", "meta")
    assert TITLE in decoded.text


def test_wrong_header_charset_falls_back_to_meta() -> None:
    # 响应头谎称 utf-8，严格解 GBK 字节失败，落到页面 meta 的 gbk。
    body = _page("gbk").encode("gbk")
    decoded = decode_body(body, {"content-type": "text/html; charset=utf-8"})
    assert (decoded.encoding, decoded.source) == ("gbk", "meta")
    assert TITLE in decoded.text


def test_unknown_header_charset_falls_back_to_meta() -> None:
    body = _page("gbk").encode("gbk")
    decoded = decode_body(body, {"content-type": "text/html; charset=x-bogus-enc"})
    assert (decoded.encoding, decoded.source) == ("gbk", "meta")
    assert TITLE in decoded.text


def test_meta_http_equiv_form_is_recognized() -> None:
    html = (
        '<html><head><meta http-equiv="Content-Type" '
        'content="text/html; charset=gbk"></head>'
        f"<body><h1 class='title'>{TITLE}</h1></body></html>"
    )
    decoded = decode_body(html.encode("gbk"), {"content-type": "text/html"})
    assert (decoded.encoding, decoded.source) == ("gbk", "meta")
    assert TITLE in decoded.text


# ---- 第 3 步：UTF-8 容错兜底 ----


def test_fallback_utf8_when_nothing_declared() -> None:
    body = _page().encode("utf-8")
    decoded = decode_body(body, {"content-type": "text/html"})
    assert (decoded.encoding, decoded.source) == ("utf-8", "fallback")
    assert TITLE in decoded.text


def test_fallback_replaces_invalid_bytes_without_raising() -> None:
    decoded = decode_body(b"\xff\xfe\x89<html>", {"content-type": "text/html"})
    assert (decoded.encoding, decoded.source) == ("utf-8", "fallback")
    assert "\ufffd" in decoded.text


def test_empty_body_decodes_to_empty_text() -> None:
    decoded = decode_body(b"", {"content-type": "text/html; charset=gbk"})
    assert decoded.text == ""


# ---- 非 HTML 响应不嗅探 meta（API 源不受影响）----


def test_json_response_skips_meta_sniffing() -> None:
    payload = json.dumps(
        {"note": "<meta charset=gbk>", "title": TITLE}, ensure_ascii=False
    ).encode("utf-8")
    decoded = decode_body(payload, {"content-type": "application/json"})
    assert (decoded.encoding, decoded.source) == ("utf-8", "fallback")
    assert json.loads(decoded.text)["title"] == TITLE


# ---- Fetcher 集成：真实 HTTP 响应走完整解码链 ----


def test_fetcher_decodes_gbk_and_keeps_raw_bytes(
    fetcher: Fetcher, mock_base_url: str
) -> None:
    result = fetcher.get(f"{mock_base_url}/lab/item/gbk")
    assert result.ok
    assert (result.encoding, result.encoding_source) == ("gbk", "header")
    assert LAB_TITLE in result.body
    # 原始字节原样保留，与 mock 站点发出的字节一致
    assert result.content == LAB_CASES["gbk"][1]
    assert result.raw_content == LAB_CASES["gbk"][1]


def test_fetcher_strips_bom_but_archive_keeps_it(
    fetcher: Fetcher, mock_base_url: str
) -> None:
    result = fetcher.get(f"{mock_base_url}/lab/item/utf8-bom")
    assert result.encoding == "utf-8"
    assert not result.body.startswith("\ufeff")
    # 留档字节仍带 BOM，不转码
    assert result.raw_content.startswith(UTF8_BOM)
    assert result.raw_content == LAB_CASES["utf8-bom"][1]


def test_fetcher_wrong_header_uses_meta(
    fetcher: Fetcher, mock_base_url: str
) -> None:
    result = fetcher.get(f"{mock_base_url}/lab/item/wrong-header")
    assert (result.encoding, result.encoding_source) == ("gbk", "meta")
    assert LAB_TITLE in result.body
