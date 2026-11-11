"""编码链与解析容错：多编码统一 Unicode、畸形 HTML 不误判失败。

覆盖：
- 解码优先级：响应头 charset → BOM → 页面 meta → UTF-8 容错；
- 响应头声明错误（与页面 meta 冲突）时回退到 meta；
- GBK / GB2312 / UTF-8 BOM / 未闭合标签 / 完全不可解析页面；
- 选择器缺字段与原始字节留档。
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from typing import Any

import httpx
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.encoding import decode_body
from crawl_flow.models import RawRecordRow, RecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.sources.demo_html import DemoHtmlAdapter
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def _page(title: str, *, meta: str | None = None, content: str = "") -> str:
    head = f'<meta charset="{meta}">' if meta else ""
    return (
        f"<!doctype html><html><head>{head}<title>{title}</title></head>"
        f'<body><h1 class="title">{title}</h1>'
        f'<div class="content">{content}</div></body></html>'
    )


# ---- 解码链单元测试 ----


def test_response_header_charset_takes_priority() -> None:
    # 页面 meta 谎报 utf-8，响应头声明 gbk；响应头优先。
    content = _page("响应头优先", meta="utf-8").encode("gbk")

    decoded = decode_body(content, "text/html; charset=gbk")

    assert decoded.source == "header"
    assert decoded.encoding == "gbk"
    assert decoded.declared == "gbk"
    assert "响应头优先" in decoded.text


def test_meta_charset_used_when_header_has_none() -> None:
    content = _page("元信息声明", meta="gbk").encode("gbk")

    decoded = decode_body(content, "text/html")

    assert decoded.source == "meta"
    assert decoded.encoding == "gbk"
    assert "元信息声明" in decoded.text


def test_gb2312_declaration_decoded_with_gbk_superset() -> None:
    # 「喆」在 GB2312 中没有编码，只有 GBK 有；按 gb2312 严格解码会失败。
    content = _page("巡查记录", meta="gb2312", content="负责人：张喆。").encode("gbk")

    decoded = decode_body(content, "text/html")

    assert decoded.source == "meta"
    assert decoded.encoding == "gbk"
    assert decoded.declared == "gb2312"
    assert "张喆" in decoded.text


def test_utf8_bom_is_stripped() -> None:
    content = _page("带 BOM 的页面", meta="utf-8").encode("utf-8-sig")

    without_header = decode_body(content, "text/html")
    with_header = decode_body(content, "text/html; charset=utf-8")

    assert without_header.source == "bom"
    assert without_header.encoding == "utf-8"
    assert not without_header.text.startswith("﻿")
    assert with_header.source == "header"
    assert not with_header.text.startswith("﻿")
    assert "带 BOM 的页面" in with_header.text


def test_wrong_header_charset_falls_back_to_meta() -> None:
    # 响应头声称 utf-8，实际是 GBK 字节；严格 utf-8 解码失败后退回 meta 的 gbk。
    content = _page("声明错误的响应头", meta="gbk").encode("gbk")

    decoded = decode_body(content, "text/html; charset=utf-8")

    assert decoded.source == "meta"
    assert decoded.encoding == "gbk"
    assert "声明错误的响应头" in decoded.text


def test_unknown_header_charset_is_ignored() -> None:
    content = _page("未知编码标签", meta="gbk").encode("gbk")

    with_meta = decode_body(content, "text/html; charset=x-unknown-codec")
    without_meta = decode_body(_page("无 meta 页面").encode("utf-8"), "text/html; charset=x-unknown-codec")

    assert with_meta.source == "meta"
    assert "未知编码标签" in with_meta.text
    assert without_meta.source == "fallback"
    assert "无 meta 页面" in without_meta.text


def test_fallback_is_utf8_replace_and_never_raises() -> None:
    # 无法按任何声明解码的坏字节：末位 UTF-8 容错，不抛异常。
    content = b"\xff\xfe<html><body><h1 class=\"title\">\x81\x82</h1></body></html>"

    decoded = decode_body(content, "text/html")

    assert decoded.source == "fallback"
    assert decoded.encoding == "utf-8"
    assert "�" in decoded.text


def test_json_response_does_not_scan_meta() -> None:
    # JSON 正文里恰好出现 meta 字样时，不应把内容当成 HTML 去猜编码。
    payload = '{"note": "<meta charset=gbk>", "title": "中文标题"}'.encode("utf-8")

    decoded = decode_body(payload, "application/json")

    assert decoded.source == "fallback"
    assert decoded.text == payload.decode("utf-8")


# ---- 端到端：多编码 / 畸形 HTML / 完全不可解析 ----


@pytest.fixture
def legacy_source(make_source: Any) -> SourceConfig:
    return make_source(
        "demo_legacy",
        "html",
        list_path="/legacy/list",
        detail_path="/legacy/item/{id}",
    )


def test_legacy_source_unifies_encodings_to_unicode(
    legacy_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
) -> None:
    summary = run_source(legacy_source, db, raw_store)

    # 5 条可解析（含畸形页面）正常入库；2 条必填字段完全缺失记失败。
    assert (summary.ok, summary.failed, summary.skipped) == (5, 2, 0)

    with Session(db.engine) as session:
        rows = {
            row.external_id: row
            for row in session.execute(
                select(RecordRow).where(RecordRow.source == "demo_legacy")
            ).scalars()
        }

    assert set(rows) == {
        "legacy-gbk",
        "legacy-gb2312",
        "legacy-utf8-bom",
        "legacy-bad-header",
        "legacy-malformed",
    }

    # GBK 与 UTF-8 BOM 两个页面同一标题：入库后是完全相同的 Unicode。
    assert rows["legacy-gbk"].title == "社区卫生服务站运营月报"
    assert rows["legacy-utf8-bom"].title == rows["legacy-gbk"].title
    assert "社区卫生服务站运营月报" == rows["legacy-utf8-bom"].title

    assert rows["legacy-gb2312"].title == "老旧厂区安全巡查记录"
    assert "张喆" in rows["legacy-gb2312"].content
    assert rows["legacy-bad-header"].title == "秋季道路养护计划"
    assert "坑槽修补" in rows["legacy-bad-header"].content
    assert rows["legacy-malformed"].title == "河道水质周报"
    assert "溶解氧" in rows["legacy-malformed"].content
    assert "浊度" in rows["legacy-malformed"].content

    for row in rows.values():
        assert "�" not in row.title
        assert "�" not in row.content


def test_malformed_html_extracts_required_fields(
    legacy_source: SourceConfig,
    fetcher: Fetcher,
) -> None:
    adapter = DemoHtmlAdapter(legacy_source, fetcher)
    result = adapter.fetch_detail("legacy-malformed")
    fields = adapter.parse_detail(result, "legacy-malformed")

    assert fields["title"] == "河道水质周报"
    assert "溶解氧与氨氮指标均达标" in fields["content"]
    assert "下游断面浊度较上周下降" in fields["content"]


def test_missing_content_selector_raises_parse_error(
    legacy_source: SourceConfig,
    fetcher: Fetcher,
) -> None:
    adapter = DemoHtmlAdapter(legacy_source, fetcher)
    result = adapter.fetch_detail("legacy-no-content")

    with pytest.raises(ParseError) as excinfo:
        adapter.parse_detail(result, "legacy-no-content")

    assert excinfo.value.external_id == "legacy-no-content"
    assert "div.content" in excinfo.value.reason


def test_unparseable_page_raises_parse_error(
    legacy_source: SourceConfig,
    fetcher: Fetcher,
) -> None:
    adapter = DemoHtmlAdapter(legacy_source, fetcher)
    result = adapter.fetch_detail("legacy-broken")

    with pytest.raises(ParseError) as excinfo:
        adapter.parse_detail(result, "legacy-broken")

    assert excinfo.value.external_id == "legacy-broken"
    assert "h1.title" in excinfo.value.reason


def test_failed_items_record_error_and_keep_raw_snapshot(
    legacy_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
) -> None:
    run_source(legacy_source, db, raw_store)

    with Session(db.engine) as session:
        raw_rows = {
            row.external_id: row
            for row in session.execute(
                select(RawRecordRow).where(RawRecordRow.source == "demo_legacy")
            ).scalars()
        }

    failed = {name: row for name, row in raw_rows.items() if row.status == "failed"}
    assert set(failed) == {"legacy-no-content", "legacy-broken"}
    assert failed["legacy-no-content"].error is not None
    assert "div.content" in failed["legacy-no-content"].error
    assert failed["legacy-broken"].error is not None
    assert "h1.title" in failed["legacy-broken"].error
    for row in failed.values():
        assert row.raw_path is not None
        assert Path(row.raw_path).is_file()


def test_raw_archive_keeps_original_bytes(
    legacy_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    mock_base_url: str,
) -> None:
    run_source(legacy_source, db, raw_store)

    with Session(db.engine) as session:
        row = session.execute(
            select(RawRecordRow).where(RawRecordRow.external_id == "legacy-gbk")
        ).scalar_one()

    assert row.raw_path is not None
    payload = json.loads(Path(row.raw_path).read_text(encoding="utf-8"))
    served = httpx.get(f"{mock_base_url}/legacy/item/legacy-gbk").content

    # 留档的是服务端原始 GBK 字节，不是转码后的 UTF-8 文本。
    assert base64.b64decode(payload["body_b64"]) == served
    assert payload["encoding"] == "gbk"
    assert payload["encoding_source"] == "meta"
    assert "body" not in payload
