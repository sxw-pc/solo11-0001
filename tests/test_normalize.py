"""归一化函数：空白、日期、标签、HTML 正文与字段映射。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crawl_flow.config import FieldMapping, SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.pipeline.normalize import (
    clean_multiline,
    clean_text,
    html_to_text,
    normalize_record,
    parse_datetime,
    split_tags,
)


def test_clean_text_trims_and_collapses_whitespace() -> None:
    assert clean_text("  城 市 \n\t 公园  ") == "城 市 公园"
    assert clean_text(None) == ""


def test_clean_multiline_keeps_paragraphs() -> None:
    assert clean_multiline(" 第一段 \n\n 第二段\t续 ") == "第一段\n第二段 续"


def test_html_to_text_strips_tags_and_scripts() -> None:
    html = "<div><p>第一段</p><p>第二段 <b>加粗</b></p><script>bad()</script></div>"
    assert html_to_text(html) == "第一段\n第二段 加粗"


def test_split_tags_supports_chinese_separators_and_dedupe() -> None:
    assert split_tags(" 公园 、 花期|观测 ; 公园 ") == ["公园", "花期", "观测"]
    assert split_tags(["数据库", " 性能 ", "", None]) == ["数据库", "性能"]
    assert split_tags(None) == []


def test_parse_datetime_supports_iso_and_local_formats() -> None:
    assert parse_datetime("2024-03-01T09:30:00+08:00") == datetime(
        2024, 3, 1, 9, 30, tzinfo=timezone(timedelta(hours=8))
    )
    assert parse_datetime("2024年03月02日") == datetime(2024, 3, 2)
    assert parse_datetime("2024/03/03 10:00:00") == datetime(2024, 3, 3, 10, 0, 0)
    assert parse_datetime("2024.03.04") == datetime(2024, 3, 4)
    assert parse_datetime("2024-03-05 09:30") == datetime(2024, 3, 5, 9, 30)
    assert parse_datetime("不是日期") is None
    assert parse_datetime("") is None


def test_normalize_record_uses_field_map() -> None:
    source = SourceConfig(
        name="custom",
        type="api",
        base_url="http://127.0.0.1:1",
        field_map=FieldMapping(
            title="name",
            category="kind",
            tags="labels",
            published_at="date",
            content="body",
            url="link",
        ),
    )
    record = normalize_record(
        source,
        "x-1",
        {
            "name": "  带空格的标题 ",
            "kind": "技术",
            "labels": "缓存, 架构",
            "date": "2024/3/5",
            "body": "<p>正文段落</p>",
            "link": "http://example.test/x-1",
        },
        detail_url="http://detail.test/x-1",
        fetched_at=datetime(2024, 3, 5, tzinfo=timezone.utc),
    )

    assert record.title == "带空格的标题"
    assert record.category == "技术"
    assert record.tags == ["缓存", "架构"]
    assert record.published_at == datetime(2024, 3, 5)
    assert record.content == "正文段落"
    assert record.url == "http://example.test/x-1"


def test_normalize_record_falls_back_to_detail_url() -> None:
    source = SourceConfig(
        name="custom",
        type="html",
        base_url="http://127.0.0.1:1",
    )
    record = normalize_record(
        source,
        "x-2",
        {"title": "标题", "content": "正文"},
        detail_url="http://detail.test/x-2",
        fetched_at=datetime(2024, 3, 5, tzinfo=timezone.utc),
    )
    assert record.url == "http://detail.test/x-2"
    assert record.category is None
    assert record.tags == []
    assert record.published_at is None


def test_normalize_record_rejects_empty_title() -> None:
    source = SourceConfig(name="custom", type="api", base_url="http://127.0.0.1:1")
    with pytest.raises(ParseError) as excinfo:
        normalize_record(
            source,
            "x-3",
            {"title": "   "},
            detail_url="http://detail.test/x-3",
            fetched_at=datetime(2024, 3, 5, tzinfo=timezone.utc),
        )
    assert "标题为空" in excinfo.value.reason
