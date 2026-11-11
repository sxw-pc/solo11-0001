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

# 相对时间统一锚点：2026-11-11T00:30:00Z，刻意贴近 UTC 零点便于覆盖跨日边界。
ANCHOR = datetime(2026, 11, 11, 0, 30, tzinfo=timezone.utc)
TZ_PLUS8 = timezone(timedelta(hours=8))
TZ_MINUS5 = timezone(timedelta(hours=-5))
TZ_PLUS5 = timezone(timedelta(hours=5))


def parse(value: object) -> datetime | None:
    return parse_datetime(value, now=ANCHOR)


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


# ---- 用例类别 1：时区偏移（ISO Z/±HH:MM 与 GMT/UTC 后缀，含跨 UTC 零点）----


def test_parse_datetime_keeps_iso_offset() -> None:
    assert parse("2024-03-01T09:30:00+08:00") == datetime(
        2024, 3, 1, 9, 30, tzinfo=TZ_PLUS8
    )
    assert parse("2024-03-01T09:30:00Z") == datetime(
        2024, 3, 1, 9, 30, tzinfo=timezone.utc
    )


def test_parse_datetime_supports_gmt_utc_offset_suffixes() -> None:
    assert parse("2024-03-01T09:30:00 GMT+08:00") == datetime(
        2024, 3, 1, 9, 30, tzinfo=TZ_PLUS8
    )
    assert parse("2024-03-01 09:30:00 UTC-5") == datetime(
        2024, 3, 1, 9, 30, tzinfo=TZ_MINUS5
    )
    # 无冒号偏移与裸 GMT 后缀（视为 +00:00）
    assert parse("2024-03-01T09:30:00 GMT+0800") == datetime(
        2024, 3, 1, 9, 30, tzinfo=TZ_PLUS8
    )
    assert parse("2024-03-01T09:30:00 GMT") == datetime(2024, 3, 1, 9, 30)


def test_parse_datetime_offset_converts_to_same_utc_instant() -> None:
    # 三种写法表达同一时刻，换算 UTC 后必须完全相等（误差 0 秒）
    instant = parse("2024-03-01T09:30:00+08:00").astimezone(timezone.utc)
    assert parse("2024-03-01T06:30:00+05:00").astimezone(timezone.utc) == instant
    assert parse("2024-03-01T01:30:00Z").astimezone(timezone.utc) == instant


def test_parse_datetime_offset_crosses_utc_midnight() -> None:
    # 正偏移：UTC 日期回退到前一天
    assert parse("2026-11-11T01:30:00+05:00").astimezone(timezone.utc) == datetime(
        2026, 11, 10, 20, 30, tzinfo=timezone.utc
    )
    # 负偏移：UTC 日期前进到后一天
    assert parse("2026-11-10T20:00:00-04:00").astimezone(timezone.utc) == datetime(
        2026, 11, 11, 0, 0, tzinfo=timezone.utc
    )
    # 整点边界：+08:00 的 08:00 恰好是 UTC 零点
    assert parse("2026-11-11T08:00:00+08:00").astimezone(timezone.utc) == datetime(
        2026, 11, 11, 0, 0, tzinfo=timezone.utc
    )


# ---- 用例类别 2：中文（带空格/全角）、斜杠、点号等本地格式 ----


def test_parse_datetime_chinese_with_spaces_and_fullwidth() -> None:
    assert parse("2024 年 03 月 02 日 10:20") == datetime(2024, 3, 2, 10, 20)
    assert parse("2024年 03月 02日 10 时 20 分") == datetime(2024, 3, 2, 10, 20)
    assert parse("2024年03月02日 10时20分30秒") == datetime(2024, 3, 2, 10, 20, 30)
    # NFKC 全角数字与连字符
    assert parse("２０２４－０３－０５") == datetime(2024, 3, 5)
    # 首尾空白、换行、全角空格在解析前清洗
    assert parse("  \n2024年03月02日\t　 ") == datetime(2024, 3, 2)


def test_parse_datetime_slash_dot_and_compact_formats() -> None:
    assert parse("2024/3/5 9:05") == datetime(2024, 3, 5, 9, 5)
    assert parse("2024/03/03 10:00:00") == datetime(2024, 3, 3, 10, 0, 0)
    assert parse("2024.03.04") == datetime(2024, 3, 4)
    assert parse("20240305") == datetime(2024, 3, 5)
    assert parse("20240305102030") == datetime(2024, 3, 5, 10, 20, 30)


# ---- 用例类别 3：相对时间（以 fetched_at 为锚点，可复现）----


def test_parse_datetime_relative_chinese_units() -> None:
    assert parse("3 天前") == datetime(2026, 11, 8, 0, 30, tzinfo=timezone.utc)
    assert parse("3天前") == datetime(2026, 11, 8, 0, 30, tzinfo=timezone.utc)
    assert parse("2小时前") == datetime(2026, 11, 10, 22, 30, tzinfo=timezone.utc)
    assert parse("30分钟前") == datetime(2026, 11, 11, 0, 0, tzinfo=timezone.utc)
    assert parse("2周前") == datetime(2026, 10, 28, 0, 30, tzinfo=timezone.utc)
    assert parse("1个月前") == datetime(2026, 10, 11, 0, 30, tzinfo=timezone.utc)


def test_parse_datetime_relative_special_words_and_english() -> None:
    assert parse("刚刚") == ANCHOR
    assert parse("刚才") == ANCHOR
    assert parse("昨天") == datetime(2026, 11, 10, 0, 30, tzinfo=timezone.utc)
    assert parse("前天") == datetime(2026, 11, 9, 0, 30, tzinfo=timezone.utc)
    assert parse("2 hours ago") == datetime(
        2026, 11, 10, 22, 30, tzinfo=timezone.utc
    )
    assert parse("3 days ago") == datetime(
        2026, 11, 8, 0, 30, tzinfo=timezone.utc
    )


def test_parse_datetime_relative_crosses_utc_midnight() -> None:
    # 锚点 00:30Z，1 小时前落在前一天的 23:30Z
    assert parse("1小时前") == datetime(2026, 11, 10, 23, 30, tzinfo=timezone.utc)
    assert parse("30 分钟前") == datetime(2026, 11, 11, 0, 0, tzinfo=timezone.utc)


def test_parse_datetime_relative_is_deterministic_for_fixed_anchor() -> None:
    # 同一输入 + 同一锚点，重复解析结果稳定（不会这次成功下次 NULL/漂移）
    for _ in range(5):
        assert parse_datetime("3 天前", now=ANCHOR) == datetime(
            2026, 11, 8, 0, 30, tzinfo=timezone.utc
        )
        assert parse_datetime("不是日期", now=ANCHOR) is None


# ---- 用例类别 4：非法值仍按现有规则返回 None（落 NULL）----


@pytest.mark.parametrize(
    "raw",
    [
        "不是日期",
        "",
        "   ",
        "2024-13-40",
        "2024-02-30 10:00",
        "2024/13/01",
        "abc 天前",
        "3 天后",  # 只支持过去时的相对时间
        "昨天 10:00",
        "2024-03-01T25:00:00",
        None,
    ],
)
def test_parse_datetime_invalid_returns_none(raw: object) -> None:
    assert parse(raw) is None


def test_parse_datetime_passes_through_datetime_instances() -> None:
    aware = datetime(2024, 3, 1, 9, 30, tzinfo=TZ_PLUS8)
    assert parse_datetime(aware, now=ANCHOR) is aware
    assert parse_datetime(None, now=ANCHOR) is None


def test_normalize_record_passes_fetched_at_as_relative_anchor() -> None:
    source = SourceConfig(name="custom", type="html", base_url="http://127.0.0.1:1")
    record = normalize_record(
        source,
        "x-rel",
        {"title": "标题", "published_at": " 3 天前 "},
        detail_url="http://detail.test/x-rel",
        fetched_at=ANCHOR,
    )
    assert record.published_at == datetime(2026, 11, 8, 0, 30, tzinfo=timezone.utc)


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
