"""时间工具：清洗、ISO/中文/相对时间解析、UTC 归一化与存储形态。"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from crawl_flow.timeutils import (
    clean_datetime_text,
    parse_datetime,
    to_utc_naive,
    utc_date_text,
    utc_storage_text,
)

ANCHOR = datetime(2024, 5, 10, 12, 0, 0)
BEIJING = timezone(timedelta(hours=8))


def test_clean_datetime_text_normalizes_fullwidth_and_whitespace() -> None:
    assert clean_datetime_text("　２０２４－０３－０１　") == "2024-03-01"
    assert clean_datetime_text("\n\t 2024-03-01 \r\n") == "2024-03-01"
    assert clean_datetime_text("2024：03：01") == "2024:03:01"
    assert clean_datetime_text(None) == ""
    # 中文与日期汉字不受 NFKC 影响
    assert clean_datetime_text("2024年5月1日") == "2024年5月1日"


def test_parse_iso_keeps_offsets_without_regression() -> None:
    assert parse_datetime("2024-03-01T09:30:00+08:00") == datetime(
        2024, 3, 1, 9, 30, tzinfo=BEIJING
    )
    assert parse_datetime("2024-03-01T09:30:00Z") == datetime(
        2024, 3, 1, 9, 30, tzinfo=timezone.utc
    )
    # Python 3.11 原生支持无冒号偏移
    assert parse_datetime("2024-03-01T09:30:00+0800") == datetime(
        2024, 3, 1, 9, 30, tzinfo=BEIJING
    )
    assert parse_datetime("2024-03-05 09:30") == datetime(2024, 3, 5, 9, 30)
    assert parse_datetime("2024/03/03 10:00:00") == datetime(2024, 3, 3, 10, 0, 0)
    assert parse_datetime("2024.03.04") == datetime(2024, 3, 4)


def test_parse_chinese_dates_with_and_without_padding() -> None:
    assert parse_datetime("2024年03月02日") == datetime(2024, 3, 2)
    assert parse_datetime("2024年5月1日") == datetime(2024, 5, 1)
    assert parse_datetime("2024年3月7日 08:15") == datetime(2024, 3, 7, 8, 15)
    assert parse_datetime("2024年03月15日 10:20:30") == datetime(
        2024, 3, 15, 10, 20, 30
    )
    # 日期与时间之间无空白、全角冒号（NFKC 后归一）
    assert parse_datetime("2024年03月15日10:20:30") == datetime(2024, 3, 15, 10, 20, 30)
    assert parse_datetime("2024年03月15日 10：20") == datetime(2024, 3, 15, 10, 20)
    # 非法日历日返回 None
    assert parse_datetime("2024年2月30日") is None
    assert parse_datetime("2024-13-01") is None


def test_parse_relative_time_uses_explicit_anchor() -> None:
    assert parse_datetime("3天前", anchor=ANCHOR) == datetime(2024, 5, 7, 12, 0)
    assert parse_datetime("2 小时前", anchor=ANCHOR) == datetime(2024, 5, 10, 10, 0)
    assert parse_datetime("5分钟前", anchor=ANCHOR) == datetime(2024, 5, 10, 11, 55)
    assert parse_datetime("30秒前", anchor=ANCHOR) == datetime(2024, 5, 10, 11, 59, 30)
    assert parse_datetime("1周前", anchor=ANCHOR) == datetime(2024, 5, 3, 12, 0)
    assert parse_datetime("刚刚", anchor=ANCHOR) == ANCHOR
    # 英文相对时间
    assert parse_datetime("2 days ago", anchor=ANCHOR) == datetime(2024, 5, 8, 12, 0)
    assert parse_datetime("1 hour ago", anchor=ANCHOR) == datetime(2024, 5, 10, 11, 0)
    assert parse_datetime("30 SECONDS AGO", anchor=ANCHOR) == datetime(
        2024, 5, 10, 11, 59, 30
    )
    assert parse_datetime("just now", anchor=ANCHOR) == ANCHOR
    # 全角数字与内嵌空白也能识别
    assert parse_datetime("　３　天前　", anchor=ANCHOR) == datetime(2024, 5, 7, 12, 0)


def test_parse_relative_month_uses_calendar_with_clamping() -> None:
    assert parse_datetime("1个月前", anchor=datetime(2024, 5, 10, 12, 0)) == datetime(
        2024, 4, 10, 12, 0
    )
    # 目标月没有 31 号时夹到月末（2024 为闰年，2 月 29 天）
    assert parse_datetime("1个月前", anchor=datetime(2024, 3, 31)) == datetime(
        2024, 2, 29
    )
    assert parse_datetime("2个月前", anchor=datetime(2024, 1, 31)) == datetime(
        2023, 11, 30
    )
    assert parse_datetime("12个月前", anchor=datetime(2024, 1, 15)) == datetime(
        2023, 1, 15
    )


def test_parse_relative_time_with_aware_anchor_returns_naive_utc() -> None:
    result = parse_datetime(
        "3天前", anchor=datetime(2024, 5, 10, 12, tzinfo=timezone.utc)
    )
    assert result == datetime(2024, 5, 7, 12)
    assert result is not None and result.tzinfo is None


def test_parse_relative_time_without_anchor_does_not_raise() -> None:
    assert isinstance(parse_datetime("2天前"), datetime)
    assert parse_datetime("刚刚") is not None


def test_parse_datetime_cleans_surrounding_whitespace_and_newlines() -> None:
    assert parse_datetime("\n\t 2024-03-01 09:30:00 \n") == datetime(2024, 3, 1, 9, 30)
    assert parse_datetime("2024年5月1日\n") == datetime(2024, 5, 1)
    assert parse_datetime("　2024-03-01　") == datetime(2024, 3, 1)


def test_parse_datetime_returns_none_for_unparseable_input() -> None:
    assert parse_datetime("不是日期") is None
    assert parse_datetime("昨天") is None
    assert parse_datetime("") is None
    assert parse_datetime(None) is None
    assert parse_datetime("   \n  ") is None


def test_parse_datetime_passes_through_datetime_instances() -> None:
    value = datetime(2024, 1, 1, tzinfo=BEIJING)
    assert parse_datetime(value) is value


def test_relative_parsing_is_deterministic_for_same_anchor() -> None:
    # 同一输入 + 同一锚点必须得到同一结果（重复采集幂等的基础）
    first = parse_datetime("3天前", anchor=ANCHOR)
    second = parse_datetime("3天前", anchor=ANCHOR)
    assert first == second


def test_to_utc_naive_converts_offsets_and_preserves_naive() -> None:
    assert to_utc_naive(datetime(2024, 3, 1, 9, 30, tzinfo=BEIJING)) == datetime(
        2024, 3, 1, 1, 30
    )
    naive = datetime(2024, 3, 1, 1, 30)
    assert to_utc_naive(naive) == naive
    assert to_utc_naive(naive).tzinfo is None


def test_utc_storage_text_matches_sqlite_storage_shape() -> None:
    assert utc_storage_text(datetime(2024, 3, 2, 1, 30, tzinfo=timezone.utc)) == (
        "2024-03-02 01:30:00"
    )
    assert utc_storage_text(
        datetime(2024, 3, 2, 1, 30, 0, 123456, tzinfo=timezone.utc)
    ) == "2024-03-02 01:30:00.123456"
    # 带偏移输入先换算 UTC
    assert utc_storage_text(datetime(2024, 3, 2, 9, 30, tzinfo=BEIJING)) == (
        "2024-03-02 01:30:00"
    )


def test_utc_date_text_uses_utc_calendar_day() -> None:
    # 北京时间 3 月 2 日 07:59 仍是 UTC 3 月 1 日
    assert utc_date_text(datetime(2024, 3, 2, 7, 59, tzinfo=BEIJING)) == "2024-03-01"
    assert utc_date_text(datetime(2024, 3, 2, 8, 0, tzinfo=BEIJING)) == "2024-03-02"
    assert utc_date_text(datetime(2024, 3, 2, 0, 0, tzinfo=timezone.utc)) == (
        "2024-03-02"
    )


@pytest.mark.parametrize(
    "value",
    [
        "2024-03-01T09:30:00+08:00",
        "2024-03-01T09:30:00Z",
        "2024年5月1日",
        "3天前",
        "　２０２４－０３－０１　",
    ],
)
def test_parse_then_utc_roundtrip(value: str) -> None:
    parsed = parse_datetime(value, anchor=ANCHOR)
    assert parsed is not None
    stored = to_utc_naive(parsed)
    assert stored.tzinfo is None
    # 存储形态可被 SQLite DATETIME 重新读回为等价 naive datetime
    read_back = datetime.strptime(utc_storage_text(parsed)[:19], "%Y-%m-%d %H:%M:%S")
    assert read_back == stored
