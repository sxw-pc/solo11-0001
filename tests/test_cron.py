"""cron 表达式解析与下次触发时间计算。"""

from __future__ import annotations

from datetime import datetime

import pytest

from crawl_flow.scheduler.cron import CronError, CronSchedule


def test_every_minute() -> None:
    cron = CronSchedule("* * * * *")
    assert cron.next_after(datetime(2026, 11, 12, 10, 3, 45)) == datetime(2026, 11, 12, 10, 4)


def test_next_fire_aligns_to_minute_boundary() -> None:
    cron = CronSchedule("*/7 * * * *")
    fire = cron.next_after(datetime(2026, 11, 12, 10, 3, 59, 999999))
    assert fire is not None
    assert fire.second == 0 and fire.microsecond == 0
    assert fire.minute == 7


def test_step_expression() -> None:
    cron = CronSchedule("*/15 * * * *")
    assert cron.next_after(datetime(2026, 11, 12, 10, 15)) == datetime(2026, 11, 12, 10, 30)
    assert cron.next_after(datetime(2026, 11, 12, 10, 45)) == datetime(2026, 11, 12, 11, 0)


def test_specific_time_of_day() -> None:
    cron = CronSchedule("30 2 * * *")
    assert cron.next_after(datetime(2026, 11, 12, 1, 0)) == datetime(2026, 11, 12, 2, 30)
    assert cron.next_after(datetime(2026, 11, 12, 2, 30)) == datetime(2026, 11, 13, 2, 30)


def test_comma_list_and_range() -> None:
    cron = CronSchedule("0,30 9-17 * * *")
    assert cron.next_after(datetime(2026, 11, 12, 9, 0)) == datetime(2026, 11, 12, 9, 30)
    assert cron.next_after(datetime(2026, 11, 12, 17, 30)) == datetime(2026, 11, 13, 9, 0)


def test_range_with_step() -> None:
    cron = CronSchedule("10-20/5 * * * *")
    assert cron.next_after(datetime(2026, 11, 12, 10, 10)) == datetime(2026, 11, 12, 10, 15)
    assert cron.next_after(datetime(2026, 11, 12, 10, 20)) == datetime(2026, 11, 12, 11, 10)


def test_single_value_with_step_extends_to_max() -> None:
    # Vixie 语义：5/15 等价 5-59/15
    cron = CronSchedule("5/15 * * * *")
    assert cron.next_after(datetime(2026, 11, 12, 10, 5)) == datetime(2026, 11, 12, 10, 20)


def test_day_of_week_names() -> None:
    cron = CronSchedule("0 9 * * MON")
    # 2026-11-12 是周四，下一个周一是 2026-11-16
    assert cron.next_after(datetime(2026, 11, 12, 0, 0)) == datetime(2026, 11, 16, 9, 0)


def test_day_of_week_range_names() -> None:
    cron = CronSchedule("0 0 * * MON-FRI")
    # 2026-11-14 是周六，下一个是周一 2026-11-16
    assert cron.next_after(datetime(2026, 11, 14, 0, 0)) == datetime(2026, 11, 16, 0, 0)


def test_day_of_week_seven_is_sunday() -> None:
    cron = CronSchedule("0 0 * * 7")
    # 2026-11-15 是周日
    assert cron.next_after(datetime(2026, 11, 12, 0, 0)) == datetime(2026, 11, 15, 0, 0)


def test_month_names() -> None:
    cron = CronSchedule("0 0 1 JAN *")
    assert cron.next_after(datetime(2026, 11, 12)) == datetime(2027, 1, 1, 0, 0)


def test_dom_and_dow_union_semantics() -> None:
    # 两个字段都受限时取并集：每月 13 号或每周五
    cron = CronSchedule("0 0 13 * FRI")
    # 2026-11-13 是周五且是 13 号；下一个并集命中是 2026-11-20（周五）
    assert cron.next_after(datetime(2026, 11, 13, 0, 0)) == datetime(2026, 11, 20, 0, 0)
    # 2026-12-13 是周日（仅 13 号命中）
    assert cron.next_after(datetime(2026, 11, 21, 0, 0)) == datetime(2026, 11, 27, 0, 0)


def test_dom_step_counts_as_star_for_union() -> None:
    # dom 以 * 开头视为未受限：只在周一生效
    cron = CronSchedule("0 0 */2 * MON")
    assert cron.next_after(datetime(2026, 11, 12, 0, 0)) == datetime(2026, 11, 16, 0, 0)


def test_dom_only() -> None:
    cron = CronSchedule("0 12 15 * *")
    assert cron.next_after(datetime(2026, 11, 12, 0, 0)) == datetime(2026, 11, 15, 12, 0)


def test_leap_day() -> None:
    cron = CronSchedule("0 0 29 2 *")
    assert cron.next_after(datetime(2026, 3, 1)) == datetime(2028, 2, 29, 0, 0)


def test_never_fires_returns_none() -> None:
    cron = CronSchedule("0 0 31 2 *")  # 2 月没有 31 号
    assert cron.next_after(datetime(2026, 1, 1)) is None


def test_year_rollover() -> None:
    cron = CronSchedule("0 0 1 1 *")
    assert cron.next_after(datetime(2026, 11, 12)) == datetime(2027, 1, 1, 0, 0)


@pytest.mark.parametrize(
    "expression",
    [
        "* * * *",  # 只有 4 段
        "* * * * * *",  # 6 段
        "60 * * * *",  # 分钟超界
        "* 24 * * *",  # 小时超界
        "0 0 0 * *",  # 日不能为 0
        "0 0 * 13 *",  # 月份超界
        "0 0 * * 8",  # 星期超界
        "*/0 * * * *",  # 步长为 0
        "*/x * * * *",  # 步长非数字
        "20-10 * * * *",  # 区间反向
        "a b c d e",  # 非法取值
        "0 0 * * FOO",  # 未知星期名
        "0 0 * * ",  # 空段（实际 4 段）
        ",,,,",
    ],
)
def test_invalid_expressions(expression: str) -> None:
    with pytest.raises(CronError):
        CronSchedule(expression)


def test_case_insensitive_names() -> None:
    cron = CronSchedule("0 0 * jan mon")
    assert cron.next_after(datetime(2026, 11, 12)) == datetime(2027, 1, 4, 0, 0)  # 2027-01-04 是周一
