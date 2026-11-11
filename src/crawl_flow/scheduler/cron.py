"""标准 5 段 cron 表达式解析与下次触发时间计算。

仅使用标准库实现，不依赖任何第三方调度框架。支持的语法：

- ``*``：匹配该字段所有合法值
- 数字：``5``
- 列表：``1,15,30``
- 范围：``1-5``
- 步长：``*/15``、``1-10/2``（``a-b/n`` 的简写 ``*/n`` 等价于全范围步长）

字段顺序固定为标准 cron：``分 时 日 月 周``，不支持秒位与年份位。
星期取值 ``0-6``（0 与 7 均表示周日，这里只接受 0-6）。
``日`` 与 ``周`` 的匹配遵循标准 cron 语义：只要其中一个不是 ``*``，
两者为「或」关系（命中其一即触发）。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

# 各字段合法范围：分钟、小时、日、月、星期（星期 0=周一 … 6=周日）
FIELD_RANGES: tuple[tuple[int, int], ...] = (
    (0, 59),
    (0, 23),
    (1, 31),
    (1, 12),
    (0, 6),
)
FIELD_NAMES = ("minute", "hour", "day_of_month", "month", "day_of_week")


class CronParseError(ValueError):
    """cron 表达式语法或取值非法。"""


@dataclass(frozen=True)
class CronSpec:
    """解析后的 cron 表达式，:meth:`next_after` 计算下次触发时刻。"""

    expression: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days_of_month: frozenset[int]
    months: frozenset[int]
    days_of_week: frozenset[int]
    # 记录日 / 周字段是否为通配，用于实现标准 cron 的「或」语义
    dom_restricted: bool
    dow_restricted: bool

    def matches(self, moment: datetime) -> bool:
        """判断某个时刻（naive UTC）是否命中表达式。"""

        if moment.month not in self.months:
            return False
        if moment.hour not in self.hours or moment.minute not in self.minutes:
            return False
        dom_hit = moment.day in self.days_of_month
        # Python weekday()：周一=0 … 周日=6，与本模块星期字段定义一致
        dow_hit = moment.weekday() in self.days_of_week
        if not self.dom_restricted and not self.dow_restricted:
            return True
        if self.dom_restricted and self.dow_restricted:
            return dom_hit or dow_hit
        return dom_hit if self.dom_restricted else dow_hit

    def next_after(self, moment: datetime) -> datetime:
        """返回严格晚于 ``moment`` 的下一次触发时刻（naive UTC，秒以下清零）。

        采用逐分钟推进的方式查找，最多扫描 4 年，避免构造触发序列带来的
        内存开销；找不到时抛 :class:`CronParseError`（例如 ``2 2 31 2 *``，
        2 月永远没有 31 号）。
        """

        candidate = moment.replace(second=0, microsecond=0) + timedelta(minutes=1)
        deadline = moment + timedelta(days=366 * 4)
        while candidate <= deadline:
            if candidate.month not in self.months:
                # 跳到下个月 1 号 00:00
                candidate = _first_of_next_month(candidate)
                continue
            if candidate.day not in self._candidate_days(candidate):
                candidate = (candidate + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if candidate.hour not in self.hours:
                candidate = (candidate + timedelta(hours=1)).replace(minute=0)
                continue
            if candidate.minute not in self.minutes:
                candidate += timedelta(minutes=1)
                continue
            return candidate
        raise CronParseError(
            f"cron 表达式 {self.expression!r} 在未来 4 年内没有任何触发时刻"
        )

    def _candidate_days(self, moment: datetime) -> frozenset[int]:
        """日 / 周「或」语义下，当前月份实际可命中的「日」集合。"""

        if not self.dom_restricted and not self.dow_restricted:
            return self.days_of_month
        import calendar

        last_day = calendar.monthrange(moment.year, moment.month)[1]
        allowed: set[int] = set()
        for day in range(1, last_day + 1):
            probe = moment.replace(day=day)
            dom_hit = day in self.days_of_month
            dow_hit = probe.weekday() in self.days_of_week
            if self.dom_restricted and self.dow_restricted:
                if dom_hit or dow_hit:
                    allowed.add(day)
            elif self.dom_restricted:
                if dom_hit:
                    allowed.add(day)
            elif dow_hit and day <= last_day:
                allowed.add(day)
        return frozenset(allowed)


def parse_cron(expression: str) -> CronSpec:
    """解析标准 5 段 cron 表达式，非法时抛 :class:`CronParseError`。"""

    if not isinstance(expression, str):
        raise CronParseError("cron 表达式必须是字符串")
    fields = expression.split()
    if len(fields) != 5:
        raise CronParseError(
            f"cron 表达式必须恰好包含 5 段（分 时 日 月 周），实际 {len(fields)} 段: {expression!r}"
        )

    parsed: list[frozenset[int]] = []
    for index, (raw, (minimum, maximum)) in enumerate(zip(fields, FIELD_RANGES)):
        try:
            values = _parse_field(raw, minimum, maximum)
        except CronParseError as exc:
            raise CronParseError(f"第 {index + 1} 段（{FIELD_NAMES[index]}）{exc}") from exc
        parsed.append(frozenset(values))

    minutes, hours, days, months, weekdays = parsed
    return CronSpec(
        expression=expression,
        minutes=minutes,
        hours=hours,
        days_of_month=days,
        months=months,
        days_of_week=weekdays,
        dom_restricted=not fields[2].strip().startswith("*"),
        dow_restricted=not fields[4].strip().startswith("*"),
    )


def _parse_field(raw: str, minimum: int, maximum: int) -> set[int]:
    """解析单个 cron 字段为合法整数集合。"""

    values: set[int] = set()
    for piece in raw.split(","):
        if not piece:
            raise CronParseError(f"存在空片段: {raw!r}")
        step = 1
        if "/" in piece:
            head, step_text = piece.split("/", 1)
            if not step_text.isdigit() or int(step_text) <= 0:
                raise CronParseError(f"步长必须是正整数: {piece!r}")
            step = int(step_text)
        else:
            head = piece

        if head == "*":
            start, end = minimum, maximum
        elif "-" in head:
            range_text = head.split("-")
            if len(range_text) != 2 or not all(part.isdigit() for part in range_text):
                raise CronParseError(f"范围格式应为 a-b: {piece!r}")
            start, end = int(range_text[0]), int(range_text[1])
        elif head.isdigit():
            if "/" in piece:
                # ``5/10`` 这种写法按「从 5 到字段上限每 10」处理
                start, end = int(head), maximum
            else:
                start = end = int(head)
        else:
            raise CronParseError(f"包含非法字符: {piece!r}")

        if not minimum <= start <= maximum or not minimum <= end <= maximum:
            raise CronParseError(f"取值超出合法范围 {minimum}-{maximum}: {piece!r}")
        if start > end:
            raise CronParseError(f"范围起点大于终点: {piece!r}")
        values.update(range(start, end + 1, step))

    if not values:
        raise CronParseError(f"没有匹配到任何合法值: {raw!r}")
    return values


def _first_of_next_month(moment: datetime) -> datetime:
    if moment.month == 12:
        return moment.replace(year=moment.year + 1, month=1, day=1, hour=0, minute=0)
    return moment.replace(month=moment.month + 1, day=1, hour=0, minute=0)
