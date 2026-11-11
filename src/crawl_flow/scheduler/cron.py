"""标准 5 段 cron 表达式解析与下次触发时间计算。

只依赖标准库，不引入任何调度框架。支持语法：

- ``*`` 与 ``*/n``（每 n 个单位）
- 单值 ``5``、区间 ``1-5``、区间步长 ``1-10/2``、``5/15``（等价 ``5-max/15``）
- 逗号列表，如 ``0,30``、``1,15/2``
- 月份缩写 ``JAN``–``DEC``、星期缩写 ``SUN``–``SAT``（大小写不敏感）
- 星期字段 ``0`` 与 ``7`` 都表示周日

日期语义与 Vixie cron 一致：``day_of_month`` 与 ``day_of_week`` 两个字段
都被限制（不以 ``*`` 开头）时取**并集**，否则取交集。表达式按本地时间解释。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

# 下次触发时间的搜索上限：8 年覆盖闰日（含 2100 这类非闰世纪年）。
_MAX_SEARCH_DAYS = 366 * 8 + 2

_MONTH_NAMES = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

_DOW_NAMES = {
    "SUN": 0,
    "MON": 1,
    "TUE": 2,
    "WED": 3,
    "THU": 4,
    "FRI": 5,
    "SAT": 6,
}


class CronError(ValueError):
    """cron 表达式语法或取值非法。"""


class _Field:
    """单个字段的解析结果：取值集合 + 是否以 ``*`` 开头（Vixie star 标记）。"""

    __slots__ = ("values", "star")

    def __init__(self, values: set[int], star: bool) -> None:
        self.values = frozenset(values)
        self.star = star


class CronSchedule:
    """一个解析后的 cron 表达式，可反复计算下次触发时间。"""

    def __init__(self, expression: str) -> None:
        self.expression = expression.strip()
        parts = self.expression.split()
        if len(parts) != 5:
            raise CronError(f"cron 表达式必须是 5 段（分 时 日 月 周），实际 {len(parts)} 段")
        self._minute = _parse_field(parts[0], 0, 59, "minute")
        self._hour = _parse_field(parts[1], 0, 23, "hour")
        self._dom = _parse_field(parts[2], 1, 31, "day_of_month")
        self._month = _parse_field(parts[3], 1, 12, "month", names=_MONTH_NAMES)
        self._dow = _parse_field(parts[4], 0, 7, "day_of_week", names=_DOW_NAMES)
        # 星期的 7 归一为 0（周日）。
        self._dow_values = frozenset(value % 7 for value in self._dow.values)
        self._sorted_minutes = sorted(self._minute.values)
        self._sorted_hours = sorted(self._hour.values)

    def next_after(self, moment: datetime) -> datetime | None:
        """返回 ``moment`` 之后（不含）的下一个触发时间；8 年内无触发返回 None。

        返回值秒与微秒恒为 0，即触发点严格对齐到分钟边界。
        """

        base = moment.replace(second=0, microsecond=0) + timedelta(minutes=1)
        for offset in range(_MAX_SEARCH_DAYS):
            day = base.date() + timedelta(days=offset)
            if not self._day_matches(day):
                continue
            for hour in self._sorted_hours:
                if offset == 0 and hour < base.hour:
                    continue
                for minute in self._sorted_minutes:
                    if offset == 0 and hour == base.hour and minute < base.minute:
                        continue
                    return datetime(
                        day.year, day.month, day.day, hour, minute, tzinfo=moment.tzinfo
                    )
        return None

    def _day_matches(self, day: date) -> bool:
        if day.month not in self._month.values:
            return False
        dom_match = day.day in self._dom.values
        # Python weekday(): 周一=0 … 周日=6；cron: 周日=0。
        cron_dow = (day.weekday() + 1) % 7
        dow_match = cron_dow in self._dow_values
        if self._dom.star and self._dow.star:
            return True
        if self._dom.star:
            return dow_match
        if self._dow.star:
            return dom_match
        # 两个字段都被限制时按 Vixie cron 语义取并集。
        return dom_match or dow_match

    def __repr__(self) -> str:
        return f"CronSchedule({self.expression!r})"


def _parse_field(
    text: str,
    minimum: int,
    maximum: int,
    field_name: str,
    *,
    names: dict[str, int] | None = None,
) -> _Field:
    """解析一个字段为取值集合；非法输入抛 :class:`CronError`。"""

    values: set[int] = set()
    for atom in text.split(","):
        atom = atom.strip()
        if not atom:
            raise CronError(f"字段 {field_name} 含有空的分段: {text!r}")
        values.update(_parse_atom(atom, minimum, maximum, field_name, names=names))
    return _Field(values, star=text.startswith("*"))


def _parse_atom(
    atom: str,
    minimum: int,
    maximum: int,
    field_name: str,
    *,
    names: dict[str, int] | None,
) -> set[int]:
    if "/" in atom:
        range_part, _, step_text = atom.partition("/")
        try:
            step = int(step_text)
        except ValueError:
            raise CronError(f"字段 {field_name} 步长非法: {atom!r}") from None
        if step < 1:
            raise CronError(f"字段 {field_name} 步长必须 >= 1: {atom!r}")
    else:
        range_part, step = atom, 1

    if range_part == "*":
        low, high = minimum, maximum
    elif "-" in range_part:
        low_text, _, high_text = range_part.partition("-")
        low = _parse_value(low_text, minimum, maximum, field_name, names)
        high = _parse_value(high_text, minimum, maximum, field_name, names)
        if low > high:
            raise CronError(f"字段 {field_name} 区间起点大于终点: {atom!r}")
    else:
        low = _parse_value(range_part, minimum, maximum, field_name, names)
        # 带步长的单值（如 5/15）按 Vixie 语义扩展到字段上限。
        high = maximum if "/" in atom else low

    return set(range(low, high + 1, step))


def _parse_value(
    text: str,
    minimum: int,
    maximum: int,
    field_name: str,
    names: dict[str, int] | None,
) -> int:
    if names and text.upper() in names:
        return names[text.upper()]
    try:
        value = int(text)
    except ValueError:
        raise CronError(f"字段 {field_name} 含有非法取值: {text!r}") from None
    if not minimum <= value <= maximum:
        raise CronError(
            f"字段 {field_name} 取值 {value} 超出范围 [{minimum}, {maximum}]"
        )
    return value
