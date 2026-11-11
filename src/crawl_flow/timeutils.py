"""时间处理工具：清洗、解析、UTC 归一化与存储格式，全部基于 Python 标准库。

约定：

- 外部输入的日期字符串先做全角/空白清洗，再按 ISO、固定模板、相对时间依次解析。
- 带时区偏移的时间统一换算为 UTC；不带偏移的时间视为 UTC（naive）。
- SQLite 的 ``DATETIME`` 列不保留时区，入库前一律转成无时区的 UTC ``datetime``，
  存储文本统一为 ``YYYY-MM-DD HH:MM:SS[.ffffff]``，与现有数据模型兼容。
- 无法识别的输入返回 ``None``，由调用方决定是否落 NULL。
"""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta, timezone
from typing import Any

# 固定模板：覆盖中文、斜杠、点号、短横线等常见写法；strptime 在 Python 3.11
# 下对 %m/%d 接受补零与非补零两种形式。
_DATETIME_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d",
    "%Y.%m.%d %H:%M:%S",
    "%Y.%m.%d %H:%M",
    "%Y.%m.%d",
    "%Y年%m月%d日 %H:%M:%S",
    "%Y年%m月%d日 %H:%M",
    "%Y年%m月%d日",
)

# 「3天前」「2 小时前」「2 days ago」「刚刚」等相对时间。
_RELATIVE_PATTERN = re.compile(
    r"^\s*(?P<value>\d+)\s*"
    r"(?P<unit>个?月|周|星期|天|日|小时|分钟?|秒钟?|h|hr|hour|hours|d|day|days|m|min|minute|minutes|s|sec|second|seconds)\s*"
    r"(?P<suffix>前|ago)\s*$",
    re.IGNORECASE,
)
_JUST_NOW_PATTERN = re.compile(
    r"^\s*(刚刚|刚(才)?|just\s*now)\s*$", re.IGNORECASE
)

# 「2024年5月1日」允许月/日不补零，且可带时分（可选秒）。
_CHINESE_DATE_PATTERN = re.compile(
    r"^\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日?\s*"
    r"(?:(\d{1,2})\s*[:：]\s*(\d{1,2})\s*(?:[:：]\s*(\d{1,2})\s*)?)?\s*$"
)

# 统一的无时区 UTC 存储格式（SQLAlchemy SQLite 方言写库时也是这个形态）。
_STORAGE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
_STORAGE_FORMAT_NO_MICROS = "%Y-%m-%d %H:%M:%S"

_RELATIVE_UNIT_MAP: dict[str, str] = {
    "个月": "month",
    "月": "month",
    "周": "week",
    "星期": "week",
    "天": "day",
    "日": "day",
    "小时": "hour",
    "时": "hour",
    "h": "hour",
    "hr": "hour",
    "hour": "hour",
    "hours": "hour",
    "d": "day",
    "day": "day",
    "days": "day",
    "m": "minute",
    "分": "minute",
    "分钟": "minute",
    "min": "minute",
    "minute": "minute",
    "minutes": "minute",
    "s": "second",
    "秒": "second",
    "秒钟": "second",
    "sec": "second",
    "second": "second",
    "seconds": "second",
}


def clean_datetime_text(value: Any) -> str:
    """清洗日期文本：NFKC 归一化全角字符，压缩并裁剪各类空白（含换行）。"""

    if value is None:
        return ""
    normalized = unicodedata.normalize("NFKC", str(value))
    lines = [
        re.sub(r"[ \t\u3000\xa0]+", " ", line).strip()
        for line in normalized.splitlines()
    ]
    return " ".join(line for line in lines if line)


def to_utc_naive(value: datetime) -> datetime:
    """把 ``datetime`` 转成无时区的 UTC 时间。

    带 tzinfo 的时间先换算到 UTC 再去掉时区；naive 时间按 UTC 约定直接保留。
    """

    if value.tzinfo is not None:
        value = value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def utc_storage_text(value: datetime) -> str:
    """生成与 SQLite 列内形态完全一致的 UTC 文本，用于 raw 留档等场景。"""

    naive = to_utc_naive(value)
    if naive.microsecond:
        return naive.strftime(_STORAGE_FORMAT)
    return naive.strftime(_STORAGE_FORMAT_NO_MICROS)


def utc_date_text(value: datetime) -> str:
    """返回 ``YYYY-MM-DD`` 形式的 UTC 日期，用于 raw 文件分目录。"""

    return to_utc_naive(value).strftime("%Y-%m-%d")


def _subtract_months(anchor: datetime, months: int) -> datetime:
    """按日历月回退，目标日超出当月天数时夹取到月末（如 3/31 回退一个月 → 2/29）。"""

    year = anchor.year
    month = anchor.month - months
    while month <= 0:
        month += 12
        year -= 1
    day = min(anchor.day, _days_in_month(year, month))
    return anchor.replace(year=year, month=month, day=day)


def _days_in_month(year: int, month: int) -> int:
    if month == 12:
        next_month = datetime(year + 1, 1, 1)
    else:
        next_month = datetime(year, month + 1, 1)
    return (next_month - timedelta(days=1)).day


def _parse_relative(text: str, anchor: datetime) -> datetime | None:
    if _JUST_NOW_PATTERN.match(text):
        return anchor

    match = _RELATIVE_PATTERN.match(text)
    if match is None:
        return None

    amount = int(match.group("value"))
    unit = _RELATIVE_UNIT_MAP.get(match.group("unit").lower())
    if unit is None or amount < 0:
        return None

    if unit == "month":
        return _subtract_months(anchor, amount)
    if unit == "week":
        return anchor - timedelta(weeks=amount)
    if unit == "day":
        return anchor - timedelta(days=amount)
    if unit == "hour":
        return anchor - timedelta(hours=amount)
    if unit == "minute":
        return anchor - timedelta(minutes=amount)
    return anchor - timedelta(seconds=amount)


def _parse_chinese_date(text: str) -> datetime | None:
    match = _CHINESE_DATE_PATTERN.match(text)
    if match is None:
        return None
    year, month, day = (int(match.group(i)) for i in (1, 2, 3))
    hour = int(match.group(4)) if match.group(4) else 0
    minute = int(match.group(5)) if match.group(5) else 0
    second = int(match.group(6)) if match.group(6) else 0
    try:
        return datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None


def parse_datetime(value: Any, *, anchor: datetime | None = None) -> datetime | None:
    """解析常见日期格式，无法识别时返回 ``None``（该字段允许缺失）。

    解析顺序：

    1. 已是 ``datetime`` 直接返回；
    2. ISO 8601（含 ``Z`` 与 ``+08:00``/``+0800`` 偏移，Python 3.11 原生支持）；
    3. 中文 ``2024年5月1日`` 等固定模板；
    4. 相对时间（``3天前``、``刚刚`` 等），以 ``anchor``（默认当前 UTC）为基准。

    输入文本会先经过 :func:`clean_datetime_text` 处理，全角数字/符号、首尾空白、
    换行都不会导致解析失败。返回值保留输入自身的时区信息（naive 输入得 naive，
    带偏移输入得 aware），UTC 归一化在入库阶段统一完成，保证解析规则单一可复现。
    """

    if value is None:
        return None
    if isinstance(value, datetime):
        return value

    text = clean_datetime_text(value)
    if not text:
        return None

    try:
        return datetime.fromisoformat(text)
    except ValueError:
        pass

    chinese = _parse_chinese_date(text)
    if chinese is not None:
        return chinese

    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    if anchor is not None:
        reference = to_utc_naive(anchor)
    else:
        reference = datetime.now(timezone.utc).replace(tzinfo=None)
    relative = _parse_relative(text, reference)
    if relative is not None:
        return relative

    return None
