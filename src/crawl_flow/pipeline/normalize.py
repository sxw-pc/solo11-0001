"""字段清洗与归一化：空白、日期、标签、HTML 正文。"""

from __future__ import annotations

import calendar
import re
import unicodedata
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any, Iterable, Mapping

from bs4 import BeautifulSoup

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.models import Record

TAG_SEPARATOR = re.compile(r"[,，、|;；]+")
SPACE_RUN = re.compile(r"[ \t　\xa0]+")

DATETIME_FORMATS: tuple[str, ...] = (
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
    "%Y年%m月%d日%H:%M:%S",
    "%Y年%m月%d日%H:%M",
    "%Y年%m月%d日 %H时%M分%S秒",
    "%Y年%m月%d日 %H时%M分",
    "%Y年%m月%d日%H时%M分%S秒",
    "%Y年%m月%d日%H时%M分",
    "%Y年%m月%d日",
    "%Y%m%d%H%M%S",
    "%Y%m%d",
)

# 显式 GMT/UTC 偏移后缀，如 ``GMT+08:00``、``UTC-5``、``GMT+0800``。
GMT_OFFSET = re.compile(
    r"\s*(?:GMT|UTC)\s*([+-])\s*(\d{1,2})(?::?(\d{2}))?\s*$",
    re.IGNORECASE,
)
# 不带偏移量的 GMT/UTC 后缀，语义等同 +00:00。
BARE_UTC_SUFFIX = re.compile(r"\s*(?:GMT|UTC)\s*$", re.IGNORECASE)

RELATIVE_ZH = re.compile(
    r"^(?P<amount>\d+)\s*个?"
    r"(?P<unit>秒钟|秒|分钟|分|钟头|小时|天|日|周|星期|个月|月|年)前$"
)
RELATIVE_EN = re.compile(
    r"^(?P<amount>\d+)\s+"
    r"(?P<unit>seconds?|minutes?|hours?|days?|weeks?|months?|years?)\s+ago$",
    re.IGNORECASE,
)
RELATIVE_NOW_ZH = frozenset({"刚刚", "刚才", "刚"})
RELATIVE_DAY_ZH: dict[str, int] = {
    "昨天": 1,
    "昨日": 1,
    "前天": 2,
    "前日": 2,
}

UNIT_SECONDS_ZH: dict[str, int] = {
    "秒": 1,
    "秒钟": 1,
    "分": 60,
    "分钟": 60,
    "小时": 3600,
    "钟头": 3600,
    "天": 86400,
    "日": 86400,
    "周": 7 * 86400,
    "星期": 7 * 86400,
}
UNIT_SECONDS_EN: dict[str, int] = {
    "second": 1,
    "minute": 60,
    "hour": 3600,
    "day": 86400,
    "week": 7 * 86400,
}
MONTH_UNITS_ZH = frozenset({"月", "个月"})
YEAR_UNITS_ZH = frozenset({"年"})


def clean_text(value: Any) -> str:
    """裁剪首尾空白并把连续空白压成单个空格。"""

    if value is None:
        return ""
    text = SPACE_RUN.sub(" ", str(value))
    lines = [line.strip() for line in text.splitlines()]
    return " ".join(line for line in lines if line)


def clean_multiline(value: Any) -> str:
    """保留段落换行：逐行裁剪、丢弃空行、压缩行内空白。"""

    if value is None:
        return ""
    normalized = str(value).replace("\r\n", "\n").replace("\r", "\n")
    lines = []
    for line in normalized.split("\n"):
        text = SPACE_RUN.sub(" ", line).strip()
        if text:
            lines.append(text)
    return "\n".join(lines)


BLOCK_TAGS = (
    "p",
    "div",
    "li",
    "tr",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "br",
    "blockquote",
)


def html_to_text(value: Any) -> str:
    """去掉 HTML 标签，保留段落边界。"""

    if value is None:
        return ""
    raw = str(value)
    if not raw.strip():
        return ""
    soup = BeautifulSoup(raw, "lxml")
    for element in soup(["script", "style"]):
        element.decompose()
    for element in soup.find_all(BLOCK_TAGS):
        element.insert_before("\n")
        element.insert_after("\n")
    return clean_multiline(soup.get_text(""))


def split_tags(value: Any) -> list[str]:
    """把列表或分隔字符串统一成去重后的标签列表。"""

    if value is None:
        return []
    if isinstance(value, str):
        candidates: Iterable[Any] = TAG_SEPARATOR.split(value)
    elif isinstance(value, (list, tuple, set)):
        candidates = value
    else:
        candidates = [value]

    tags: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        tag = clean_text(candidate)
        if not tag or tag in seen:
            continue
        seen.add(tag)
        tags.append(tag)
    return tags


def _preprocess_datetime_text(value: Any) -> str:
    """NFKC 归一化全角字符，再按通用空白规则裁剪、压平换行。"""

    normalized = unicodedata.normalize("NFKC", str(value))
    return clean_text(normalized)


def _shift_calendar(dt: datetime, *, months: int = 0, years: int = 0) -> datetime:
    """按自然月/年回退，目标月份天数不足时夹到月末（如 3 月 31 日减 1 个月）。"""

    month_index = dt.month - 1 + months + years * 12
    year = dt.year + month_index // 12
    month = month_index % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    return dt.replace(year=year, month=month, day=min(dt.day, last_day))


def _parse_relative(text: str, anchor: datetime) -> datetime | None:
    """解析「3 天前」「刚刚」「昨天」「3 days ago」等相对时间。

    全部以 ``anchor``（管道传入本次抓取的 ``fetched_at``）为基准，
    同一输入在同一锚点下结果稳定，不依赖调用时刻的系统时钟。
    """

    if text in RELATIVE_NOW_ZH:
        return anchor
    day_offset = RELATIVE_DAY_ZH.get(text)
    if day_offset is not None:
        return anchor - timedelta(days=day_offset)

    match = RELATIVE_ZH.match(text)
    if match is not None:
        amount = int(match.group("amount"))
        unit = match.group("unit").removeprefix("个")
        seconds = UNIT_SECONDS_ZH.get(unit)
        if seconds is not None:
            return anchor - timedelta(seconds=seconds * amount)
        if unit in MONTH_UNITS_ZH:
            return _shift_calendar(anchor, months=-amount)
        if unit in YEAR_UNITS_ZH:
            return _shift_calendar(anchor, years=-amount)
        return None

    match = RELATIVE_EN.match(text)
    if match is not None:
        amount = int(match.group("amount"))
        unit = match.group("unit").rstrip("s").lower()
        seconds = UNIT_SECONDS_EN.get(unit)
        if seconds is not None:
            return anchor - timedelta(seconds=seconds * amount)
        if unit == "month":
            return _shift_calendar(anchor, months=-amount)
        if unit == "year":
            return _shift_calendar(anchor, years=-amount)
    return None


def _rewrite_gmt_offset(match: re.Match[str]) -> str:
    sign = match.group(1)
    hours = int(match.group(2))
    minutes = int(match.group(3) or 0)
    return f"{sign}{hours:02d}:{minutes:02d}"


# 中文日期分隔符两侧的空白，如 ``2024 年 03 月 02 日 10 : 20`` → 紧凑形式。
CHINESE_DATE_PUNCT = re.compile(r"\s*(年|月|日|时|分|秒)\s*")


def parse_datetime(value: Any, *, now: datetime | None = None) -> datetime | None:
    """把可识别的日期文本统一解析为 :class:`datetime`，无法识别返回 ``None``。

    解析链路：

    1. NFKC 归一化全角字符并清洗首尾空白、换行；
    2. 相对时间（``3 天前``/``刚刚``/``昨天``/``3 days ago``）以 ``now``
       为锚点，管道调用时传入本次 ``fetched_at``，保证重复采集可复现；
    3. ISO 8601（含 ``Z``、``GMT/UTC±HH:MM`` 后缀）；
    4. 中文年月日时分、斜杠、点号、紧凑数字等固定格式；
    5. RFC 2822（如 ``Wed, 02 Oct 2024 13:00:00 +0800``）兜底。

    带时区信息的结果保留源偏移，由落盘层（``storage.db``）统一换算成 UTC
    墙钟时间入库；无时区信息的结果视为 UTC 墙钟时间。解析纯函数化，同一
    输入要么稳定得到值，要么稳定得到 ``None``，不会这次成功下次失败。
    """

    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = _preprocess_datetime_text(value)
    if not text:
        return None

    anchor = now if now is not None else datetime.now().astimezone()
    relative = _parse_relative(text, anchor)
    if relative is not None:
        return relative

    iso_candidate = BARE_UTC_SUFFIX.sub("", GMT_OFFSET.sub(_rewrite_gmt_offset, text))
    try:
        return datetime.fromisoformat(iso_candidate)
    except ValueError:
        pass

    # 兼容中文分隔符两侧带空格的写法，如 ``2024 年 03 月 02 日 10 时 20 分``。
    compact_cn = CHINESE_DATE_PUNCT.sub(r"\1", text)
    for candidate in (text, compact_cn):
        for fmt in DATETIME_FORMATS:
            try:
                return datetime.strptime(candidate, fmt)
            except ValueError:
                continue

    try:
        return parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None


def normalize_record(
    source: SourceConfig,
    external_id: str,
    fields: Mapping[str, Any],
    *,
    detail_url: str,
    fetched_at: datetime,
) -> Record:
    """把适配器返回的源字段字典转成 :class:`Record`。

    ``field_map`` 决定每个标准字段从哪个源键取值；标题缺失视为解析失败。
    """

    mapping = source.field_map
    title = clean_text(fields.get(mapping.title))
    if not title:
        raise ParseError(source.name, external_id, "标题为空")

    category_value = clean_text(fields.get(mapping.category)) if mapping.category else ""
    source_url = clean_text(fields.get(mapping.url)) if mapping.url else ""
    canonical_url = source_url or detail_url

    return Record(
        source=source.name,
        external_id=external_id,
        title=title,
        category=category_value or None,
        tags=split_tags(fields.get(mapping.tags)) if mapping.tags else [],
        published_at=parse_datetime(fields.get(mapping.published_at), now=fetched_at)
        if mapping.published_at
        else None,
        content=html_to_text(fields.get(mapping.content)) if mapping.content else "",
        url=canonical_url,
        fetched_at=fetched_at,
    )
