"""字段清洗与归一化：空白、日期、标签、HTML 正文。"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterable, Mapping

from bs4 import BeautifulSoup

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.models import Record
from crawl_flow.timeutils import parse_datetime as _parse_datetime

TAG_SEPARATOR = re.compile(r"[,，、|;；]+")
SPACE_RUN = re.compile(r"[ \t　 ]+")


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


def parse_datetime(value: Any, *, anchor: datetime | None = None) -> datetime | None:
    """解析常见日期格式，无法识别时返回 ``None``（该字段允许缺失）。

    兼容 ISO 8601（含 ``Z`` 与时区偏移）、中文日期（``2024年5月1日``）、
    相对时间（``3天前``、``刚刚``）以及全角字符/首尾空白/换行混杂的输入。
    相对时间以 ``anchor`` 为基准；归一化流程固定传入本条记录的 ``fetched_at``，
    保证重复采集对同一输入得到完全一致的结果。
    """

    return _parse_datetime(value, anchor=anchor)


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
    日期字段先经全角/空白清洗再解析，相对时间以本条记录的 ``fetched_at``
    为锚点；UTC 归一化在入库阶段统一完成。
    """

    mapping = source.field_map
    title = clean_text(fields.get(mapping.title))
    if not title:
        raise ParseError(source.name, external_id, "标题为空")

    category_value = clean_text(fields.get(mapping.category)) if mapping.category else ""
    source_url = clean_text(fields.get(mapping.url)) if mapping.url else ""
    canonical_url = source_url or detail_url

    if mapping.published_at:
        raw_published_at = fields.get(mapping.published_at)
        published_at = parse_datetime(raw_published_at, anchor=fetched_at)
    else:
        published_at = None

    return Record(
        source=source.name,
        external_id=external_id,
        title=title,
        category=category_value or None,
        tags=split_tags(fields.get(mapping.tags)) if mapping.tags else [],
        published_at=published_at,
        content=html_to_text(fields.get(mapping.content)) if mapping.content else "",
        url=canonical_url,
        fetched_at=fetched_at,
    )
