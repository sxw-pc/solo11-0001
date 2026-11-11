"""默认字段清洗原语：空白、多行文本、HTML、标签拆分、日期解析。

这些函数是历史硬编码清洗逻辑的载体，同时作为规则引擎的**默认降级路径**，
独立成模块以避免 ``pipeline.normalize`` 与 ``rules.engine`` 之间的循环导入。
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Iterable

from bs4 import BeautifulSoup

TAG_SEPARATOR = re.compile(r"[,，、|;；]+")
SPACE_RUN = re.compile(r"[ \t　 ]+")
DATETIME_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d",
    "%Y.%m.%d",
    "%Y年%m月%d日 %H:%M:%S",
    "%Y年%m月%d日 %H:%M",
    "%Y年%m月%d日",
)


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


def parse_datetime(value: Any) -> datetime | None:
    """解析常见日期格式，无法识别时返回 ``None``（该字段允许缺失）。"""

    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    text = clean_text(value)
    if not text:
        return None

    iso_candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        return datetime.fromisoformat(iso_candidate)
    except ValueError:
        pass

    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None
