"""字段清洗与归一化：空白、日期、标签、HTML 正文。"""

from __future__ import annotations

import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterable, Mapping

from bs4 import BeautifulSoup

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.models import Record

if TYPE_CHECKING:
    from crawl_flow.rules.engine import QualityReport, RuleEngine

TAG_SEPARATOR = re.compile(r"[,，、|;；]+")
SPACE_RUN = re.compile(r"[ \t\u3000\xa0]+")

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


def normalize_record(
    source: SourceConfig,
    external_id: str,
    fields: Mapping[str, Any],
    *,
    detail_url: str,
    fetched_at: datetime,
    rule_engine: "RuleEngine | None" = None,
    report: "QualityReport | None" = None,
) -> Record:
    """把适配器返回的源字段字典转成 :class:`Record`。

    ``field_map`` 决定每个标准字段从哪个源键取值；标题缺失视为解析失败。

    处理顺序：field_map 提取 → 规则引擎清洗（可选）→ 默认清洗（始终执行，
    即规则异常时的降级基线）→ 规则引擎校验与评分（可选）。启用规则引擎时，
    质量分与标记会写回 ``record.quality_score`` / ``record.quality_flags``，
    处置动作体现在 ``report.action``。
    """

    mapping = source.field_map
    extracted: dict[str, Any] = {
        "title": fields.get(mapping.title),
        "category": fields.get(mapping.category) if mapping.category else None,
        "tags": fields.get(mapping.tags) if mapping.tags else None,
        "published_at": fields.get(mapping.published_at) if mapping.published_at else None,
        "content": fields.get(mapping.content) if mapping.content else None,
        "url": fields.get(mapping.url) if mapping.url else None,
    }

    use_rules = rule_engine is not None and report is not None
    if use_rules:
        extracted = rule_engine.clean_fields(extracted, report)

    title = clean_text(extracted["title"])
    if not title:
        raise ParseError(source.name, external_id, "标题为空")

    category_value = clean_text(extracted["category"])
    source_url = clean_text(extracted["url"])
    canonical_url = source_url or detail_url

    record = Record(
        source=source.name,
        external_id=external_id,
        title=title,
        category=category_value or None,
        tags=split_tags(extracted["tags"]) if mapping.tags else [],
        published_at=parse_datetime(extracted["published_at"])
        if mapping.published_at
        else None,
        content=html_to_text(extracted["content"]) if mapping.content else "",
        url=canonical_url,
        fetched_at=fetched_at,
    )

    if use_rules:
        rule_engine.validate_record(
            {
                "title": record.title,
                "category": record.category,
                "tags": record.tags,
                "published_at": record.published_at,
                "content": record.content,
                "url": record.url,
            },
            report,
        )
        rule_engine.finalize(report)
        record.quality_score = report.score
        record.quality_flags = list(report.flags)

    return record
