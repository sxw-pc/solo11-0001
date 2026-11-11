"""字段清洗与归一化入口。

历史行为（硬编码默认清洗）完整保留，现有调用方与测试无需改动：

- :func:`normalize_record`：字段映射 + 默认清洗，行为与规则引擎出现之前一致。
- :func:`normalize_record_with_rules`：字段映射后交给规则引擎，按配置执行
  清洗规则链、质量校验与评分；规则缺失/异常时自动回退默认清洗。

清洗原语统一维护在 :mod:`crawl_flow.cleaning`，这里重新导出以兼容
``from crawl_flow.pipeline.normalize import clean_text`` 等既有导入。
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Mapping

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.models import Record
from crawl_flow.cleaning import (
    DATETIME_FORMATS,
    clean_multiline,
    clean_text,
    html_to_text,
    parse_datetime,
    split_tags,
)

if TYPE_CHECKING:
    from crawl_flow.rules.engine import ProcessResult, RuleEngine

__all__ = [
    "DATETIME_FORMATS",
    "build_record",
    "clean_multiline",
    "clean_text",
    "extract_raw_values",
    "html_to_text",
    "normalize_record",
    "normalize_record_with_rules",
    "parse_datetime",
    "split_tags",
]


def extract_raw_values(
    source: SourceConfig,
    fields: Mapping[str, Any],
    *,
    detail_url: str,
) -> dict[str, Any]:
    """按 ``field_map`` 取出六个标准字段的原始值（未做任何清洗）。"""

    mapping = source.field_map
    source_url = clean_text(fields.get(mapping.url)) if mapping.url else ""
    return {
        "title": fields.get(mapping.title),
        "category": fields.get(mapping.category) if mapping.category else None,
        "tags": fields.get(mapping.tags) if mapping.tags else None,
        "published_at": fields.get(mapping.published_at) if mapping.published_at else None,
        "content": fields.get(mapping.content) if mapping.content else None,
        "url": source_url or detail_url,
    }


def normalize_record(
    source: SourceConfig,
    external_id: str,
    fields: Mapping[str, Any],
    *,
    detail_url: str,
    fetched_at: datetime,
) -> Record:
    """把适配器返回的源字段字典转成 :class:`Record`（默认清洗路径）。

    ``field_map`` 决定每个标准字段从哪个源键取值；标题缺失视为解析失败。
    """

    mapping = source.field_map
    title = clean_text(fields.get(mapping.title))
    if not title:
        raise ParseError(source.name, external_id, "标题为空")

    category_value = clean_text(fields.get(mapping.category)) if mapping.category else ""

    return Record(
        source=source.name,
        external_id=external_id,
        title=title,
        category=category_value or None,
        tags=split_tags(fields.get(mapping.tags)) if mapping.tags else [],
        published_at=parse_datetime(fields.get(mapping.published_at))
        if mapping.published_at
        else None,
        content=html_to_text(fields.get(mapping.content)) if mapping.content else "",
        url=extract_raw_values(source, fields, detail_url=detail_url)["url"],
        fetched_at=fetched_at,
    )


def normalize_record_with_rules(
    source: SourceConfig,
    external_id: str,
    fields: Mapping[str, Any],
    *,
    detail_url: str,
    fetched_at: datetime,
    engine: "RuleEngine",
) -> tuple[Record, "ProcessResult"]:
    """字段映射后交由规则引擎处理，返回 ``(Record, 引擎处理结果)``。

    标题缺失（含规则处理后为空字符串）仍按既有约定抛 :class:`ParseError`，
    保证采集主流程的失败语义不变。
    """

    raw_values = extract_raw_values(source, fields, detail_url=detail_url)
    result = engine.process(source.name, external_id, raw_values)
    record = build_record(
        source.name,
        external_id,
        result,
        fetched_at=fetched_at,
    )
    if not record.title:
        raise ParseError(source.name, external_id, "标题为空")
    return record, result


def build_record(
    source_name: str,
    external_id: str,
    result: "ProcessResult",
    *,
    fetched_at: datetime,
) -> Record:
    """把引擎输出值组装为 :class:`Record`，并附带质量报告。"""

    values = result.values
    category = values.get("category")
    quality_report = {
        "quality_score": result.quality_score,
        "low_quality": result.low_quality,
        "action": result.action,
        "threshold": result.threshold,
        "field_scores": result.field_scores,
        "validation_failures": result.validation_failures,
        "degraded_fields": result.degraded_fields,
        "trace": result.trace_as_dicts(),
    }
    return Record(
        source=source_name,
        external_id=external_id,
        title=values.get("title", ""),
        category=(clean_text(category) or None) if category is not None else None,
        tags=list(values.get("tags") or []),
        published_at=values.get("published_at"),
        content=values.get("content", ""),
        url=values.get("url", ""),
        fetched_at=fetched_at,
        quality_score=result.quality_score,
        low_quality=result.low_quality,
        quality_report=quality_report,
    )
