"""dry-run 差异对比：把本次采集结果与存量记录做只读比对。

设计要点：

- **只读**：对比只通过 :meth:`Database.fetch_diff_snapshot` 进行批量查询，
  不产生任何 raw 文件或数据库写入。
- **批量**：按 ``source`` 一次性取存量 ID，按本次抓取 ID 分块 ``IN`` 查询
  对比字段，避免逐条查询数据库。
- **容错**：存量缺失、字段类型不匹配、JSON 标签无序、``None`` / 空串
  都做了兼容，单个字段无法比较时归入「异常」而不是中断整轮。
- **脱敏**：变更摘要只输出字段名与安全描述；``content`` 只暴露长度变化，
  绝不打印正文内容。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Mapping, Sequence

from crawl_flow.models import Record
from crawl_flow.pipeline.normalize import clean_multiline, clean_text, parse_datetime, split_tags

# 统计状态：新增 / 修改 / 无变化 / 异常。
STATUS_ADDED = "added"
STATUS_MODIFIED = "modified"
STATUS_UNCHANGED = "unchanged"
STATUS_ANOMALY = "anomaly"

REASON_STORED_MISSING = "stored_missing"
REASON_COMPARE_ERROR = "compare_error"

SUMMARY_MAX_LENGTH = 60


@dataclass(frozen=True)
class FieldChange:
    """单个字段的变更摘要（不含完整正文）。"""

    field: str
    old: str
    new: str

    def render(self) -> str:
        return f"{self.field}({self.old}→{self.new})"


@dataclass(frozen=True)
class DiffEntry:
    """一条记录的对比结果；``unchanged`` 不进入明细以控制内存占用。"""

    external_id: str
    status: str
    changes: tuple[FieldChange, ...] = ()
    reason_code: str | None = None
    reason: str | None = None


@dataclass
class DiffReport:
    """一个源的差异统计结果。"""

    source: str
    fields: list[str]
    added: int = 0
    modified: int = 0
    unchanged: int = 0
    anomaly: int = 0
    entries: list[DiffEntry] = field(default_factory=list)
    field_changes: dict[str, int] = field(default_factory=dict)
    elapsed_ms: int = 0

    @property
    def compared(self) -> int:
        """参与逐字段比较的记录数（新增记录没有存量基线）。"""

        return self.modified + self.unchanged + self._compared_anomalies

    @property
    def _compared_anomalies(self) -> int:
        return sum(
            1
            for entry in self.entries
            if entry.status == STATUS_ANOMALY and entry.reason_code == REASON_COMPARE_ERROR
        )

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.modified or self.anomaly)


def compare_snapshot(
    *,
    source: str,
    fetched: Sequence[Record],
    stored: Mapping[str, Mapping[str, Any]],
    stored_ids: Iterable[str],
    observed_ids: Iterable[str],
    fields: Sequence[str],
    elapsed_ms: int = 0,
) -> DiffReport:
    """对比本次抓取结果与存量快照。

    ``observed_ids`` 是本轮列表页出现过的全部 ID（含超出 ``--limit`` 被跳过
    的条目），因此有意图的限量采集不会被误判成「存量缺失」。
    """

    field_list = list(fields)
    stored_values = {str(key): value for key, value in stored.items()}
    stored_set = {str(item) for item in stored_ids}
    observed_set = {str(item) for item in observed_ids}

    report = DiffReport(source=source, fields=field_list, elapsed_ms=elapsed_ms)

    for record in fetched:
        external_id = record.external_id
        old_values = stored_values.get(external_id)
        if old_values is None:
            report.added += 1
            report.entries.append(DiffEntry(external_id=external_id, status=STATUS_ADDED))
            continue

        changes, errors = _compare_fields(field_list, record, old_values)
        if errors:
            report.anomaly += 1
            report.entries.append(
                DiffEntry(
                    external_id=external_id,
                    status=STATUS_ANOMALY,
                    changes=tuple(changes),
                    reason_code=REASON_COMPARE_ERROR,
                    reason="; ".join(errors),
                )
            )
            for change in changes:
                report.field_changes[change.field] = report.field_changes.get(change.field, 0) + 1
            continue

        if changes:
            report.modified += 1
            report.entries.append(
                DiffEntry(
                    external_id=external_id,
                    status=STATUS_MODIFIED,
                    changes=tuple(changes),
                )
            )
            for change in changes:
                report.field_changes[change.field] = report.field_changes.get(change.field, 0) + 1
        else:
            report.unchanged += 1

    # 存量有、本次列表未出现：作为「存量缺失」异常报告，不删除、不写入。
    for missing_id in sorted(stored_set - observed_set):
        report.anomaly += 1
        report.entries.append(
            DiffEntry(
                external_id=missing_id,
                status=STATUS_ANOMALY,
                reason_code=REASON_STORED_MISSING,
                reason="存量记录缺失（本次列表未出现）",
            )
        )

    return report


def _compare_fields(
    fields: Sequence[str],
    record: Record,
    old_values: Mapping[str, Any],
) -> tuple[list[FieldChange], list[str]]:
    changes: list[FieldChange] = []
    errors: list[str] = []

    for name in fields:
        old_value, old_error = _canonical_value(name, old_values.get(name))
        if old_error is not None:
            errors.append(f"{name}: 存量{old_error}")
            continue
        new_value, new_error = _canonical_value(name, getattr(record, name, None))
        if new_error is not None:
            errors.append(f"{name}: 采集{new_error}")
            continue
        if old_value != new_value:
            changes.append(
                FieldChange(
                    field=name,
                    old=_describe_value(name, old_value),
                    new=_describe_value(name, new_value),
                )
            )
    return changes, errors


def _canonical_value(name: str, value: Any) -> tuple[Any, str | None]:
    """把字段值转成可安全比较的规范形式。

    返回 ``(规范值, 错误原因)``；错误原因非空表示该字段无法可靠比较。
    """

    if name == "tags":
        return _canonical_tags(value)
    if name == "published_at":
        return _canonical_datetime(value)
    if name == "content":
        return _canonical_content(value)
    if name in {"title", "category", "url"}:
        return _canonical_text(value)
    return None, f"不支持的字段类型 {name!r}"


def _canonical_content(value: Any) -> tuple[Any, str | None]:
    # 正文按入库语义（保留段落换行）规范化；空值与纯空白视为同一种「空」。
    if value is None:
        return "", None
    if isinstance(value, bool):
        return None, "类型不匹配(bool)"
    if isinstance(value, (int, float)):
        return clean_multiline(value), None
    if isinstance(value, str):
        return clean_multiline(value), None
    return None, f"类型不匹配({type(value).__name__})"


def _canonical_text(value: Any) -> tuple[Any, str | None]:
    # 空值兼容：None、空串、纯空白都视为同一种「空」。
    if value is None:
        return "", None
    if isinstance(value, bool):
        return None, "类型不匹配(bool)"
    if isinstance(value, (int, float)):
        return clean_text(value), None
    if isinstance(value, datetime):
        return value.isoformat(), None
    if isinstance(value, str):
        return clean_text(value), None
    return None, f"类型不匹配({type(value).__name__})"


def _canonical_tags(value: Any) -> tuple[Any, str | None]:
    if value is None:
        return (), None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return (), None
        if text.startswith("["):
            try:
                loaded = json.loads(text)
            except json.JSONDecodeError:
                return None, "JSON 标签无法解析"
            return _canonical_tags(loaded)
        # 兼容旧数据：以分隔字符串形式存下来的标签。
        return tuple(sorted(set(split_tags(text)))), None
    if isinstance(value, (list, tuple, set)):
        tags: list[str] = []
        for item in value:
            if item is None:
                continue
            if isinstance(item, bool) or not isinstance(item, (str, int, float)):
                return None, f"标签元素类型不匹配({type(item).__name__})"
            text = clean_text(item)
            if text:
                tags.append(text)
        # JSON 标签视为无序集合，避免仅顺序变化造成伪变更。
        return tuple(sorted(set(tags))), None
    return None, f"类型不匹配({type(value).__name__})"


def _canonical_datetime(value: Any) -> tuple[Any, str | None]:
    if value is None:
        return None, None
    if isinstance(value, datetime):
        # SQLite DATETIME 不保留时区，统一去掉 tzinfo 以匹配落库语义。
        return value.replace(tzinfo=None), None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None, None
        parsed = parse_datetime(text)
        if parsed is None:
            return None, "发布时间无法解析"
        return parsed.replace(tzinfo=None), None
    return None, f"类型不匹配({type(value).__name__})"


def _describe_value(name: str, value: Any) -> str:
    """生成安全的变更摘要：正文只显示长度，其他字段截断展示。"""

    if name == "content":
        return f"长度 {len(value or '')}"
    if name == "published_at":
        if value is None:
            return "(空)"
        return value.isoformat() if isinstance(value, datetime) else str(value)
    if name == "tags":
        rendered = ",".join(value)
        return _truncate(f"[{rendered}]")
    if not isinstance(value, str):
        value = str(value)
    if not value:
        return "(空)"
    return _truncate(value)


def _truncate(text: str) -> str:
    if len(text) <= SUMMARY_MAX_LENGTH:
        return text
    return f"{text[:SUMMARY_MAX_LENGTH]}…"
