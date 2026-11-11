"""跨源关联：预加载关联源索引，把多源同实体记录合并为统一宽表。

设计要点：

- 默认关闭：只有 ``SourceConfig.correlation`` 显式声明且 ``enabled=True`` 时，
  ``run_source`` 才会构建 :class:`Correlator`；未配置的源逻辑完全不变。
- 预加载索引：关联源记录在一轮采集开始时一次性读入内存并按键建索引，
  后续每条本地记录只做内存查找，不逐行查库。
- 异常隔离：关联键缺失、跨源 ID 类型不匹配、多命中冲突、关联自身报错都
  不会中断主采集流程；原始单源记录照常写入 ``records``，失败情况写入
  ``correlation_events`` 留痕。
- 无冗余存储：宽表与事件均按唯一键幂等 upsert，重复采集不产生新行。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from crawl_flow.config import CorrelationConfig, JoinKey, WideFieldRule
from crawl_flow.models import Record
from crawl_flow.pipeline.normalize import parse_datetime
from crawl_flow.storage.db import Database

LOGGER_NAME = "crawl_flow.correlation"

# 关联事件类型
EVENT_MISSING_FIELD = "missing_field"
EVENT_TYPE_MISMATCH = "type_mismatch"
EVENT_NOT_MATCHED = "not_matched"
EVENT_CONFLICT = "conflict"
EVENT_ERROR = "error"

# 关联结果（写入宽表时的状态）
STATUS_OK = "ok"
STATUS_CONFLICT = "conflict"


@dataclass(frozen=True)
class CorrelationResult:
    """单条本地记录的关联结果。"""

    status: str
    wide_written: bool
    event_type: str | None = None
    message: str = ""


def coerce_join_value(value: Any, mode: str) -> str | None:
    """把关联键值按配置模式归一化为可比较的字符串。

    - 空值（None / 空白字符串）返回 ``None``，调用方按字段缺失处理。
    - ``string``：转字符串并裁剪；bool 不接受（避免 True/False 误匹配）。
    - ``integer``：必须可无损转整数，否则抛 :class:`ValueError`（类型不匹配）。
    - ``datetime``：必须可解析为时间，统一成 ISO 形式，否则抛 ``ValueError``。
    """

    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"布尔值不能作为关联键: {value!r}")
    if mode == "string":
        if isinstance(value, datetime):
            return value.isoformat()
        text = str(value).strip()
        return text or None
    if mode == "integer":
        if isinstance(value, int):
            return str(value)
        text = str(value).strip()
        if not text:
            return None
        try:
            return str(int(text))
        except ValueError as exc:
            raise ValueError(f"关联键无法转为整数: {value!r}") from exc
    if mode == "datetime":
        if isinstance(value, datetime):
            return value.isoformat()
        text = str(value).strip()
        if not text:
            return None
        parsed = parse_datetime(text)
        if parsed is None:
            raise ValueError(f"关联键无法解析为时间: {value!r}")
        return parsed.isoformat()
    raise ValueError(f"未知的关联键归一化模式: {mode}")


class Correlator:
    """执行一个源对单个关联源的宽表合并。"""

    def __init__(
        self,
        source_name: str,
        config: CorrelationConfig,
        db: Database,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.source_name = source_name
        self.config = config
        self.db = db
        self.log = logger or logging.getLogger(LOGGER_NAME)
        # key 为各 join key 归一化值组成的元组，value 为命中的远端记录列表
        self._index: dict[tuple[str, ...], list[Record]] = {}
        self._preloaded = False

    # ---- 预加载 ----
    def preload(self) -> int:
        """一次性加载关联源全部记录并建立内存索引，返回索引条数。"""

        records = self.db.load_records(self.config.remote_source)
        index: dict[tuple[str, ...], list[Record]] = {}
        for record in records:
            key = self._build_key(record, local=False)
            if key is None:
                # 远端记录关联键缺失：跳过索引并留痕，不影响其他记录
                self._safe_emit(
                    record,
                    EVENT_MISSING_FIELD,
                    {"reason": "关联源记录的关联键为空，已跳过索引"},
                )
                continue
            index.setdefault(key, []).append(record)
        self._index = index
        self._preloaded = True
        self.log.info(
            "correlation index preloaded local_source=%s remote_source=%s entries=%d",
            self.source_name,
            self.config.remote_source,
            len(records),
        )
        return len(records)

    # ---- 主入口 ----
    def on_record(self, record: Record) -> CorrelationResult:
        """处理一条刚入库的本地记录，返回关联结果；任何异常都不外抛。"""

        try:
            return self._process(record)
        except Exception as exc:  # 关联是旁路能力，绝不中断主采集流程
            self.log.warning(
                "correlation error source=%s external_id=%s error=%s",
                self.source_name,
                record.external_id,
                exc,
            )
            self._safe_emit(
                record,
                EVENT_ERROR,
                {"reason": str(exc)[:500]},
            )
            return CorrelationResult(
                status=EVENT_ERROR,
                wide_written=False,
                event_type=EVENT_ERROR,
                message=str(exc),
            )

    def _process(self, record: Record) -> CorrelationResult:
        if not self._preloaded:
            self.preload()

        try:
            local_key = self._build_key(record, local=True)
        except ValueError as exc:
            return self._fail_event(
                record,
                EVENT_TYPE_MISMATCH,
                f"关联键类型不匹配: {exc}",
            )
        if local_key is None:
            return self._fail_event(
                record,
                EVENT_MISSING_FIELD,
                "关联键字段缺失或为空，保留原始单源数据",
            )

        matches = self._index.get(local_key, [])

        if not matches:
            return self._fail_event(
                record,
                EVENT_NOT_MATCHED,
                f"关联源 {self.config.remote_source} 中无匹配记录",
            )

        if len(matches) > 1:
            return self._handle_conflict(record, matches, local_key)

        return self._write_wide(record, matches, STATUS_OK, None)

    # ---- 冲突处理 ----
    def _handle_conflict(
        self, record: Record, matches: list[Record], key: tuple[str, ...]
    ) -> CorrelationResult:
        remote_ids = [item.external_id for item in matches]
        detail = {
            "reason": "关联键命中多条远端记录",
            "join_key": list(key),
            "remote_external_ids": remote_ids,
            "policy": self.config.wide_table.conflict_policy,
        }
        self._safe_emit(record, EVENT_CONFLICT, detail)

        if self.config.wide_table.conflict_policy == "skip":
            self.log.warning(
                "correlation conflict skipped source=%s external_id=%s remote_ids=%s",
                self.source_name,
                record.external_id,
                remote_ids,
            )
            return CorrelationResult(
                status=EVENT_CONFLICT,
                wide_written=False,
                event_type=EVENT_CONFLICT,
                message="多命中且冲突策略为 skip，未生成宽表行",
            )

        return self._write_wide(record, matches, STATUS_CONFLICT, detail)

    # ---- 宽表生成 ----
    def _write_wide(
        self,
        record: Record,
        matches: list[Record],
        status: str,
        conflict_detail: dict[str, Any] | None,
    ) -> CorrelationResult:
        participants = [record, *matches]
        by_source: dict[str, list[Record]] = {}
        for participant in participants:
            by_source.setdefault(participant.source, []).append(participant)

        fields: dict[str, Any] = {}
        for rule in self.config.wide_table.fields:
            fields[rule.name] = self._build_field(rule, by_source)

        entity_key = self._entity_key(record)
        source_names: list[str] = []
        for participant in participants:
            if participant.source not in source_names:
                source_names.append(participant.source)

        self.db.upsert_wide_record(
            table_name=self.config.wide_table.table,
            entity_key=entity_key,
            status=status,
            primary_source=self.source_name,
            sources=source_names,
            fields=fields,
            conflict_detail=conflict_detail,
            fetched_at=record.fetched_at or datetime.now(timezone.utc),
        )
        return CorrelationResult(
            status=status,
            wide_written=True,
            message="宽表已更新" if status == STATUS_OK else "冲突已按 merge 策略合并",
        )

    def _build_field(
        self,
        rule: WideFieldRule,
        by_source: dict[str, list[Record]],
    ) -> Any:
        field_name = rule.field or rule.name
        values: list[Any] = []
        for source_name in rule.sources:
            for participant in by_source.get(source_name, []):
                values.append(getattr(participant, field_name, None))

        if rule.strategy == "first":
            for value in values:
                if not _is_empty(value):
                    return _json_safe(value)
            return rule.default

        if rule.strategy == "list":
            collected: list[Any] = []
            for value in values:
                if _is_empty(value):
                    continue
                if isinstance(value, (list, tuple, set)):
                    for item in value:
                        if not _is_empty(item) and item not in collected:
                            collected.append(_json_safe(item))
                elif value not in collected:
                    collected.append(_json_safe(value))
            return collected

        # concat：标量拼接；列表摊平后去重，再按 separator 连接
        concat_parts: list[str] = []
        for value in values:
            if _is_empty(value):
                continue
            if isinstance(value, (list, tuple, set)):
                for item in value:
                    text = "" if item is None else str(item).strip()
                    if text and text not in concat_parts:
                        concat_parts.append(text)
            else:
                text = str(value).strip()
                if text and text not in concat_parts:
                    concat_parts.append(text)
        if not concat_parts:
            return rule.default
        return rule.separator.join(concat_parts)

    # ---- 键与事件 ----
    def _build_key(self, record: Record, *, local: bool) -> tuple[str, ...] | None:
        parts: list[str] = []
        for join_key in self.config.join_keys:
            field_name = join_key.local_field if local else join_key.remote_field
            raw_value = getattr(record, field_name, None)
            try:
                normalized = coerce_join_value(raw_value, join_key.coerce)
            except ValueError:
                if local:
                    # 本地记录类型不匹配：直接交给上层留痕
                    raise
                # 远端记录类型不匹配：记事件并跳过该条索引
                self._safe_emit(
                    record,
                    EVENT_TYPE_MISMATCH,
                    {
                        "field": field_name,
                        "value": repr(raw_value),
                        "coerce": join_key.coerce,
                    },
                )
                return None
            if normalized is None:
                return None
            parts.append(normalized)
        return tuple(parts)

    def _entity_key(self, record: Record) -> str:
        return f"{self.source_name}:{record.external_id}"

    def _fail_event(
        self, record: Record, event_type: str, message: str
    ) -> CorrelationResult:
        self._safe_emit(record, event_type, {"reason": message})
        return CorrelationResult(
            status=event_type,
            wide_written=False,
            event_type=event_type,
            message=message,
        )

    def _safe_emit(
        self, record: Record, event_type: str, detail: dict[str, Any]
    ) -> None:
        try:
            self._emit_event(record, event_type, detail)
        except Exception as exc:  # 留痕失败也不能影响主流程
            self.log.warning("correlation event write failed: %s", exc)

    def _emit_event(
        self, record: Record, event_type: str, detail: dict[str, Any]
    ) -> None:
        self.db.upsert_correlation_event(
            table_name=self.config.wide_table.table,
            local_source=record.source,
            local_external_id=record.external_id,
            remote_source=self.config.remote_source,
            event_type=event_type,
            detail=detail,
        )


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _json_safe(value: Any) -> Any:
    """把 datetime 等非 JSON 原生类型转成可存入 JSON 列的形式。"""

    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value
