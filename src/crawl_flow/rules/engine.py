"""规则引擎：配置加载、编译合并、规则执行与字段质量评分。

核心类型：

- :func:`load_rules`：加载并**启动期校验**规则配置，任何语法/参数错误汇总为
  :class:`RuleConfigError`；配置文件不存在时返回内置空引擎（自动降级）。
- :class:`RuleEngine`：按来源处理一条记录的全部标准字段，输出清洗结果、
  质量评分、校验结论与逐字段处理轨迹（trace）。

引擎只依赖标准库与项目内的默认清洗函数；规则编译（正则/格式预解析）只在
加载期执行一次，记录处理路径上没有 YAML 解析或动态编译，保证低延迟。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from crawl_flow.cleaning import (
    clean_multiline,
    clean_text,
    html_to_text,
    parse_datetime,
    split_tags,
)
from crawl_flow.rules.errors import (
    RuleConfigError,
    RuleExecutionError,
    ValidationFailure,
)
from crawl_flow.rules.operators import (
    CleanFn,
    ValidateFn,
    compile_operator,
    operator_names,
)
from crawl_flow.rules.schema import (
    FieldRuleSet,
    QualityConfig,
    RulesConfig,
    STANDARD_FIELDS,
)

LOGGER_NAME = "crawl_flow.rules"
#: 单条 trace 中 before/after 值的最大记录长度，超长截断并加省略号。
TRACE_VALUE_LIMIT = 500

#: 字段 → 默认值形态，未显式配置 type_hint 时使用。
DEFAULT_TYPE_HINTS: dict[str, str] = {
    "title": "text",
    "category": "text",
    "tags": "tags",
    "published_at": "datetime",
    "content": "multiline",
    "url": "text",
}


# ---------------------------------------------------------------------------
# 编译产物
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CompiledRule:
    type: str
    name: str
    on_fail: str
    fn: CleanFn
    index: int


@dataclass(frozen=True)
class CompiledValidation:
    type: str
    name: str
    on_fail: str | None
    weight: float | None
    fn: ValidateFn
    index: int


@dataclass
class CompiledField:
    name: str
    type_hint: str
    weight: float | None
    rules: list[CompiledRule] = field(default_factory=list)
    validations: list[CompiledValidation] = field(default_factory=list)


@dataclass(frozen=True)
class CompiledProfile:
    source: str
    quality: QualityConfig
    fields: dict[str, CompiledField]


# ---------------------------------------------------------------------------
# trace 与处理结果
# ---------------------------------------------------------------------------


@dataclass
class RuleTraceEntry:
    """单条规则的执行轨迹（清洗与校验共用）。"""

    field: str
    phase: str  # "clean" | "validate"
    rule: str
    index: int
    before: Any
    after: Any
    passed: bool
    error: str | None = None
    degraded: bool = False


@dataclass
class FieldQuality:
    field: str
    score: float
    weight: float
    passed: int
    failed: int


@dataclass
class ProcessResult:
    """一条记录经过规则引擎后的完整结果。"""

    values: dict[str, Any]
    quality_score: float
    low_quality: bool
    action: str  # "pass" | "warn" | "mark" | "skip"
    threshold: float
    validation_failures: list[dict[str, Any]]
    field_scores: dict[str, float]
    degraded_fields: list[str]
    trace: list[RuleTraceEntry]

    def trace_as_dicts(self) -> list[dict[str, Any]]:
        """把轨迹转成可 JSON 序列化的字典列表（写入 raw 快照）。"""

        return [entry.__dict__.copy() for entry in self.trace]


# ---------------------------------------------------------------------------
# 配置加载与编译
# ---------------------------------------------------------------------------


def load_rules(
    path: str | Path,
    *,
    known_sources: list[str] | None = None,
    logger: logging.Logger | None = None,
) -> "RuleEngine":
    """加载规则配置文件并完成全部启动期校验。

    文件不存在不算错误：记录一条 WARNING 并返回空引擎，所有来源走默认清洗，
    满足「规则异常不影响采集主流程」的降级要求。
    """

    log = logger or logging.getLogger(LOGGER_NAME)
    config_path = Path(path)
    if not config_path.is_file():
        log.warning("规则配置不存在，全部来源使用默认清洗 path=%s", config_path)
        empty_profile = CompiledProfile(source="*", quality=QualityConfig(), fields={})
        return RuleEngine(
            profiles={"*": empty_profile},
            source_path=str(config_path),
            missing=True,
            logger=log,
        )

    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise RuleConfigError(f"规则配置 YAML 解析失败: {exc}") from exc
    except OSError as exc:
        raise RuleConfigError(f"规则配置读取失败: {exc}") from exc

    if payload is None:
        payload = {}
    if not isinstance(payload, dict):
        raise RuleConfigError("规则配置顶层必须是映射（key/value 结构）")

    try:
        config = RulesConfig.model_validate(payload)
    except ValidationError as exc:
        details = [
            f"{'.'.join(str(x) for x in err['loc'])}: {err['msg']}" for err in exc.errors()
        ]
        raise RuleConfigError(
            "规则配置语法校验失败，共 %d 处问题" % len(details),
            errors=details,
        ) from exc

    errors: list[str] = []
    global_quality = config.quality or QualityConfig()
    global_defaults = config.field_defaults
    global_fields = config.fields

    _check_field_names("fields", global_fields, errors)
    for source_name, profile in config.sources.items():
        _check_field_names(f"sources[{source_name}].fields", profile.fields, errors)

    profiles: dict[str, CompiledProfile] = {}
    # 全局层（虚拟源名 *）始终编译，供未知来源兜底。
    profiles["*"] = _compile_profile(
        "*", global_quality, global_defaults, global_fields, errors
    )

    for source_name, profile in config.sources.items():
        quality = _merge_quality(global_quality, profile.quality)
        profiles[source_name] = _compile_profile(
            source_name,
            quality,
            profile.field_defaults if profile.field_defaults is not None else global_defaults,
            profile.fields,
            errors,
            base_fields=global_fields,
        )

    if known_sources is not None:
        for name in known_sources:
            if name not in config.sources:
                log.info("来源 %s 未配置独立规则，使用全局规则", name)

    if errors:
        raise RuleConfigError(
            f"规则配置校验失败，共 {len(errors)} 处问题",
            errors=sorted(set(errors)),
        )
    return RuleEngine(profiles=profiles, source_path=str(config_path), logger=log)


def _check_field_names(location: str, fields: dict[str, Any], errors: list[str]) -> None:
    unknown = sorted(name for name in fields if name not in STANDARD_FIELDS)
    for name in unknown:
        errors.append(
            f"{location}[{name}]: 未知字段，可配置字段: {', '.join(STANDARD_FIELDS)}"
        )


def _merge_quality(base: QualityConfig, override: QualityConfig | None) -> QualityConfig:
    if override is None:
        return base.model_copy()
    merged = base.model_copy()
    for attr in ("threshold", "default_weight", "on_validation_fail", "low_quality_flag"):
        if attr in override.model_fields_set:
            setattr(merged, attr, getattr(override, attr))
    return merged


def _resolve_field_set(
    field_name: str,
    defaults: FieldRuleSet | None,
    fields: dict[str, FieldRuleSet],
    base_fields: dict[str, FieldRuleSet] | None,
) -> FieldRuleSet | None:
    """按「来源显式字段 → 全局显式字段 → field_defaults」的顺序确定字段规则集。"""

    explicit = fields.get(field_name)
    if explicit is not None:
        return explicit
    if base_fields and field_name in base_fields:
        return base_fields[field_name]
    if defaults is None or (not defaults.rules and not defaults.validations):
        return None
    hint = (
        defaults.type_hint
        if "type_hint" in defaults.model_fields_set
        else DEFAULT_TYPE_HINTS[field_name]
    )
    return FieldRuleSet(
        type_hint=hint,  # type: ignore[arg-type]
        weight=defaults.weight,
        rules=defaults.rules,
        validations=defaults.validations,
    )


def _compile_profile(
    source: str,
    quality: QualityConfig,
    defaults: FieldRuleSet | None,
    fields: dict[str, FieldRuleSet],
    errors: list[str],
    *,
    base_fields: dict[str, FieldRuleSet] | None = None,
) -> CompiledProfile:
    compiled_fields: dict[str, CompiledField] = {}
    for field_name in STANDARD_FIELDS:
        rule_set = _resolve_field_set(field_name, defaults, fields, base_fields)
        if rule_set is None:
            continue
        compiled = _compile_field(source, field_name, rule_set, errors)
        if compiled.rules or compiled.validations:
            compiled_fields[field_name] = compiled
    return CompiledProfile(source=source, quality=quality, fields=compiled_fields)


def _compile_field(
    source: str,
    field_name: str,
    rule_set: FieldRuleSet,
    errors: list[str],
) -> CompiledField:
    compiled = CompiledField(
        name=field_name,
        type_hint=rule_set.type_hint,
        weight=rule_set.weight,
    )
    for index, spec in enumerate(rule_set.rules):
        if not spec.enabled:
            continue
        location = f"sources[{source}].fields[{field_name}].rules[{index}]({spec.type})"
        try:
            kind, fn = compile_operator(spec.type, spec.params)
        except (RuleExecutionError, ValueError) as exc:
            errors.append(f"{location}: {exc}")
            continue
        if kind != "clean":
            errors.append(f"{location}: {spec.type} 是校验算子，不能用在 rules 中")
            continue
        compiled.rules.append(
            CompiledRule(
                type=spec.type,
                name=spec.name or spec.type,
                on_fail=spec.on_fail,
                fn=fn,
                index=index,
            )
        )
    for index, spec in enumerate(rule_set.validations):
        if not spec.enabled:
            continue
        location = f"sources[{source}].fields[{field_name}].validations[{index}]({spec.type})"
        try:
            kind, fn = compile_operator(spec.type, spec.params)
        except (RuleExecutionError, ValueError) as exc:
            errors.append(f"{location}: {exc}")
            continue
        if kind != "validate":
            errors.append(f"{location}: {spec.type} 是清洗算子，不能用在 validations 中")
            continue
        compiled.validations.append(
            CompiledValidation(
                type=spec.type,
                name=spec.name or spec.type,
                on_fail=spec.on_fail,
                weight=spec.weight,
                fn=fn,
                index=index,
            )
        )
    return compiled


# ---------------------------------------------------------------------------
# 引擎
# ---------------------------------------------------------------------------


class RuleEngine:
    """规则引擎入口；按来源选择编译后的规则画像处理记录。"""

    def __init__(
        self,
        *,
        profiles: dict[str, CompiledProfile],
        source_path: str = "",
        missing: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self._profiles = profiles
        self.source_path = source_path
        self.missing = missing
        self._log = logger or logging.getLogger(LOGGER_NAME)

    # ---- 对外只读视图 ----
    def profile_for(self, source: str) -> CompiledProfile:
        return self._profiles.get(source) or self._profiles["*"]

    def has_rules(self, source: str) -> bool:
        """该来源是否配置了任何清洗规则或质量校验。"""

        profile = self._profiles.get(source) or self._profiles.get("*")
        if profile is None:
            return False
        return any(field.rules or field.validations for field in profile.fields.values())

    def configured_sources(self) -> list[str]:
        return sorted(name for name in self._profiles if name != "*")

    def describe_operators(self) -> dict[str, list[str]]:
        return {
            "clean": operator_names("clean"),
            "validate": operator_names("validate"),
        }

    # ---- 主流程 ----
    def process(
        self,
        source: str,
        external_id: str,
        raw_values: dict[str, Any],
    ) -> ProcessResult:
        """处理一条记录的全部标准字段。

        ``raw_values`` 为字段映射后的原始值（键为标准字段名）；任何引擎内部
        异常都被兜底捕获并降级为默认清洗，不向采集主流程抛出。
        """

        try:
            return self._process(source, external_id, raw_values)
        except Exception as exc:  # 最后一道防线：绝不让规则引擎拖垮采集
            self._log.error(
                "规则引擎内部异常，整条记录降级默认清洗 source=%s external_id=%s error=%s",
                source,
                external_id,
                exc,
                exc_info=True,
            )
            values = {
                name: self._default_clean(name, raw_values.get(name)) for name in STANDARD_FIELDS
            }
            return ProcessResult(
                values=values,
                quality_score=100.0,
                low_quality=False,
                action="pass",
                threshold=100.0,
                validation_failures=[{"field": "*", "rule": "engine", "message": f"引擎降级: {exc}"}],
                field_scores={},
                degraded_fields=list(STANDARD_FIELDS),
                trace=[],
            )

    def _process(self, source: str, external_id: str, raw_values: dict[str, Any]) -> ProcessResult:
        profile = self.profile_for(source)
        trace: list[RuleTraceEntry] = []
        values: dict[str, Any] = {}
        degraded_fields: list[str] = []
        field_quality: list[FieldQuality] = []
        all_failures: list[dict[str, Any]] = []

        for field_name in STANDARD_FIELDS:
            compiled = profile.fields.get(field_name)
            raw = raw_values.get(field_name)
            hint = compiled.type_hint if compiled else DEFAULT_TYPE_HINTS[field_name]

            if compiled is None or not compiled.rules:
                values[field_name] = self._default_clean(field_name, raw, hint)
                if compiled is not None and compiled.validations:
                    # 只有校验、没有清洗规则的字段同样参与评分。
                    quality = self._run_validations(
                        source, external_id, compiled, profile.quality,
                        values[field_name], trace, all_failures,
                    )
                    if quality is not None:
                        field_quality.append(quality)
                continue

            current = raw
            field_degraded = False
            for rule in compiled.rules:
                before = current
                try:
                    current = self._apply_rule(rule, current, hint)
                    trace.append(
                        RuleTraceEntry(
                            field=field_name,
                            phase="clean",
                            rule=rule.name,
                            index=rule.index,
                            before=_preview(before),
                            after=_preview(current),
                            passed=True,
                        )
                    )
                except RuleExecutionError as exc:
                    trace.append(
                        RuleTraceEntry(
                            field=field_name,
                            phase="clean",
                            rule=rule.name,
                            index=rule.index,
                            before=_preview(before),
                            after=_preview(before),
                            passed=False,
                            error=exc.message,
                            degraded=True,
                        )
                    )
                    if rule.on_fail == "keep":
                        self._log.warning(
                            "清洗规则失败，保留原值 source=%s external_id=%s field=%s rule=%s error=%s",
                            source, external_id, field_name, rule.name, exc.message,
                        )
                        continue
                    # default / abort_field：整个字段回退默认清洗
                    current = self._default_clean(field_name, raw, hint)
                    field_degraded = True
                    degraded_fields.append(field_name)
                    self._log.warning(
                        "清洗规则失败，字段降级默认清洗 source=%s external_id=%s field=%s rule=%s error=%s",
                        source, external_id, field_name, rule.name, exc.message,
                    )
                    break

            values[field_name] = self._coerce(field_name, current, hint, raw)
            if field_degraded:
                trace.append(
                    RuleTraceEntry(
                        field=field_name,
                        phase="clean",
                        rule="<default-fallback>",
                        index=-1,
                        before=_preview(raw),
                        after=_preview(values[field_name]),
                        passed=True,
                        degraded=True,
                    )
                )

            quality = self._run_validations(
                source, external_id, compiled, profile.quality,
                values[field_name], trace, all_failures,
            )
            if quality is not None:
                field_quality.append(quality)

        score, field_scores = self._score(field_quality, profile.quality)
        action = self._decide_action(all_failures)
        low_quality = profile.quality.low_quality_flag and score < profile.quality.threshold

        result = ProcessResult(
            values=values,
            quality_score=round(score, 2),
            low_quality=low_quality,
            action=action,
            threshold=profile.quality.threshold,
            validation_failures=all_failures,
            field_scores={name: round(value, 2) for name, value in field_scores.items()},
            degraded_fields=sorted(set(degraded_fields)),
            trace=trace,
        )
        self._log.debug(
            "规则引擎处理完成 source=%s external_id=%s score=%.2f action=%s low_quality=%s",
            source, external_id, result.quality_score, result.action,
            str(result.low_quality).lower(),
        )
        return result

    # ---- 规则执行 ----
    def _apply_rule(self, rule: CompiledRule, value: Any, hint: str) -> Any:
        if hint == "tags" and isinstance(value, (list, tuple)):
            output = [rule.fn(item) for item in value]
            # 算子可能把标量展开成列表（如 split_tags），展平并保持顺序。
            flattened: list[Any] = []
            for item in output:
                if isinstance(item, (list, tuple)):
                    flattened.extend(item)
                else:
                    flattened.append(item)
            return flattened
        return rule.fn(value)

    def _run_validations(
        self,
        source: str,
        external_id: str,
        compiled: CompiledField,
        quality: QualityConfig,
        value: Any,
        trace: list[RuleTraceEntry],
        all_failures: list[dict[str, Any]],
    ) -> FieldQuality | None:
        if not compiled.validations:
            return None
        passed = 0
        weighted_pass = 0.0
        weighted_total = 0.0
        for validation in compiled.validations:
            weight = validation.weight or quality.default_weight
            weighted_total += weight
            try:
                validation.fn(value)
            except ValidationFailure as exc:
                action = validation.on_fail or quality.on_validation_fail
                all_failures.append(
                    {
                        "field": compiled.name,
                        "rule": validation.name,
                        "action": action,
                        "message": exc.message,
                    }
                )
                trace.append(
                    RuleTraceEntry(
                        field=compiled.name,
                        phase="validate",
                        rule=validation.name,
                        index=validation.index,
                        before=_preview(value),
                        after=_preview(value),
                        passed=False,
                        error=exc.message,
                    )
                )
                self._log_validation(source, external_id, validation.name, compiled.name, exc.message, action)
                continue
            passed += 1
            weighted_pass += weight
            trace.append(
                RuleTraceEntry(
                    field=compiled.name,
                    phase="validate",
                    rule=validation.name,
                    index=validation.index,
                    before=_preview(value),
                    after=_preview(value),
                    passed=True,
                )
            )
        score = 100.0 * weighted_pass / weighted_total if weighted_total else 100.0
        return FieldQuality(
            field=compiled.name,
            score=score,
            weight=compiled.weight or 1.0,
            passed=passed,
            failed=len(compiled.validations) - passed,
        )

    def _log_validation(
        self,
        source: str,
        external_id: str,
        rule: str,
        field_name: str,
        message: str,
        action: str,
    ) -> None:
        line = (
            "质量校验未通过 source=%s external_id=%s field=%s rule=%s action=%s reason=%s"
            % (source, external_id, field_name, rule, action, message)
        )
        if action == "warn":
            self._log.warning(line)
        else:
            self._log.info(line)

    # ---- 评分与处置 ----
    def _score(
        self,
        field_quality: list[FieldQuality],
        quality: QualityConfig,
    ) -> tuple[float, dict[str, float]]:
        if not field_quality:
            return 100.0, {}
        total_weight = sum(item.weight for item in field_quality)
        weighted = sum(item.score * item.weight for item in field_quality) / total_weight
        per_field = {item.field: item.score for item in field_quality}
        return weighted, per_field

    def _decide_action(self, failures: list[dict[str, Any]]) -> str:
        if not failures:
            return "pass"
        priority = {"skip": 3, "mark": 2, "warn": 1}
        return max((item["action"] for item in failures), key=lambda action: priority[action])

    # ---- 默认清洗（降级路径，与 normalize.py 既有行为一致）----
    def _default_clean(self, field_name: str, value: Any, hint: str | None = None) -> Any:
        effective_hint = hint or DEFAULT_TYPE_HINTS[field_name]
        if effective_hint == "tags":
            return split_tags(value)
        if effective_hint == "html":
            return html_to_text(value)
        if effective_hint == "multiline":
            return clean_multiline(value)
        if effective_hint == "datetime":
            return parse_datetime(value)
        return clean_text(value)

    def _coerce(self, field_name: str, value: Any, hint: str, raw: Any) -> Any:
        """把规则链输出转成 Record 字段需要的最终类型。"""

        if field_name == "tags":
            if isinstance(value, list):
                return [clean_text(item) for item in value if clean_text(item)]
            return split_tags(value if value is not None else raw)
        if field_name == "published_at":
            if isinstance(value, datetime):
                return value
            return parse_datetime(value if value is not None else raw)
        if value is None:
            return ""
        if isinstance(value, list):
            return clean_text(value)
        return value if isinstance(value, str) else str(value)


def _preview(value: Any) -> Any:
    """trace 落盘前的安全预览：非标量转字符串，长文本截断。"""

    if value is None or isinstance(value, (int, float, bool, list, dict)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value)
    if len(text) <= TRACE_VALUE_LIMIT:
        return text
    return text[:TRACE_VALUE_LIMIT] + "…<truncated>"
