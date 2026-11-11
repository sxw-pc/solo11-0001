"""规则引擎：编译规则、执行清洗与校验、计算质量分、输出执行轨迹。

设计约束：

- **绝不向采集主流程抛异常**：算子失败只记录轨迹并保留原值（降级为默认清洗）。
- **启动时编译**：正则、参数校验都在 :class:`RuleSet` 构建时完成，
  运行期只做字典查找与闭包调用，单条开销为微秒级。
- **全程可追溯**：每条规则执行都留下 before/after 轨迹，汇总为质量报告。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any

from crawl_flow.rules.config import RuleFileConfig, RuleSpec
from crawl_flow.rules.operators import (
    CleanFailed,
    Operator,
    PreparedRule,
    RuleViolation,
    get_operator,
)

# 严重程度对应的扣分系数：error 扣全部权重，warning 扣一半。
SEVERITY_FACTOR: dict[str, float] = {"error": 1.0, "warning": 0.5}

# 轨迹中字段值的预览长度上限，避免大正文刷屏与内存拷贝开销。
PREVIEW_LIMIT = 120


def _preview(value: Any, limit: int = PREVIEW_LIMIT) -> str:
    """把任意值转成定长单行预览，用于日志与轨迹。"""

    if value is None:
        return ""
    text = str(value).replace("\n", "\\n")
    if len(text) > limit:
        return text[:limit] + "…"
    return text


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


@dataclass(frozen=True)
class CompiledRule:
    """一条已编译规则：算子闭包 + 计分元数据。"""

    field: str
    name: str
    kind: str  # clean | validate
    list_mode: str  # each | whole
    order: int
    severity: str
    weight: float
    func: PreparedRule


@dataclass(frozen=True)
class RuleTrace:
    """单条规则的执行轨迹（含处理前后值）。"""

    field: str
    rule: str
    kind: str
    status: str  # ok | failed | error | skipped
    before: str
    after: str
    message: str
    elapsed_us: int


@dataclass
class QualityReport:
    """单条记录的规则执行汇总：轨迹、违规、质量分与处置动作。"""

    source: str
    external_id: str
    threshold: int
    on_failure: str
    traces: list[RuleTrace] = field(default_factory=list)
    violations: list[str] = field(default_factory=list)
    flags: list[str] = field(default_factory=list)
    total_weight: float = 0.0
    lost_weight: float = 0.0
    score: int = 100
    action: str = "ok"  # ok | skip | mark | alert
    elapsed_us: int = 0

    @property
    def is_low_quality(self) -> bool:
        return self.score < self.threshold


class RuleEngine:
    """单个源的规则引擎：清洗 → 校验 → 评分。"""

    def __init__(
        self,
        source_name: str,
        rules: list[CompiledRule],
        *,
        threshold: int,
        on_failure: str,
    ) -> None:
        self.source_name = source_name
        self.clean_rules = [rule for rule in rules if rule.kind == "clean"]
        self.validate_rules = [rule for rule in rules if rule.kind == "validate"]
        self.threshold = threshold
        self.on_failure = on_failure

    # ---- 对外接口 ----

    def new_report(self, external_id: str) -> QualityReport:
        return QualityReport(
            source=self.source_name,
            external_id=external_id,
            threshold=self.threshold,
            on_failure=self.on_failure,
        )

    def clean_fields(
        self, fields: dict[str, Any], report: QualityReport
    ) -> dict[str, Any]:
        """对 field_map 提取后的原始字段执行清洗规则，返回新字典。

        算子失败（含未预期异常）时保留原值并记录轨迹，保证默认清洗逻辑
        仍能在原值上继续执行。
        """

        result = dict(fields)
        for rule in self.clean_rules:
            value = result.get(rule.field)
            before = _preview(value)
            started = time.perf_counter_ns()
            try:
                new_value = self._apply(rule, value)
            except CleanFailed as exc:
                self._record(report, rule, "failed", before, before, str(exc), started)
                continue
            except Exception as exc:  # 算子自身异常：降级保留原值
                self._record(
                    report, rule, "error", before, before, f"算子异常: {exc}", started
                )
                continue
            result[rule.field] = new_value
            self._record(report, rule, "ok", before, _preview(new_value), "", started)
        return result

    def validate_record(
        self, values: dict[str, Any], report: QualityReport
    ) -> None:
        """对默认清洗后的字段值执行校验规则，结果写入 ``report``。"""

        for rule in self.validate_rules:
            value = values.get(rule.field)
            before = _preview(value)
            started = time.perf_counter_ns()
            # 空值只由 required 规则负责，其余校验跳过且不计分。
            if rule.name != "required" and _is_empty(value):
                self._record(
                    report, rule, "skipped", before, before, "空值跳过", started,
                    charge=False,
                )
                continue
            try:
                self._apply(rule, value)
            except RuleViolation as exc:
                self._record(report, rule, "failed", before, before, str(exc), started)
                continue
            except Exception as exc:  # 算子自身异常按失败计分，但不中断流程
                self._record(
                    report, rule, "error", before, before, f"算子异常: {exc}", started
                )
                continue
            self._record(report, rule, "ok", before, before, "", started)

    def finalize(self, report: QualityReport) -> QualityReport:
        """计算质量分、标记与处置动作。"""

        if report.total_weight > 0:
            ratio = 1.0 - report.lost_weight / report.total_weight
            report.score = max(0, min(100, round(100 * ratio)))
        else:
            report.score = 100
        if report.is_low_quality:
            report.action = self.on_failure
            if "low_quality" not in report.flags:
                report.flags.append("low_quality")
        else:
            report.action = "ok"
        return report

    # ---- 内部 ----

    def _apply(self, rule: CompiledRule, value: Any) -> Any:
        if rule.list_mode == "each" and isinstance(value, (list, tuple)):
            return [rule.func(item) for item in value]
        return rule.func(value)

    def _record(
        self,
        report: QualityReport,
        rule: CompiledRule,
        status: str,
        before: str,
        after: str,
        message: str,
        started_ns: int,
        *,
        charge: bool = True,
    ) -> None:
        elapsed_us = (time.perf_counter_ns() - started_ns) // 1000
        report.elapsed_us += elapsed_us
        report.traces.append(
            RuleTrace(
                field=rule.field,
                rule=rule.name,
                kind=rule.kind,
                status=status,
                before=before,
                after=after,
                message=message,
                elapsed_us=elapsed_us,
            )
        )
        if not charge:
            return
        report.total_weight += rule.weight
        if status in ("failed", "error"):
            factor = SEVERITY_FACTOR.get(rule.severity, 1.0)
            report.lost_weight += rule.weight * factor
            report.flags.append(f"{rule.kind}:{rule.field}:{rule.name}")
            if message:
                report.violations.append(f"{rule.field}.{rule.name}: {message}")


class RuleSet:
    """编译后的规则集：按源提供 :class:`RuleEngine`。

    构建时完成全部算子参数校验与正则预编译（启动时校验），
    任何配置问题都会以带位置信息的 :class:`ConfigError` 抛出，
    避免运行到具体源时才发现语法错误。
    """

    def __init__(self, config: RuleFileConfig) -> None:
        self._config = config
        self._engines: dict[str, RuleEngine | None] = {}
        # 启动时全量编译：先探测 defaults 本身合法性，再编译每个已配置源。
        if config.defaults.fields:
            self._build("__defaults_probe__")
        for source_name in config.sources:
            self._engines[source_name] = self._build(source_name)

    @property
    def config(self) -> RuleFileConfig:
        return self._config

    def engine_for(self, source_name: str) -> RuleEngine | None:
        """返回该源的引擎；源无任何规则（含 defaults）时返回 ``None``。"""

        if source_name in self._engines:
            return self._engines[source_name]
        engine = self._build(source_name)
        self._engines[source_name] = engine
        return engine

    def describe(self) -> list[str]:
        """供 ``rules check`` 输出的配置摘要。"""

        lines = [
            f"quality_threshold={self._config.quality_threshold} "
            f"on_failure={self._config.on_failure}",
            f"defaults: {self._count_rules(self._config.defaults.fields)} 条字段规则",
        ]
        for name, source_rules in sorted(self._config.sources.items()):
            lines.append(f"source {name}: {self._count_rules(source_rules.fields)} 条字段规则")
        return lines

    @staticmethod
    def _count_rules(fields: dict[str, list[RuleSpec]]) -> int:
        return sum(len(specs) for specs in fields.values())

    def _build(self, source_name: str) -> RuleEngine | None:
        config = self._config
        source_rules = config.sources.get(source_name)

        # 合并 defaults 与源级字段规则：源级同名字段整体覆盖 defaults。
        merged: dict[str, list[RuleSpec]] = dict(config.defaults.fields)
        if source_rules is not None:
            merged.update(source_rules.fields)

        compiled: list[CompiledRule] = []
        for field_name, specs in merged.items():
            # 错误位置指向实际生效的配置块，便于定位。
            if source_rules is not None and field_name in source_rules.fields:
                where_prefix = f"sources.{source_name}.fields.{field_name}"
            else:
                where_prefix = f"defaults.fields.{field_name}"
            for index, spec in enumerate(specs):
                if not spec.enabled:
                    continue
                compiled.append(self._compile(spec, field_name, f"{where_prefix}[{index}]"))

        if not compiled:
            return None
        # 执行顺序：order 小的先跑，相同 order 保持配置书写顺序。
        compiled.sort(key=lambda rule: rule.order)

        threshold = (
            source_rules.quality_threshold
            if source_rules is not None and source_rules.quality_threshold is not None
            else config.quality_threshold
        )
        on_failure = (
            source_rules.on_failure
            if source_rules is not None and source_rules.on_failure is not None
            else config.on_failure
        )
        return RuleEngine(source_name, compiled, threshold=threshold, on_failure=on_failure)

    @staticmethod
    def _compile(spec: RuleSpec, field_name: str, where: str) -> CompiledRule:
        operator: Operator = get_operator(spec.rule, where)
        func = operator.prepare(spec.operator_params(), where)
        return CompiledRule(
            field=field_name,
            name=operator.name,
            kind=operator.kind,
            list_mode=operator.list_mode,
            order=spec.order,
            severity=spec.severity,
            weight=spec.weight,
            func=func,
        )


def load_rule_set(path: str) -> RuleSet:
    """读取规则文件并编译为 :class:`RuleSet`（启动时全量校验）。"""

    from crawl_flow.rules.config import load_rule_config

    return RuleSet(load_rule_config(path))


def log_quality(logger: logging.Logger, report: QualityReport) -> None:
    """输出单条质量汇总（INFO）与逐字段轨迹（DEBUG，含处理前后值）。"""

    log = logger.info
    if report.action in ("skip", "alert"):
        log = logger.warning
    elif report.is_low_quality:
        log = logger.warning
    log(
        "event=quality source=%s external_id=%s score=%d threshold=%d action=%s "
        "violations=%d elapsed_us=%d",
        report.source,
        report.external_id,
        report.score,
        report.threshold,
        report.action,
        len(report.violations),
        report.elapsed_us,
    )
    for violation in report.violations:
        logger.warning(
            "event=quality_violation source=%s external_id=%s %s",
            report.source,
            report.external_id,
            violation,
        )
    for trace in report.traces:
        logger.debug(
            "event=rule_trace source=%s external_id=%s field=%s rule=%s kind=%s "
            "status=%s before=%r after=%r message=%s elapsed_us=%d",
            report.source,
            report.external_id,
            trace.field,
            trace.rule,
            trace.kind,
            trace.status,
            trace.before,
            trace.after,
            trace.message,
            trace.elapsed_us,
        )
