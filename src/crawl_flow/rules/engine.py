"""规则引擎：按配置执行字段清洗与质量校验，输出质量评分与执行轨迹。

设计要点：

- **启动时编译**：引擎构造时把配置编译成执行计划（算子查表、参数校验、
  正则预编译），配置错误在此刻抛 :class:`RuleConfigError`，运行期不再暴露。
- **单条隔离**：任何规则执行异常都被捕获为 ``error`` 轨迹并按失败计分，
  字段保留上一次的值，绝不影响采集主流程。
- **可追溯**：每条规则产生一条 :class:`RuleTrace`（处理前后值），DEBUG 日志
  逐条输出；质量闸门决策以 WARNING 输出。
- **轻量**：纯标准库实现，单条处理为微秒级，远低于单条采集耗时的 10%。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Mapping

from pydantic import ValidationError

from crawl_flow.errors import RuleConfigError
from crawl_flow.rules.config import (
    FailureMode,
    RuleConfig,
    RuleSetConfig,
    SourceRules,
)
from crawl_flow.rules.operators import OPERATORS, OperatorSpec, RuleFailed

LOGGER_NAME = "crawl_flow.rules"
MAX_PREVIEW_LENGTH = 120

# 失败处理模式的严重级别：skip > mark > warn
_ACTION_SEVERITY: dict[str, int] = {"ok": 0, "warn": 1, "mark": 2, "skip": 3}


@dataclass(frozen=True)
class RuleTrace:
    """单条规则的执行轨迹：字段、算子、状态与处理前后值。"""

    field: str
    rule: str
    status: str  # ok | failed | error
    before: Any
    after: Any
    message: str = ""


@dataclass(frozen=True)
class RuleFailure:
    """一次规则未命中：计入质量分的扣分项。"""

    field: str
    rule: str
    reason: str
    weight: int
    on_failure: FailureMode | None  # 规则级覆盖；None 表示跟随源级模式


@dataclass(frozen=True)
class QualityReport:
    """单条记录的质量报告。"""

    score: int  # 0-100，100 为满分
    threshold: int
    below_threshold: bool
    action: str  # ok | skip | mark | warn
    failures: tuple[RuleFailure, ...] = ()
    traces: tuple[RuleTrace, ...] = ()

    @property
    def low_quality(self) -> bool:
        """是否打低质量标记：仅 ``mark`` 模式落库标记，``warn`` 只告警。"""

        return self.action == "mark"


@dataclass(frozen=True)
class RuleResult:
    """规则引擎输出：清洗后的字段字典 + 质量报告。"""

    fields: dict[str, Any]
    report: QualityReport


@dataclass(frozen=True)
class _CompiledRule:
    name: str
    operator: OperatorSpec
    params: Any
    weight: int
    on_failure: FailureMode | None


@dataclass(frozen=True)
class _SourcePlan:
    threshold: int
    on_quality_failure: FailureMode
    fields: tuple[tuple[str, tuple[_CompiledRule, ...]], ...]  # 保序


class RuleEngine:
    """字段规则引擎：构造时编译全部规则，逐条处理采集字段。"""

    def __init__(
        self,
        config: RuleSetConfig,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._log = logger or logging.getLogger(LOGGER_NAME)
        self._defaults = config.defaults
        self._plans = {
            name: self._compile_plan(f"sources.{name}", source_rules)
            for name, source_rules in config.sources.items()
        }
        self._default_plan = (
            self._compile_plan("default", config.default) if config.default is not None else None
        )

    # ---- 编译（启动校验） ----

    def _compile_plan(self, where: str, source_rules: SourceRules) -> _SourcePlan:
        compiled_fields: list[tuple[str, tuple[_CompiledRule, ...]]] = []
        for field_name, field_rules in source_rules.fields.items():
            compiled = tuple(
                self._compile_rule(f"{where}.fields.{field_name}.rules[{index}]", rule)
                for index, rule in enumerate(field_rules.rules, start=1)
            )
            compiled_fields.append((field_name, compiled))
        return _SourcePlan(
            threshold=(
                source_rules.threshold
                if source_rules.threshold is not None
                else self._defaults.threshold
            ),
            on_quality_failure=(
                source_rules.on_quality_failure
                if source_rules.on_quality_failure is not None
                else self._defaults.on_quality_failure
            ),
            fields=tuple(compiled_fields),
        )

    @staticmethod
    def _compile_rule(where: str, rule: RuleConfig) -> _CompiledRule:
        operator = OPERATORS.get(rule.name)
        if operator is None:
            available = ", ".join(sorted(OPERATORS))
            raise RuleConfigError(
                f"{where}: 未知规则 {rule.name!r}，可用规则: {available}"
            )
        try:
            params = operator.params_model.model_validate(rule.params)
        except ValidationError as exc:
            raise RuleConfigError(
                f"{where}: 规则 {rule.name} 参数错误: {exc}"
            ) from exc
        return _CompiledRule(
            name=rule.name,
            operator=operator,
            params=params,
            weight=rule.weight,
            on_failure=rule.on_failure,
        )

    # ---- 运行 ----

    def has_rules(self, source: str) -> bool:
        """该源是否有可用规则（含 default 兜底段）。"""

        return source in self._plans or self._default_plan is not None

    def process(
        self,
        source: str,
        external_id: str,
        fields: Mapping[str, Any],
    ) -> RuleResult | None:
        """对一条记录的源字段执行规则链；该源无规则时返回 ``None``。

        单条规则异常被隔离为 ``error`` 轨迹（按失败计分、保留原值），
        不会向上抛出，保证采集主流程不被规则问题中断。
        """

        plan = self._plans.get(source, self._default_plan)
        if plan is None:
            return None

        cleaned = dict(fields)
        traces: list[RuleTrace] = []
        failures: list[RuleFailure] = []

        for field_name, rules in plan.fields:
            current = cleaned.get(field_name)
            for rule in rules:
                before = current
                try:
                    current = rule.operator.func(current, rule.params)
                    traces.append(RuleTrace(field_name, rule.name, "ok", before, current))
                except RuleFailed as exc:
                    traces.append(
                        RuleTrace(field_name, rule.name, "failed", before, before, str(exc))
                    )
                    failures.append(
                        RuleFailure(field_name, rule.name, str(exc), rule.weight, rule.on_failure)
                    )
                except Exception as exc:  # 算子内部异常：按失败计分，保留原值，不中断
                    traces.append(
                        RuleTrace(
                            field_name, rule.name, "error", before, before, repr(exc)
                        )
                    )
                    failures.append(
                        RuleFailure(
                            field_name,
                            rule.name,
                            f"规则执行异常: {exc}",
                            rule.weight,
                            rule.on_failure,
                        )
                    )
            if field_name in cleaned or current is not None:
                cleaned[field_name] = current

        score = max(0, 100 - sum(failure.weight for failure in failures))
        below_threshold = score < plan.threshold
        action = _resolve_action(failures, below_threshold, plan)
        report = QualityReport(
            score=score,
            threshold=plan.threshold,
            below_threshold=below_threshold,
            action=action,
            failures=tuple(failures),
            traces=tuple(traces),
        )
        self._log_result(source, external_id, report)
        return RuleResult(fields=cleaned, report=report)

    # ---- 日志 ----

    def _log_result(self, source: str, external_id: str, report: QualityReport) -> None:
        for trace in report.traces:
            self._log.debug(
                "rule_trace source=%s external_id=%s field=%s rule=%s status=%s "
                "before=%s after=%s message=%s",
                source,
                external_id,
                trace.field,
                trace.rule,
                trace.status,
                _preview(trace.before),
                _preview(trace.after),
                trace.message or "-",
            )
        if report.action == "ok":
            return
        summary = "; ".join(
            f"{failure.field}.{failure.rule}({failure.reason})" for failure in report.failures
        )
        self._log.warning(
            "quality_gate source=%s external_id=%s action=%s score=%d threshold=%d failures=%s",
            source,
            external_id,
            report.action,
            report.score,
            report.threshold,
            summary or "-",
        )


def _resolve_action(
    failures: list[RuleFailure],
    below_threshold: bool,
    plan: _SourcePlan,
) -> str:
    """汇总条目级处理动作：规则级覆盖与阈值 breach 取最严重者。"""

    modes: list[str] = [
        failure.on_failure for failure in failures if failure.on_failure is not None
    ]
    if below_threshold:
        modes.append(plan.on_quality_failure)
    if not modes:
        return "ok"
    return max(modes, key=lambda mode: _ACTION_SEVERITY[mode])


def _preview(value: Any, limit: int = MAX_PREVIEW_LENGTH) -> str:
    """日志用的值预览：repr 并截断，避免长正文刷屏。"""

    text = repr(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"
