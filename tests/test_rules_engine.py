"""规则引擎：执行顺序、质量评分、处理模式、降级与执行轨迹。"""

from __future__ import annotations

import logging
import time

import pytest

from crawl_flow.rules import RuleEngine, RuleSetConfig
from crawl_flow.rules import operators


def _engine(payload: dict, **kwargs) -> RuleEngine:
    return RuleEngine(RuleSetConfig.model_validate(payload), **kwargs)


def _source_payload(
    rules: list[dict],
    *,
    field: str = "title",
    threshold: int = 60,
    mode: str = "mark",
) -> dict:
    return {
        "sources": {
            "demo": {
                "threshold": threshold,
                "on_quality_failure": mode,
                "fields": {field: {"rules": rules}},
            }
        }
    }


def test_rules_execute_in_configured_order() -> None:
    # 先替换再截断：替换产生的长文本被截断；顺序反过来则不会被截断
    engine = _engine(
        _source_payload(
            [
                {"name": "regex_replace", "params": {"pattern": "x", "replacement": "长文本替换内容"}},
                {"name": "truncate", "params": {"max_length": 5}},
            ]
        )
    )
    result = engine.process("demo", "id-1", {"title": "x"})
    assert result is not None
    assert result.fields["title"] == "长文本替换"

    engine_reversed = _engine(
        _source_payload(
            [
                {"name": "truncate", "params": {"max_length": 5}},
                {"name": "regex_replace", "params": {"pattern": "x", "replacement": "长文本替换内容"}},
            ]
        )
    )
    reversed_result = engine_reversed.process("demo", "id-1", {"title": "x"})
    assert reversed_result is not None
    assert reversed_result.fields["title"] == "长文本替换内容"


def test_rules_are_scoped_per_source_and_field() -> None:
    engine = _engine(
        {
            "sources": {
                "source_a": {"fields": {"title": {"rules": [{"name": "strip"}]}}},
                "source_b": {"fields": {"title": {"rules": [{"name": "truncate", "params": {"max_length": 2}}]}}},
            }
        }
    )
    assert engine.process("source_a", "i", {"title": "  标题  "}).fields["title"] == "标题"
    assert engine.process("source_b", "i", {"title": "  标题  "}).fields["title"] == "  "
    # 未配置的源返回 None → 主流程走默认清洗
    assert engine.process("source_c", "i", {"title": "x"}) is None
    # 未配置的字段保持原值
    result = engine.process("source_a", "i", {"title": " t ", "other": "  不动  "})
    assert result.fields["other"] == "  不动  "


def test_default_section_applies_to_unconfigured_sources() -> None:
    engine = _engine(
        {
            "default": {"fields": {"title": {"rules": [{"name": "strip"}]}}},
            "sources": {"configured": {"fields": {"title": {"rules": [{"name": "truncate", "params": {"max_length": 1}}]}}}},
        }
    )
    assert engine.process("anything", "i", {"title": "  ab "}).fields["title"] == "ab"
    # 显式配置的源优先于 default 段
    assert engine.process("configured", "i", {"title": "  ab "}).fields["title"] == " "


def test_quality_score_deducts_failed_rule_weights() -> None:
    engine = _engine(
        _source_payload(
            [
                {"name": "required", "weight": 30},
                {"name": "length", "params": {"min": 2}, "weight": 25},
                {"name": "strip"},  # 默认权重 10，不会失败
            ]
        )
    )
    result = engine.process("demo", "i", {"title": ""})
    report = result.report
    # required(-30) 与 length(-25) 命中，strip 通过
    assert report.score == 100 - 30 - 25
    assert [f.rule for f in report.failures] == ["required", "length"]
    # 全部通过时满分
    ok = engine.process("demo", "i", {"title": "标题"})
    assert ok.report.score == 100
    assert ok.report.action == "ok"


def test_score_floors_at_zero() -> None:
    engine = _engine(
        _source_payload(
            [{"name": "required", "weight": 70}, {"name": "length", "params": {"min": 5}, "weight": 70}]
        )
    )
    report = engine.process("demo", "i", {"title": ""}).report
    assert report.score == 0
    assert report.below_threshold


@pytest.mark.parametrize(
    ("mode", "expected_action", "expected_low_quality"),
    [("skip", "skip", False), ("mark", "mark", True), ("warn", "warn", False)],
)
def test_below_threshold_triggers_configured_mode(
    mode: str, expected_action: str, expected_low_quality: bool
) -> None:
    engine = _engine(
        _source_payload(
            [{"name": "enum", "params": {"values": ["甲"]}, "weight": 50}],
            threshold=60,
            mode=mode,
        )
    )
    report = engine.process("demo", "i", {"title": "不在枚举里"}).report
    assert report.score == 50
    assert report.below_threshold
    assert report.action == expected_action
    assert report.low_quality is expected_low_quality


def test_above_threshold_stays_ok() -> None:
    engine = _engine(
        _source_payload(
            [{"name": "enum", "params": {"values": ["甲"]}, "weight": 10}],
            threshold=60,
            mode="skip",
        )
    )
    report = engine.process("demo", "i", {"title": "乙"}).report
    assert report.score == 90
    assert not report.below_threshold
    assert report.action == "ok"


def test_rule_level_on_failure_overrides_regardless_of_score() -> None:
    # 扣分很少（不触发阈值），但规则级 on_failure=skip 直接生效
    engine = _engine(
        _source_payload(
            [{"name": "required", "weight": 5, "on_failure": "skip"}],
            threshold=10,
            mode="warn",
        )
    )
    report = engine.process("demo", "i", {"title": ""}).report
    assert report.score == 95
    assert report.action == "skip"


def test_most_severe_mode_wins() -> None:
    engine = _engine(
        _source_payload(
            [
                {"name": "required", "weight": 5, "on_failure": "warn"},
                {"name": "length", "params": {"min": 3}, "weight": 5, "on_failure": "skip"},
            ]
        )
    )
    report = engine.process("demo", "i", {"title": ""}).report
    assert report.action == "skip"


def test_failed_clean_rule_keeps_previous_value() -> None:
    engine = _engine(
        _source_payload(
            [
                {"name": "strip"},
                {"name": "date_format", "weight": 10},
                {"name": "required"},
            ]
        )
    )
    result = engine.process("demo", "i", {"title": "  不是日期  "})
    # date_format 失败：字段保留上一步 strip 的结果，后续规则继续执行
    assert result.fields["title"] == "不是日期"
    trace = [t for t in result.report.traces if t.rule == "date_format"][0]
    assert trace.status == "failed"
    assert trace.before == trace.after == "不是日期"


def test_operator_runtime_error_isolated_as_error_trace() -> None:
    engine = _engine(_source_payload([{"name": "strip", "weight": 15}]))

    def _boom(value, params):  # 模拟算子内部 bug
        raise RuntimeError("boom")

    original = operators.OPERATORS["strip"].func
    object.__setattr__(operators.OPERATORS["strip"], "func", _boom)
    try:
        result = engine.process("demo", "i", {"title": "  标题  "})
    finally:
        object.__setattr__(operators.OPERATORS["strip"], "func", original)

    assert result is not None
    assert result.fields["title"] == "  标题  "  # 原值保留
    assert result.report.score == 85  # 按失败扣分
    assert result.report.traces[0].status == "error"
    assert "boom" in result.report.traces[0].message


def test_traces_record_before_and_after_values() -> None:
    engine = _engine(
        _source_payload(
            [
                {"name": "strip"},
                {"name": "truncate", "params": {"max_length": 2}},
            ]
        )
    )
    result = engine.process("demo", "i", {"title": "  标题长  "})
    traces = result.report.traces
    assert [(t.rule, t.before, t.after) for t in traces] == [
        ("strip", "  标题长  ", "标题长"),
        ("truncate", "标题长", "标题"),
    ]


def test_trace_logs_include_before_and_after(caplog: pytest.LogCaptureFixture) -> None:
    engine = _engine(
        _source_payload([{"name": "strip"}]),
        logger=logging.getLogger("crawl_flow.rules"),
    )
    with caplog.at_level(logging.DEBUG, logger="crawl_flow.rules"):
        engine.process("demo", "id-9", {"title": "  标题  "})
    assert "rule_trace" in caplog.text
    assert "before='  标题  '" in caplog.text
    assert "after='标题'" in caplog.text


def test_quality_gate_logs_warning(caplog: pytest.LogCaptureFixture) -> None:
    engine = _engine(
        _source_payload(
            [{"name": "required", "weight": 50}], threshold=60, mode="mark"
        ),
        logger=logging.getLogger("crawl_flow.rules"),
    )
    with caplog.at_level(logging.WARNING, logger="crawl_flow.rules"):
        engine.process("demo", "id-7", {"title": ""})
    assert "quality_gate" in caplog.text
    assert "action=mark" in caplog.text
    assert "score=50" in caplog.text


def test_engine_overhead_is_far_below_collection_time() -> None:
    # 性能约束：规则处理延迟须远低于单条采集总耗时的 10%。
    # 网络抓取单条通常 ≥10ms，这里验证 1 万条处理 < 1s（即单条 <0.1ms）。
    engine = _engine(
        _source_payload(
            [
                {"name": "strip"},
                {"name": "regex_replace", "params": {"pattern": r"\s+", "replacement": " "}},
                {"name": "truncate", "params": {"max_length": 100}},
                {"name": "required", "weight": 50},
                {"name": "length", "params": {"min": 2, "max": 100}, "weight": 10},
            ]
        )
    )
    fields = {"title": "  示例标题  ", "category": "技术", "content": "正文" * 100}
    engine.process("demo", "warmup", fields)  # 预热缓存
    started = time.perf_counter()
    for index in range(10_000):
        engine.process("demo", f"id-{index}", fields)
    elapsed = time.perf_counter() - started
    assert elapsed < 1.0, f"规则引擎性能回归：1 万条耗时 {elapsed:.2f}s"
