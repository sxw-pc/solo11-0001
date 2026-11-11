"""规则引擎：执行顺序、配置合并、质量评分、失败处置与降级。"""

from __future__ import annotations

import time
from typing import Any

from crawl_flow.rules.config import RuleFileConfig
from crawl_flow.rules.engine import RuleEngine, RuleSet


def build_engine(
    fields: dict[str, list[dict[str, Any]]],
    *,
    threshold: int = 60,
    on_failure: str = "mark",
    source: str = "s1",
    defaults: dict[str, list[dict[str, Any]]] | None = None,
    source_overrides: dict[str, Any] | None = None,
) -> RuleEngine:
    payload: dict[str, Any] = {
        "quality_threshold": threshold,
        "on_failure": on_failure,
        "defaults": {"fields": defaults or {}},
        "sources": {"s1": {"fields": fields, **(source_overrides or {})}},
    }
    rule_set = RuleSet(RuleFileConfig.model_validate(payload))
    engine = rule_set.engine_for(source)
    assert engine is not None
    return engine


def run_engine(engine: RuleEngine, fields: dict[str, Any]):
    report = engine.new_report("item-1")
    cleaned = engine.clean_fields(fields, report)
    engine.validate_record(cleaned, report)
    return cleaned, engine.finalize(report)


# ---------------------------------------------------------------------------
# 执行顺序
# ---------------------------------------------------------------------------


def test_rules_run_in_configured_order() -> None:
    engine = build_engine(
        {
            "title": [
                {"rule": "regex_replace", "pattern": "B", "repl": "C", "order": 2},
                {"rule": "regex_replace", "pattern": "A", "repl": "B", "order": 1},
            ]
        }
    )
    cleaned, _ = run_engine(engine, {"title": "A"})
    # order=1 先把 A→B，order=2 再把 B→C；顺序颠倒结果会停在 B
    assert cleaned["title"] == "C"


def test_same_order_keeps_declaration_sequence() -> None:
    engine = build_engine(
        {
            "title": [
                {"rule": "regex_replace", "pattern": "x", "repl": "y"},
                {"rule": "regex_replace", "pattern": "y", "repl": "z"},
            ]
        }
    )
    cleaned, _ = run_engine(engine, {"title": "x"})
    assert cleaned["title"] == "z"


# ---------------------------------------------------------------------------
# defaults 与源级配置合并
# ---------------------------------------------------------------------------


def test_source_field_overrides_defaults_field() -> None:
    engine = build_engine(
        {"title": [{"rule": "truncate", "max": 2}]},
        defaults={"title": [{"rule": "truncate", "max": 100}], "url": [{"rule": "strip"}]},
    )
    cleaned, _ = run_engine(engine, {"title": "一二三四", "url": " http://x.test "})
    # 源级 title 规则整体替换 defaults；未覆盖的 url 仍走 defaults
    assert cleaned["title"] == "一二"
    assert cleaned["url"] == "http://x.test"


def test_source_threshold_and_action_override_global() -> None:
    engine = build_engine(
        {"title": [{"rule": "pattern", "regex": "OK"}]},
        threshold=60,
        on_failure="mark",
        source_overrides={"quality_threshold": 80, "on_failure": "alert"},
    )
    _, report = run_engine(engine, {"title": "BAD"})
    assert engine.threshold == 80
    assert report.action == "alert"


# ---------------------------------------------------------------------------
# 质量评分
# ---------------------------------------------------------------------------


def test_all_pass_scores_100() -> None:
    engine = build_engine(
        {
            "title": [{"rule": "required"}, {"rule": "length", "min": 1}],
            "category": [{"rule": "enum", "values": ["技术"]}],
        }
    )
    _, report = run_engine(engine, {"title": "t", "category": "技术"})
    assert report.score == 100
    assert report.action == "ok"
    assert not report.is_low_quality


def test_failed_error_rule_deducts_full_weight() -> None:
    engine = build_engine(
        {
            "title": [{"rule": "required"}],
            "category": [{"rule": "enum", "values": ["技术"]}],
        }
    )
    _, report = run_engine(engine, {"title": "t", "category": "体育"})
    # 两条规则各权重 1，enum 失败 → 50 分
    assert report.score == 50
    assert report.is_low_quality
    assert "validate:category:enum" in report.flags


def test_warning_severity_deducts_half_weight() -> None:
    engine = build_engine(
        {
            "title": [{"rule": "required"}],
            "url": [{"rule": "pattern", "regex": "^https://", "severity": "warning"}],
        }
    )
    _, report = run_engine(engine, {"title": "t", "url": "http://x.test"})
    # 总权重 2，warning 失败扣 0.5 → 75 分
    assert report.score == 75


def test_weight_changes_score_share() -> None:
    engine = build_engine(
        {
            "title": [{"rule": "required", "weight": 3}],
            "category": [{"rule": "enum", "values": ["技术"], "weight": 1}],
        }
    )
    _, report = run_engine(engine, {"title": "t", "category": "体育"})
    # 扣 1/4 → 75 分
    assert report.score == 75


def test_score_below_threshold_triggers_configured_action() -> None:
    for on_failure in ("skip", "mark", "alert"):
        engine = build_engine(
            {"category": [{"rule": "enum", "values": ["技术"]}]},
            on_failure=on_failure,
        )
        _, report = run_engine(engine, {"category": "体育"})
        assert report.score == 0
        assert report.action == on_failure
        assert "low_quality" in report.flags


def test_score_above_threshold_keeps_action_ok() -> None:
    engine = build_engine(
        {
            "title": [{"rule": "required"}],
            "category": [
                {"rule": "enum", "values": ["技术"], "severity": "warning"}
            ],
        },
        threshold=60,
    )
    _, report = run_engine(engine, {"title": "t", "category": "体育"})
    assert report.score == 75
    assert report.action == "ok"
    assert "low_quality" not in report.flags


def test_empty_value_skips_non_required_validators_without_scoring() -> None:
    engine = build_engine(
        {
            "title": [{"rule": "required"}],
            "category": [{"rule": "enum", "values": ["技术"]}],
        }
    )
    _, report = run_engine(engine, {"title": "t", "category": None})
    skipped = [t for t in report.traces if t.status == "skipped"]
    assert len(skipped) == 1 and skipped[0].rule == "enum"
    # enum 未计入总分，required 通过 → 100
    assert report.score == 100


def test_required_failure_on_empty_value() -> None:
    engine = build_engine({"title": [{"rule": "required"}]})
    _, report = run_engine(engine, {"title": "   "})
    assert report.score == 0
    assert report.violations


# ---------------------------------------------------------------------------
# 清洗失败与算子异常：降级保留原值
# ---------------------------------------------------------------------------


def test_clean_failure_keeps_original_value() -> None:
    engine = build_engine({"published_at": [{"rule": "date_format"}]})
    cleaned, report = run_engine(engine, {"published_at": "不是日期"})
    assert cleaned["published_at"] == "不是日期"
    failed = [t for t in report.traces if t.status == "failed"]
    assert len(failed) == 1 and failed[0].rule == "date_format"
    assert report.score < 100


def test_operator_crash_is_contained_and_scored() -> None:
    import dataclasses

    engine = build_engine({"title": [{"rule": "strip"}]})
    # 直接破坏已编译闭包，模拟算子内部未预期异常
    engine.clean_rules[0] = dataclasses.replace(
        engine.clean_rules[0], func=lambda v: 1 / 0
    )
    cleaned, report = run_engine(engine, {"title": "  abc  "})
    assert cleaned["title"] == "  abc  "  # 原值保留
    error_traces = [t for t in report.traces if t.status == "error"]
    assert len(error_traces) == 1
    assert "算子异常" in error_traces[0].message


def test_list_value_applies_elementwise_rules() -> None:
    engine = build_engine(
        {"tags": [{"rule": "strip"}, {"rule": "truncate", "max": 2}]}
    )
    cleaned, _ = run_engine(engine, {"tags": [" 公园 ", "花期观测"]})
    assert cleaned["tags"] == ["公园", "花期"]


# ---------------------------------------------------------------------------
# 可追溯性
# ---------------------------------------------------------------------------


def test_traces_record_before_and_after_for_every_field() -> None:
    engine = build_engine(
        {
            "title": [{"rule": "strip"}, {"rule": "required"}],
            "category": [{"rule": "enum", "values": ["技术"]}],
        }
    )
    _, report = run_engine(engine, {"title": "  标题  ", "category": "体育"})
    by_rule = {(t.field, t.rule): t for t in report.traces}
    assert by_rule[("title", "strip")].before == "  标题  "
    assert by_rule[("title", "strip")].after == "标题"
    assert by_rule[("title", "strip")].status == "ok"
    enum_trace = by_rule[("category", "enum")]
    assert enum_trace.status == "failed"
    assert enum_trace.before == "体育"
    assert enum_trace.message
    assert report.violations


def test_long_values_are_truncated_in_traces() -> None:
    engine = build_engine({"content": [{"rule": "required"}]})
    _, report = run_engine(engine, {"content": "长" * 500})
    assert all(len(t.before) <= 121 for t in report.traces)


# ---------------------------------------------------------------------------
# 性能：单条开销必须远低于采集总耗时的 10%
# ---------------------------------------------------------------------------


def test_engine_overhead_is_sub_millisecond_per_item() -> None:
    engine = build_engine(
        {
            "title": [{"rule": "strip"}, {"rule": "truncate", "max": 200}, {"rule": "required"}],
            "category": [{"rule": "strip"}, {"rule": "enum", "values": ["技术", "环境"]}],
            "tags": [{"rule": "strip"}, {"rule": "truncate", "max": 20}],
            "published_at": [{"rule": "date_format"}, {"rule": "date_parseable"}],
            "content": [{"rule": "regex_replace", "pattern": r"\s+", "repl": " "}],
            "url": [{"rule": "pattern", "regex": "^https?://", "severity": "warning"}],
        }
    )
    fields = {
        "title": "  城市公园春季花期观测报告  ",
        "category": " 技术 ",
        "tags": [" 公园 ", "花期"],
        "published_at": "2024年03月01日",
        "content": "第一段\n第二段",
        "url": "http://example.test/1",
    }
    # 预热
    for _ in range(50):
        run_engine(engine, fields)
    rounds = 1000
    started = time.perf_counter()
    for _ in range(rounds):
        run_engine(engine, fields)
    avg_ms = (time.perf_counter() - started) / rounds * 1000
    # 单条采集（一次 HTTP 详情请求）通常 ≥10ms，10% 即 1ms；
    # 这里断言平均开销 < 1ms，留足余量。
    assert avg_ms < 1.0, f"规则引擎单条开销 {avg_ms:.3f}ms 超过 1ms 预算"
