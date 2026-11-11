"""规则引擎测试：启动期校验、分层合并、质量评分、处置模式、降级与轨迹。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from crawl_flow.rules import RuleConfigError, load_rules


def _write(tmp_path: Path, payload: str) -> Path:
    path = tmp_path / "rules.yaml"
    path.write_text(payload, encoding="utf-8")
    return path


BASE_RAW: dict[str, Any] = {
    "title": " 标题 带 空格 ",
    "category": "环境",
    "tags": "公园、花期",
    "published_at": "2024-03-01",
    "content": "<p>正文</p>",
    "url": "http://x.test/1",
}


# ---------------------------------------------------------------------------
# 启动期校验
# ---------------------------------------------------------------------------


def test_missing_config_returns_degraded_engine(tmp_path: Path) -> None:
    engine = load_rules(tmp_path / "absent.yaml")
    assert engine.missing is True
    result = engine.process("any_source", "id1", dict(BASE_RAW))
    # 空引擎等价于默认清洗
    assert result.values["title"] == "标题 带 空格"
    assert result.quality_score == 100.0
    assert result.action == "pass"


def test_invalid_yaml_raises_config_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("version: 1\nfields:\n  title:\n   rules: [\n", encoding="utf-8")
    with pytest.raises(RuleConfigError, match="YAML"):
        load_rules(path)


def test_unknown_top_level_key_rejected(tmp_path: Path) -> None:
    path = _write(tmp_path, "version: 1\nmystery: 1\n")
    with pytest.raises(RuleConfigError) as excinfo:
        load_rules(path)
    assert any("mystery" in item for item in excinfo.value.errors)


def test_all_semantic_errors_are_aggregated(tmp_path: Path) -> None:
    payload = """
version: 1
fields:
  bogus_field:
    rules: [{type: trim}]
  title:
    rules:
      - {type: truncate, params: {}}
      - {type: ghost}
    validations:
      - {type: trim}
"""
    with pytest.raises(RuleConfigError) as excinfo:
        load_rules(_write(tmp_path, payload))
    joined = "\n".join(excinfo.value.errors)
    assert "bogus_field" in joined
    assert "max_length" in joined
    assert "ghost" in joined
    assert "清洗算子，不能用在 validations" in joined


def test_bad_regex_reported_at_load_time(tmp_path: Path) -> None:
    payload = """
version: 1
fields:
  title:
    rules:
      - type: regex_replace
        params: {pattern: '([0-9'}
"""
    with pytest.raises(RuleConfigError) as excinfo:
        load_rules(_write(tmp_path, payload))
    assert "正则表达式非法" in "\n".join(excinfo.value.errors)


# ---------------------------------------------------------------------------
# 规则执行与顺序
# ---------------------------------------------------------------------------


def test_rules_execute_in_configured_order(tmp_path: Path) -> None:
    payload = """
version: 1
fields:
  title:
    type_hint: text
    rules:
      - {type: trim}
      - {type: truncate, params: {max_length: 6}}
      - {type: case, params: {mode: upper}}
"""
    engine = load_rules(_write(tmp_path, payload))
    result = engine.process("s", "id", {**BASE_RAW, "title": "  abcdefghij "})
    assert result.values["title"] == "ABCDEF"
    names = [t.rule for t in result.trace if t.phase == "clean"]
    assert names == ["trim", "truncate", "case"]


def test_trace_contains_before_and_after_values(tmp_path: Path) -> None:
    payload = """
version: 1
fields:
  title:
    type_hint: text
    rules:
      - {type: trim}
"""
    engine = load_rules(_write(tmp_path, payload))
    result = engine.process("s", "id", dict(BASE_RAW))
    entry = next(t for t in result.trace if t.field == "title" and t.phase == "clean")
    assert entry.before == " 标题 带 空格 "
    assert entry.after == "标题 带 空格"
    assert entry.passed is True
    # 轨迹可 JSON 序列化（raw 快照需要）
    import json

    json.dumps(result.trace_as_dicts(), ensure_ascii=False)


def test_tags_rules_apply_element_wise(tmp_path: Path) -> None:
    payload = """
version: 1
fields:
  tags:
    type_hint: tags
    rules:
      - {type: split_tags}
      - {type: case, params: {mode: upper}}
"""
    engine = load_rules(_write(tmp_path, payload))
    result = engine.process("s", "id", {**BASE_RAW, "tags": ["alpha", "beta"]})
    assert result.values["tags"] == ["ALPHA", "BETA"]
    # 字符串输入先 split_tags（自带去重）再逐元素处理
    result2 = engine.process("s", "id2", {**BASE_RAW, "tags": "alpha,beta,alpha"})
    assert result2.values["tags"] == ["ALPHA", "BETA"]


# ---------------------------------------------------------------------------
# 运行期失败降级
# ---------------------------------------------------------------------------


def test_rule_failure_keep_preserves_original_value(tmp_path: Path) -> None:
    payload = """
version: 1
fields:
  published_at:
    type_hint: datetime
    rules:
      - type: date_format
        on_fail: keep
        params: {to_format: '%Y-%m-%d'}
"""
    engine = load_rules(_write(tmp_path, payload))
    result = engine.process("s", "id", {**BASE_RAW, "published_at": "无法解析"})
    # keep 后引擎仍尝试默认 datetime 解析，失败得到 None，主流程不受影响
    assert result.values["published_at"] is None
    assert any(t.degraded and t.rule == "date_format" for t in result.trace)


def test_rule_failure_default_falls_back_to_default_cleaning(tmp_path: Path) -> None:
    payload = """
version: 1
fields:
  title:
    type_hint: text
    rules:
      - {type: trim}
      - type: date_format
        on_fail: default
        params: {to_format: '%Y-%m-%d'}
"""
    engine = load_rules(_write(tmp_path, payload))
    result = engine.process("s", "id", {**BASE_RAW, "title": "  普通标题  "})
    assert result.values["title"] == "普通标题"
    assert "title" in result.degraded_fields
    assert any(t.rule == "<default-fallback>" for t in result.trace)


def test_engine_internal_exception_never_propagates(tmp_path: Path) -> None:
    engine = load_rules(
        _write(
            tmp_path,
            """
version: 1
fields:
  title:
    type_hint: text
    rules: [{type: trim}]
""",
        )
    )

    def boom(value: Any) -> Any:
        raise RuntimeError("unexpected engine failure")

    # 破坏已编译算子，模拟引擎内部意外异常（frozen dataclass 用 object.__setattr__）。
    rule = engine.profile_for("s").fields["title"].rules[0]
    object.__setattr__(rule, "fn", boom)
    result = engine.process("s", "id", dict(BASE_RAW))
    assert result.action == "pass"
    assert "title" in result.degraded_fields
    assert result.values["title"] == "标题 带 空格"


# ---------------------------------------------------------------------------
# 质量评分与处置模式
# ---------------------------------------------------------------------------


def _scoring_config(action: str) -> str:
    return """
version: 1
quality: {threshold: 80, on_validation_fail: ACTION}
fields:
  title:
    type_hint: text
    weight: 3
    rules: [{type: trim}]
    validations:
      - type: required
      - type: length
        params: {min: 10}
  category:
    type_hint: text
    weight: 1
    validations:
      - type: enum
        params: {values: [环境, 技术]}
""".replace("ACTION", action)


def test_score_is_weighted_by_rule_and_field_weights(tmp_path: Path) -> None:
    engine = load_rules(_write(tmp_path, _scoring_config("mark")))
    # title 两条校验各权重 1（default_weight），通过 1/2 → title 50 分，字段权重 3
    # category 枚举失败 → 0 分，字段权重 1
    result = engine.process(
        "s", "id", {**BASE_RAW, "title": "短标题", "category": "其他"}
    )
    # title: 50 * 3 + category: 0 * 1，总分 150/4 = 37.5
    assert result.field_scores["title"] == 50.0
    assert result.field_scores["category"] == 0.0
    assert result.quality_score == 37.5
    assert result.low_quality is True
    assert result.action == "mark"


def test_action_warn_mark_and_skip(tmp_path: Path) -> None:
    raw = {**BASE_RAW, "title": "短", "category": "其他"}
    for action in ("warn", "mark", "skip"):
        sub = tmp_path / action
        sub.mkdir()
        engine = load_rules(_write(sub, _scoring_config(action)))
        result = engine.process("s", "id", raw)
        assert result.action == action
        assert all(f["action"] == action for f in result.validation_failures)


def test_no_validations_means_full_score(tmp_path: Path) -> None:
    engine = load_rules(
        _write(
            tmp_path,
            """
version: 1
fields:
  title:
    type_hint: text
    rules: [{type: trim}]
""",
        )
    )
    result = engine.process("s", "id", dict(BASE_RAW))
    assert result.quality_score == 100.0
    assert result.low_quality is False


def test_per_validation_action_overrides_global(tmp_path: Path) -> None:
    payload = """
version: 1
quality: {threshold: 60, on_validation_fail: mark}
fields:
  category:
    type_hint: text
    validations:
      - type: enum
        on_fail: warn
        params: {values: [环境]}
  title:
    type_hint: text
    validations:
      - type: required
"""
    engine = load_rules(_write(tmp_path, payload))
    result = engine.process("s", "id", {**BASE_RAW, "category": "其他"})
    actions = {f["rule"]: f["action"] for f in result.validation_failures}
    assert actions["enum"] == "warn"


# ---------------------------------------------------------------------------
# 分层合并：全局规则 + 来源覆盖
# ---------------------------------------------------------------------------


def test_source_overrides_global_and_unknown_source_uses_global(tmp_path: Path) -> None:
    payload = """
version: 1
quality: {threshold: 60}
fields:
  title:
    type_hint: text
    rules:
      - {type: trim}
      - {type: truncate, params: {max_length: 100}}
sources:
  special:
    quality: {threshold: 90}
    fields:
      title:
        type_hint: text
        rules:
          - {type: trim}
"""
    engine = load_rules(_write(tmp_path, payload))
    special = engine.profile_for("special")
    assert special.quality.threshold == 90.0
    assert [r.type for r in special.fields["title"].rules] == ["trim"]

    fallback = engine.profile_for("not_configured")
    assert fallback.quality.threshold == 60.0
    assert [r.type for r in fallback.fields["title"].rules] == ["trim", "truncate"]


def test_field_defaults_apply_to_unlisted_fields(tmp_path: Path) -> None:
    payload = """
version: 1
field_defaults:
  rules:
    - {type: trim}
fields:
  title:
    type_hint: text
    rules:
      - {type: truncate, params: {max_length: 10}}
"""
    engine = load_rules(_write(tmp_path, payload))
    profile = engine.profile_for("*")
    assert [r.type for r in profile.fields["title"].rules] == ["truncate"]
    # 未显式配置的字段继承 field_defaults
    assert [r.type for r in profile.fields["category"].rules] == ["trim"]
    assert [r.type for r in profile.fields["url"].rules] == ["trim"]
