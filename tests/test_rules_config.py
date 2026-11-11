"""规则配置：加载、启动时校验与明确的错误提示。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from crawl_flow.errors import ConfigError
from crawl_flow.rules.config import load_rule_config
from crawl_flow.rules.engine import RuleSet


def _write(tmp_path: Path, payload: Any, name: str = "rules.yaml") -> Path:
    path = tmp_path / name
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(
            yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
    return path


def _valid_payload() -> dict[str, Any]:
    return {
        "version": 1,
        "quality_threshold": 60,
        "on_failure": "mark",
        "defaults": {"fields": {"title": [{"rule": "strip"}]}},
        "sources": {
            "demo_api": {
                "quality_threshold": 70,
                "fields": {"category": [{"rule": "enum", "values": ["技术"]}]},
            }
        },
    }


def test_valid_config_loads_and_compiles(tmp_path: Path) -> None:
    config = load_rule_config(_write(tmp_path, _valid_payload()))
    assert config.quality_threshold == 60
    rule_set = RuleSet(config)
    engine = rule_set.engine_for("demo_api")
    assert engine is not None and engine.threshold == 70
    # 未单独配置的源也能用 defaults 规则
    default_engine = rule_set.engine_for("other_source")
    assert default_engine is not None and default_engine.threshold == 60


def test_missing_file_raises_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="规则配置文件不存在"):
        load_rule_config(tmp_path / "nope.yaml")


def test_bad_yaml_syntax_raises_config_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "defaults:\n  fields: [unclosed\n")
    with pytest.raises(ConfigError, match="YAML 解析失败"):
        load_rule_config(path)


def test_empty_file_raises_config_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "")
    with pytest.raises(ConfigError, match="规则配置为空"):
        load_rule_config(path)


def test_non_mapping_top_level_raises_config_error(tmp_path: Path) -> None:
    path = _write(tmp_path, "- just\n- a\n- list\n")
    with pytest.raises(ConfigError, match="顶层必须是映射"):
        load_rule_config(path)


def test_unknown_top_level_key_raises_config_error(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["typo_key"] = 1
    with pytest.raises(ConfigError, match="校验失败"):
        load_rule_config(_write(tmp_path, payload))


def test_unknown_field_name_raises_config_error(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["defaults"]["fields"]["titel"] = [{"rule": "strip"}]
    with pytest.raises(ConfigError, match="未知字段 'titel'"):
        load_rule_config(_write(tmp_path, payload))


def test_invalid_on_failure_raises_config_error(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["on_failure"] = "explode"
    with pytest.raises(ConfigError, match="校验失败"):
        load_rule_config(_write(tmp_path, payload))


def test_threshold_out_of_range_raises_config_error(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["quality_threshold"] = 120
    with pytest.raises(ConfigError, match="校验失败"):
        load_rule_config(_write(tmp_path, payload))


def test_unknown_rule_name_fails_at_compile_with_location(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["category"] = [{"rule": "magic"}]
    config = load_rule_config(_write(tmp_path, payload))
    with pytest.raises(ConfigError, match=r"sources\.demo_api\.fields\.category\[0\]"):
        RuleSet(config).engine_for("demo_api")


def test_bad_operator_params_fail_at_compile(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["category"] = [
        {"rule": "truncate"}  # 缺少 max
    ]
    config = load_rule_config(_write(tmp_path, payload))
    with pytest.raises(ConfigError, match="max"):
        RuleSet(config).engine_for("demo_api")


def test_invalid_regex_fails_at_startup_not_runtime(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["category"] = [
        {"rule": "regex_replace", "pattern": "(["}
    ]
    config = load_rule_config(_write(tmp_path, payload))
    with pytest.raises(ConfigError, match="正则表达式无效"):
        RuleSet(config).engine_for("demo_api")


def test_disabled_rule_is_skipped(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["category"] = [
        {"rule": "enum", "values": ["技术"], "enabled": False}
    ]
    rule_set = RuleSet(load_rule_config(_write(tmp_path, payload)))
    engine = rule_set.engine_for("demo_api")
    assert engine is not None
    assert all(rule.name != "enum" for rule in engine.validate_rules)


def test_source_without_any_rules_gets_no_engine(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["defaults"] = {"fields": {}}
    payload["sources"] = {}
    rule_set = RuleSet(load_rule_config(_write(tmp_path, payload)))
    assert rule_set.engine_for("anything") is None
