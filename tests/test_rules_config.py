"""规则配置加载与启动校验：语法错误、结构错误、算子参数错误。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from crawl_flow.errors import RuleConfigError
from crawl_flow.rules import RuleEngine, RuleSetConfig, load_rules


def _write_rules(tmp_path: Path, payload: Any) -> Path:
    path = tmp_path / "rules.yaml"
    if isinstance(payload, str):
        path.write_text(payload, encoding="utf-8")
    else:
        path.write_text(
            yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8"
        )
    return path


def _valid_payload() -> dict[str, Any]:
    return {
        "defaults": {"threshold": 60, "on_quality_failure": "mark"},
        "sources": {
            "demo_api": {
                "fields": {
                    "title": {
                        "rules": [
                            {"name": "strip"},
                            {"name": "truncate", "params": {"max_length": 100}},
                            {"name": "required", "weight": 50, "on_failure": "skip"},
                        ]
                    }
                }
            }
        },
    }


def test_load_valid_config(tmp_path: Path) -> None:
    config = load_rules(_write_rules(tmp_path, _valid_payload()))
    assert config.defaults.threshold == 60
    source_rules = config.sources["demo_api"]
    assert [rule.name for rule in source_rules.fields["title"].rules] == [
        "strip",
        "truncate",
        "required",
    ]
    # 引擎编译成功 = 启动校验通过
    engine = RuleEngine(config)
    assert engine.has_rules("demo_api")


def test_missing_file_raises_clear_error(tmp_path: Path) -> None:
    with pytest.raises(RuleConfigError, match="规则配置文件不存在"):
        load_rules(tmp_path / "nope.yaml")


def test_yaml_syntax_error_raises_clear_error(tmp_path: Path) -> None:
    path = _write_rules(tmp_path, "sources: [未闭合\n  bad: : :")
    with pytest.raises(RuleConfigError, match="YAML 解析失败"):
        load_rules(path)


def test_top_level_must_be_mapping(tmp_path: Path) -> None:
    path = _write_rules(tmp_path, "- 只是\n- 一个列表\n")
    with pytest.raises(RuleConfigError, match="顶层必须是映射"):
        load_rules(path)


def test_unknown_top_level_key_rejected(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["source"] = {}  # 拼写错误：应为 sources
    with pytest.raises(RuleConfigError, match="规则配置校验失败"):
        load_rules(_write_rules(tmp_path, payload))


def test_unknown_rule_name_rejected_at_compile(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["title"]["rules"].append(
        {"name": "不存在的算子"}
    )
    config = load_rules(_write_rules(tmp_path, payload))
    with pytest.raises(RuleConfigError, match="未知规则"):
        RuleEngine(config)


def test_bad_rule_params_rejected_at_compile(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["title"]["rules"][1] = {
        "name": "truncate",
        "params": {"max_lenght": 100},  # 拼写错误
    }
    config = load_rules(_write_rules(tmp_path, payload))
    with pytest.raises(RuleConfigError, match="参数错误"):
        RuleEngine(config)


def test_invalid_regex_rejected_at_compile(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["title"]["rules"].append(
        {"name": "regex_replace", "params": {"pattern": "([未闭合"}}
    )
    config = load_rules(_write_rules(tmp_path, payload))
    with pytest.raises(RuleConfigError, match="正则表达式无法编译"):
        RuleEngine(config)


def test_invalid_date_format_rejected_at_compile(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["title"]["rules"].append(
        {"name": "date_format", "params": {"output": "%Y-%Q-%d"}}
    )
    config = load_rules(_write_rules(tmp_path, payload))
    with pytest.raises(RuleConfigError, match="无效的日期格式"):
        RuleEngine(config)


def test_weight_and_threshold_range_validated(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["title"]["rules"][0]["weight"] = 101
    with pytest.raises(RuleConfigError, match="规则配置校验失败"):
        load_rules(_write_rules(tmp_path, payload))

    payload = _valid_payload()
    payload["defaults"]["threshold"] = -1
    with pytest.raises(RuleConfigError, match="规则配置校验失败"):
        load_rules(_write_rules(tmp_path, payload))


def test_invalid_failure_mode_rejected(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["defaults"]["on_quality_failure"] = "delete"  # 非法模式
    with pytest.raises(RuleConfigError, match="规则配置校验失败"):
        load_rules(_write_rules(tmp_path, payload))


def test_error_message_pinpoints_rule_location(tmp_path: Path) -> None:
    payload = _valid_payload()
    payload["sources"]["demo_api"]["fields"]["title"]["rules"].append(
        {"name": "enum", "params": {"values": []}}
    )
    config = load_rules(_write_rules(tmp_path, payload))
    with pytest.raises(RuleConfigError) as excinfo:
        RuleEngine(config)
    message = str(excinfo.value)
    assert "sources.demo_api.fields.title.rules[4]" in message


def test_programmatic_config_validated_on_engine_build() -> None:
    # 不经过 load_rules 直接构造的配置，引擎编译时同样校验
    config = RuleSetConfig.model_validate(
        {"sources": {"s": {"fields": {"f": {"rules": [{"name": "ghost"}]}}}}}
    )
    with pytest.raises(RuleConfigError, match="未知规则"):
        RuleEngine(config)
