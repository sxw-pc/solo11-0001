"""规则算子：12 个内置算子的行为与参数校验。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from crawl_flow.rules.operators import OPERATORS, RuleFailed


def _apply(name: str, field_value: object, **params: object) -> object:
    spec = OPERATORS[name]
    parsed = spec.params_model.model_validate(params)
    return spec.func(field_value, parsed)


def test_operator_registry_covers_required_rules() -> None:
    # 需求要求的截断、正则替换、日期转换、枚举校验等至少 6 种
    expected = {
        "strip",
        "truncate",
        "regex_replace",
        "date_format",
        "default",
        "type_cast",
        "map_values",
        "required",
        "enum",
        "length",
        "pattern",
        "range",
    }
    assert expected <= set(OPERATORS)


def test_strip_trims_and_collapses_whitespace() -> None:
    assert _apply("strip", "  城市 \n\t 公园  ") == "城市 公园"
    assert _apply("strip", None) == ""
    assert _apply("strip", " a  b ", collapse=False) == "a  b"


def test_truncate_cuts_long_text_with_suffix() -> None:
    assert _apply("truncate", "一二三四五", max_length=4) == "一二三四"
    assert _apply("truncate", "一二三四五", max_length=4, suffix="…") == "一二三…"
    assert _apply("truncate", "短", max_length=10) == "短"
    assert _apply("truncate", None, max_length=3) == ""


def test_truncate_rejects_bad_params() -> None:
    with pytest.raises(ValidationError):
        _apply("truncate", "x", max_length=0)
    with pytest.raises(ValidationError):
        _apply("truncate", "x", max_length=2, suffix="...")  # suffix 不比 max 短


def test_regex_replace_substitutes_and_limits_count() -> None:
    assert _apply("regex_replace", "a1b2c3", pattern=r"\d", replacement="#") == "a#b#c#"
    assert (
        _apply("regex_replace", "a1b2c3", pattern=r"\d", replacement="#", count=1)
        == "a#b2c3"
    )
    assert _apply("regex_replace", " 多  余 空 格 ", pattern=r"\s+", replacement=" ") == " 多 余 空 格 "


def test_regex_replace_rejects_invalid_pattern_at_validation() -> None:
    with pytest.raises(ValidationError) as excinfo:
        _apply("regex_replace", "x", pattern="([未闭合")
    assert "正则表达式无法编译" in str(excinfo.value)


def test_date_format_converts_supported_formats() -> None:
    assert _apply("date_format", "2024-03-01T09:30:00+08:00") == "2024-03-01 09:30:00"
    assert _apply("date_format", "2024年03月02日") == "2024-03-02 00:00:00"
    assert _apply("date_format", "2024/03/03 10:00:00") == "2024-03-03 10:00:00"
    assert _apply("date_format", "2024.03.04") == "2024-03-04 00:00:00"
    assert _apply("date_format", "2024-03-05", output="iso") == "2024-03-05T00:00:00"
    # 自定义输入格式
    assert (
        _apply("date_format", "03/06/2024", formats=["%m/%d/%Y"]) == "2024-03-06 00:00:00"
    )


def test_date_format_fails_on_unparseable_or_empty() -> None:
    with pytest.raises(RuleFailed):
        _apply("date_format", "不是日期")
    with pytest.raises(RuleFailed):
        _apply("date_format", "   ")
    with pytest.raises(RuleFailed):
        _apply("date_format", None)


def test_date_format_rejects_invalid_format_string() -> None:
    with pytest.raises(ValidationError) as excinfo:
        _apply("date_format", "2024-01-01", output="%Y-%Q-%d")
    assert "无效的日期格式" in str(excinfo.value)


def test_default_fills_only_empty_values() -> None:
    assert _apply("default", None, value="未知") == "未知"
    assert _apply("default", "  ", value="未知") == "未知"
    assert _apply("default", [], value="未知") == "未知"
    assert _apply("default", "已有", value="未知") == "已有"
    assert _apply("default", 0, value="未知") == 0  # 0 不是空值


def test_type_cast_converts_common_types() -> None:
    assert _apply("type_cast", 42, to="str") == "42"
    assert _apply("type_cast", "42", to="int") == 42
    assert _apply("type_cast", 3.0, to="int") == 3
    assert _apply("type_cast", "3.5", to="float") == 3.5
    assert _apply("type_cast", "YES", to="bool") is True
    assert _apply("type_cast", "off", to="bool") is False
    assert _apply("type_cast", ("a", "b"), to="list") == ["a", "b"]
    assert _apply("type_cast", "单值", to="list") == ["单值"]


def test_type_cast_fails_on_incompatible_value() -> None:
    with pytest.raises(RuleFailed):
        _apply("type_cast", "三点五", to="float")
    with pytest.raises(RuleFailed):
        _apply("type_cast", "3.5", to="int")
    with pytest.raises(RuleFailed):
        _apply("type_cast", "也许", to="bool")


def test_map_values_maps_and_handles_missing() -> None:
    mapping = {"新闻": "news", "公告": "notice"}
    assert _apply("map_values", "新闻", mapping=mapping) == "news"
    assert _apply("map_values", "其他", mapping=mapping) == "其他"  # keep
    assert _apply("map_values", "其他", mapping=mapping, on_missing="default", default="misc") == "misc"
    with pytest.raises(RuleFailed):
        _apply("map_values", "其他", mapping=mapping, on_missing="fail")


def test_required_rejects_empty_and_accepts_values() -> None:
    for empty in (None, "", "   ", [], {}):
        with pytest.raises(RuleFailed):
            _apply("required", empty)
    assert _apply("required", "有值") == "有值"
    assert _apply("required", 0) == 0


def test_enum_validates_membership_and_case() -> None:
    values = ["环境", "技术"]
    assert _apply("enum", "环境", values=values) == "环境"
    with pytest.raises(RuleFailed):
        _apply("enum", "体育", values=values)
    with pytest.raises(RuleFailed):
        _apply("enum", "News", values=["news"])
    assert _apply("enum", "News", values=["news"], case_sensitive=False) == "News"


def test_enum_rejects_empty_values_list() -> None:
    with pytest.raises(ValidationError):
        _apply("enum", "x", values=[])


def test_length_checks_string_and_collection_bounds() -> None:
    assert _apply("length", "标题", min=2, max=10) == "标题"
    assert _apply("length", ["a", "b"], min=1, max=3) == ["a", "b"]
    with pytest.raises(RuleFailed):
        _apply("length", "短", min=2)
    with pytest.raises(RuleFailed):
        _apply("length", "太长了太长了", max=3)
    with pytest.raises(ValidationError):
        _apply("length", "x")  # min/max 都没给


def test_pattern_matches_and_negates() -> None:
    assert _apply("pattern", "abc123", pattern=r"\d+") == "abc123"
    with pytest.raises(RuleFailed):
        _apply("pattern", "abc", pattern=r"^\d+$")
    assert _apply("pattern", "abc", pattern=r"\d+", must_match=False) == "abc"
    with pytest.raises(RuleFailed):
        _apply("pattern", "abc123", pattern=r"\d+", must_match=False)


def test_range_checks_numeric_bounds() -> None:
    assert _apply("range", "5", min=1, max=10) == "5"
    with pytest.raises(RuleFailed):
        _apply("range", "0", min=1)
    with pytest.raises(RuleFailed):
        _apply("range", "11", max=10)
    with pytest.raises(RuleFailed):
        _apply("range", "不是数", min=0)


def test_params_reject_unknown_keys() -> None:
    # extra="forbid"：参数拼写错误在启动校验时暴露
    with pytest.raises(ValidationError):
        _apply("truncate", "x", max_length=5, suffx="…")
