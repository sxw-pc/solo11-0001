"""规则算子：每个算子的正常行为、边界与参数校验。"""

from __future__ import annotations

import pytest

from crawl_flow.errors import ConfigError
from crawl_flow.rules.operators import (
    OPERATORS,
    CleanFailed,
    RuleViolation,
    get_operator,
)

WHERE = "test.fields.f[0]"


def prepare(name: str, **params):
    return get_operator(name, "test").prepare(params, WHERE)


# ---------------------------------------------------------------------------
# strip
# ---------------------------------------------------------------------------


def test_strip_collapses_whitespace() -> None:
    apply = prepare("strip")
    assert apply("  城 市 \n\t 公园  ") == "城 市 公园"
    assert apply(None) == ""


def test_strip_multiline_keeps_paragraphs() -> None:
    apply = prepare("strip", multiline=True)
    assert apply(" 第一段 \n\n 第二段 ") == "第一段\n第二段"


def test_strip_rejects_bad_param() -> None:
    with pytest.raises(ConfigError, match="不支持的参数"):
        prepare("strip", unknown=1)


# ---------------------------------------------------------------------------
# truncate
# ---------------------------------------------------------------------------


def test_truncate_shortens_long_text() -> None:
    apply = prepare("truncate", max=5)
    assert apply("一二三四五六七") == "一二三四五"
    assert apply("一二三") == "一二三"


def test_truncate_with_suffix_counts_into_max() -> None:
    apply = prepare("truncate", max=5, suffix="…")
    assert apply("一二三四五六七") == "一二三四…"


def test_truncate_requires_max() -> None:
    with pytest.raises(ConfigError, match="max"):
        prepare("truncate")
    with pytest.raises(ConfigError, match="max"):
        prepare("truncate", max=0)


# ---------------------------------------------------------------------------
# regex_replace
# ---------------------------------------------------------------------------


def test_regex_replace_removes_and_rewrites() -> None:
    apply = prepare("regex_replace", pattern=r"\s+", repl="")
    assert apply("a b\tc") == "abc"
    apply2 = prepare("regex_replace", pattern=r"\d+", repl="#")
    assert apply2("第1次和第22次") == "第#次和第#次"


def test_regex_replace_count_limits_substitutions() -> None:
    apply = prepare("regex_replace", pattern=r"\d", repl="#", count=1)
    assert apply("a1b2") == "a#b2"


def test_regex_replace_invalid_pattern_fails_at_prepare() -> None:
    with pytest.raises(ConfigError, match="正则表达式无效"):
        prepare("regex_replace", pattern="([")
    with pytest.raises(ConfigError, match="pattern"):
        prepare("regex_replace")


# ---------------------------------------------------------------------------
# date_format
# ---------------------------------------------------------------------------


def test_date_format_outputs_iso_by_default() -> None:
    apply = prepare("date_format")
    assert apply("2024年03月02日") == "2024-03-02T00:00:00"
    assert apply("2024/03/03 10:00:00") == "2024-03-03T10:00:00"


def test_date_format_custom_output_and_input_formats() -> None:
    apply = prepare("date_format", output="%Y-%m-%d")
    assert apply("2024-03-05 09:30") == "2024-03-05"
    apply2 = prepare("date_format", input_formats=["%d/%m/%Y"], output="%Y-%m-%d")
    assert apply2("05/03/2024") == "2024-03-05"


def test_date_format_unparseable_raises_clean_failed() -> None:
    apply = prepare("date_format")
    with pytest.raises(CleanFailed, match="无法解析日期"):
        apply("不是日期")
    # 空值不视为失败，原样返回交给后续默认清洗
    assert apply("") == ""
    assert apply(None) is None


# ---------------------------------------------------------------------------
# case / default / type_cast / map_values / html_strip
# ---------------------------------------------------------------------------


def test_case_modes() -> None:
    assert prepare("case", mode="lower")("AbC") == "abc"
    assert prepare("case", mode="upper")("AbC") == "ABC"
    assert prepare("case", mode="title")("hello world") == "Hello World"
    with pytest.raises(ConfigError, match="mode"):
        prepare("case", mode="swapcase")


def test_default_fills_only_empty() -> None:
    apply = prepare("default", value="未分类")
    assert apply(None) == "未分类"
    assert apply("  ") == "未分类"
    assert apply("技术") == "技术"
    with pytest.raises(ConfigError, match="value"):
        prepare("default")


def test_type_cast_converts_scalars() -> None:
    assert prepare("type_cast", to="int")("42") == 42
    assert prepare("type_cast", to="float")("3.5") == 3.5
    assert prepare("type_cast", to="str")(7) == "7"
    assert prepare("type_cast", to="bool")("yes") is True
    assert prepare("type_cast", to="bool")("0") is False


def test_type_cast_failure_raises_clean_failed() -> None:
    with pytest.raises(CleanFailed, match="类型转换失败"):
        prepare("type_cast", to="int")("abc")
    with pytest.raises(ConfigError, match="to"):
        prepare("type_cast", to="complex")


def test_map_values_hit_and_miss_policies() -> None:
    apply = prepare("map_values", mapping={"技术类": "技术"})
    assert apply("技术类") == "技术"
    assert apply("其他") == "其他"  # 默认 keep
    apply_null = prepare("map_values", mapping={"a": 1}, on_miss="null")
    assert apply_null("zzz") is None
    apply_fail = prepare("map_values", mapping={"a": 1}, on_miss="fail")
    with pytest.raises(CleanFailed, match="不在映射表"):
        apply_fail("zzz")
    with pytest.raises(ConfigError, match="mapping"):
        prepare("map_values", mapping={})


def test_html_strip_removes_tags() -> None:
    apply = prepare("html_strip")
    assert apply("<p>第一段</p><p>第二段</p>") == "第一段\n第二段"
    assert apply(None) is None


# ---------------------------------------------------------------------------
# required / enum / length / pattern / range / date_parseable
# ---------------------------------------------------------------------------


def test_required_rejects_empty() -> None:
    apply = prepare("required")
    apply("x")
    apply(["a"])
    with pytest.raises(RuleViolation):
        apply("")
    with pytest.raises(RuleViolation):
        apply(None)
    with pytest.raises(RuleViolation):
        apply([])


def test_enum_membership() -> None:
    apply = prepare("enum", values=["环境", "技术"])
    apply("环境")
    with pytest.raises(RuleViolation, match="不在枚举"):
        apply("体育")
    with pytest.raises(ConfigError, match="values"):
        prepare("enum", values=[])


def test_length_bounds_for_text_and_list() -> None:
    apply = prepare("length", min=2, max=3)
    apply("ab")
    apply(["a", "b", "c"])
    with pytest.raises(RuleViolation, match="小于下限"):
        apply("a")
    with pytest.raises(RuleViolation, match="超过上限"):
        apply("abcd")
    with pytest.raises(ConfigError, match="min 或 max"):
        prepare("length")
    with pytest.raises(ConfigError, match="min 不能大于 max"):
        prepare("length", min=5, max=2)


def test_pattern_match() -> None:
    apply = prepare("pattern", regex=r"^https?://")
    apply("https://example.test")
    with pytest.raises(RuleViolation, match="不匹配正则"):
        apply("ftp://example.test")
    with pytest.raises(ConfigError, match="正则表达式无效"):
        prepare("pattern", regex="(")


def test_range_numeric_bounds() -> None:
    apply = prepare("range", min=0, max=100)
    apply("50")
    with pytest.raises(RuleViolation, match="小于下限"):
        apply(-1)
    with pytest.raises(RuleViolation, match="超过上限"):
        apply(101)
    with pytest.raises(RuleViolation, match="不是数字"):
        apply("abc")
    with pytest.raises(ConfigError, match="min 或 max"):
        prepare("range")


def test_date_parseable() -> None:
    apply = prepare("date_parseable")
    apply("2024-03-01")
    apply("2024年03月02日")
    with pytest.raises(RuleViolation, match="不是可识别的日期"):
        apply("昨天")


def test_unknown_operator_name_lists_available() -> None:
    with pytest.raises(ConfigError, match="未知规则"):
        get_operator("nope", "test.fields.x[0]")


def test_registry_has_at_least_six_common_rules() -> None:
    required = {"truncate", "regex_replace", "date_format", "enum"}
    assert required <= set(OPERATORS)
    assert len(OPERATORS) >= 6
