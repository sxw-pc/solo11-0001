"""规则算子单元测试：11 种清洗算子 + 6 种校验算子的正常路径与参数错误。"""

from __future__ import annotations

from datetime import datetime

import pytest

from crawl_flow.rules.errors import RuleExecutionError, ValidationFailure
from crawl_flow.rules.operators import compile_operator, operator_names


def _clean(name: str, params: dict | None = None):
    kind, fn = compile_operator(name, params or {})
    assert kind == "clean"
    return fn


def _validate(name: str, params: dict | None = None):
    kind, fn = compile_operator(name, params or {})
    assert kind == "validate"
    return fn


def test_registry_exposes_at_least_six_clean_and_six_validate_operators() -> None:
    assert len(operator_names("clean")) >= 6
    assert len(operator_names("validate")) >= 6
    # 题目点名的四类规则必须存在
    for required in ("truncate", "regex_replace", "date_format", "strip_html"):
        assert required in operator_names("clean")
    assert "enum" in operator_names("validate")


# ---------------------------------------------------------------------------
# 清洗算子
# ---------------------------------------------------------------------------


def test_trim_collapses_whitespace() -> None:
    fn = _clean("trim")
    assert fn("  a  b \n c ") == "a b c"
    assert fn(None) == ""


def test_truncate_respects_max_length_and_suffix() -> None:
    fn = _clean("truncate", {"max_length": 5, "suffix": "…"})
    assert fn("abcdefgh") == "abcd…"
    assert fn("abc") == "abc"
    with pytest.raises(ValueError):
        compile_operator("truncate", {"max_length": 0})
    with pytest.raises(ValueError):
        compile_operator("truncate", {"max_length": 2, "suffix": "..."})
    with pytest.raises(ValueError):
        compile_operator("truncate", {})


def test_regex_replace_with_backreference_and_flags() -> None:
    fn = _clean(
        "regex_replace",
        {"pattern": r"(\d{4})-(\d{2})-(\d{2})", "replacement": r"\3/\2/\1"},
    )
    assert fn("日期 2024-03-01") == "日期 01/03/2024"

    ci = _clean("regex_replace", {"pattern": "abc", "replacement": "X", "flags": ["ignorecase"]})
    assert ci("aBcAbC") == "XX"

    with pytest.raises(ValueError, match="正则"):
        compile_operator("regex_replace", {"pattern": "([a-z"})


def test_remove_substring_and_case() -> None:
    remove = _clean("remove_substring", {"substr": "【广告】"})
    assert remove("标题【广告】内容") == "标题内容"
    lower = _clean("case", {"mode": "lower"})
    assert lower("AbC") == "abc"
    upper = _clean("case", {"mode": "upper"})
    assert upper("abc") == "ABC"
    title = _clean("case", {"mode": "title"})
    assert title("hello world") == "Hello World"
    with pytest.raises(ValueError):
        compile_operator("case", {"mode": "nope"})


def test_strip_html_and_multiline_clean() -> None:
    html = _clean("strip_html")
    assert html("<p>第一段</p><p>第二段<b>x</b></p>") == "第一段\n第二段x"
    multi = _clean("multiline_clean")
    assert multi(" 第一行 \n\n 第二行\t续 ") == "第一行\n第二行 续"


def test_split_and_join_tags() -> None:
    split = _clean("split_tags")
    assert split("公园、花期 | 观测;公园") == ["公园", "花期", "观测"]
    assert split(["a", " a ", "", None]) == ["a"]
    join = _clean("join_tags", {"separator": "|"})
    assert join(["a", "b"]) == "a|b"


def test_default_value_replaces_empty() -> None:
    fn = _clean("default_value", {"value": "未分类"})
    assert fn(None) == "未分类"
    assert fn("   ") == "未分类"
    assert fn("真实值") == "真实值"
    with pytest.raises(ValueError):
        compile_operator("default_value", {})


def test_date_format_converts_and_fails_loudly() -> None:
    fn = _clean("date_format", {"to_format": "%Y/%m/%d"})
    assert fn("2024年3月1日") == "2024/03/01"
    assert fn("2024-03-02T09:30:00+08:00") == "2024/03/02"
    with pytest.raises(RuleExecutionError):
        fn("不是日期")
    with pytest.raises(ValueError):
        compile_operator("date_format", {"to_format": "%-"})


# ---------------------------------------------------------------------------
# 校验算子
# ---------------------------------------------------------------------------


def test_required_and_non_empty() -> None:
    required = _validate("required")
    required("x")
    with pytest.raises(ValidationFailure):
        required(None)

    non_empty = _validate("non_empty")
    non_empty(" x ")
    non_empty([1])
    with pytest.raises(ValidationFailure):
        non_empty("   ")
    with pytest.raises(ValidationFailure):
        non_empty([])


def test_length_validation_bounds() -> None:
    fn = _validate("length", {"min": 2, "max": 4})
    fn("abc")
    fn([1, 2])
    with pytest.raises(ValidationFailure):
        fn("a")
    with pytest.raises(ValidationFailure):
        fn("abcde")
    with pytest.raises(ValueError):
        compile_operator("length", {"min": 5, "max": 2})


def test_enum_validation_scalar_list_and_case_insensitive() -> None:
    strict = _validate("enum", {"values": ["环境", "技术"]})
    strict("环境")
    strict(["环境", "技术"])
    with pytest.raises(ValidationFailure):
        strict("其他")
    with pytest.raises(ValidationFailure):
        strict(["环境", "其他"])

    ci = _validate("enum", {"values": ["Open", "Closed"], "case_sensitive": False})
    ci("open")
    with pytest.raises(ValidationFailure):
        ci("unknown")

    with pytest.raises(ValueError):
        compile_operator("enum", {"values": []})


def test_regex_validation_and_invert() -> None:
    url = _validate("regex", {"pattern": r"^https?://"})
    url("http://x.test")
    with pytest.raises(ValidationFailure):
        url("ftp://x.test")
    no_digit = _validate("regex", {"pattern": r"\d", "invert": True})
    no_digit("abc")
    with pytest.raises(ValidationFailure):
        no_digit("abc1")


def test_date_range_validation() -> None:
    fn = _validate("date_range", {"min": "2024-01-01", "max": "2024-12-31"})
    fn("2024-06-01")
    fn(datetime(2024, 6, 1))
    with pytest.raises(ValidationFailure):
        fn("2023-12-31")
    with pytest.raises(ValidationFailure):
        fn("garbage")
    with pytest.raises(ValueError):
        compile_operator("date_range", {})
    with pytest.raises(ValueError):
        compile_operator("date_range", {"min": "2025-01-01", "max": "2024-01-01"})


def test_unknown_operator_raises() -> None:
    with pytest.raises(RuleExecutionError):
        compile_operator("does_not_exist", {})
