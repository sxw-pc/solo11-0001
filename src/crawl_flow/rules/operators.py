"""规则算子库：清洗算子与质量校验算子。

只依赖标准库（``re`` / ``datetime``）以及 ``pipeline.normalize`` 中既有的
默认清洗函数，不引入任何第三方规则引擎。

设计约定：

- 每个算子注册为 :class:`Operator`，包含 ``name``、参数校验/预编译函数
  （``compile``）与执行函数（``apply`` / ``validate``）。
- ``compile(params)`` 在**配置加载期**执行一次：检查参数、预编译正则、
  预解析日期格式，返回一个可直接调用的闭包，避免每条记录重复编译。
- 清洗算子接收当前值（通常是字符串），返回清洗后的值；无法处理时抛
  :class:`RuleExecutionError`，由引擎按 ``on_fail`` 降级。
- 校验算子通过返回 ``None`` 表示通过，抛 :class:`ValidationFailure` 表示不通过。
- 列表（tags）由引擎逐元素套用文本类清洗算子，算子本身只处理标量值。
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from crawl_flow.cleaning import (
    DATETIME_FORMATS,
    clean_multiline,
    clean_text,
    html_to_text,
    parse_datetime,
    split_tags,
)
from crawl_flow.rules.errors import RuleExecutionError, ValidationFailure

# ---------------------------------------------------------------------------
# 注册表结构
# ---------------------------------------------------------------------------

CleanFn = Callable[[Any], Any]
ValidateFn = Callable[[Any], None]
CompileFn = Callable[[dict[str, Any]], Callable[..., Any]]


@dataclass(frozen=True)
class Operator:
    """一条规则的静态描述与编译入口。"""

    name: str
    kind: str  # "clean" | "validate"
    compile: CompileFn
    description: str = ""


_REGISTRY: dict[str, Operator] = {}


def _register(name: str, kind: str, description: str) -> Callable[[CompileFn], CompileFn]:
    def decorator(compile_fn: CompileFn) -> CompileFn:
        if name in _REGISTRY:
            raise RuntimeError(f"规则算子重复注册: {name}")
        _REGISTRY[name] = Operator(name=name, kind=kind, compile=compile_fn, description=description)
        return compile_fn

    return decorator


def get_operator(name: str) -> Operator:
    operator = _REGISTRY.get(name)
    if operator is None:
        available = ", ".join(sorted(_REGISTRY))
        raise RuleExecutionError(name, f"未知规则类型，可用: {available}")
    return operator


def operator_names(kind: str | None = None) -> list[str]:
    return sorted(name for name, op in _REGISTRY.items() if kind is None or op.kind == kind)


def compile_operator(name: str, params: dict[str, Any]) -> tuple[str, Callable[..., Any]]:
    """加载期编译：参数非法时直接抛异常，由 loader 汇总为 RuleConfigError。"""

    operator = get_operator(name)
    return operator.kind, operator.compile(params or {})


# ---------------------------------------------------------------------------
# 辅助
# ---------------------------------------------------------------------------


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return str(value)


def _require(params: dict[str, Any], key: str, expected: type | tuple[type, ...]) -> Any:
    if key not in params:
        raise ValueError(f"缺少必填参数: {key}")
    value = params[key]
    if not isinstance(value, expected):
        names = expected.__name__ if isinstance(expected, type) else "/".join(t.__name__ for t in expected)
        raise ValueError(f"参数 {key} 类型必须是 {names}，实际为 {type(value).__name__}")
    return value


def _optional(
    params: dict[str, Any],
    key: str,
    expected: type | tuple[type, ...],
    default: Any,
) -> Any:
    if key not in params or params[key] is None:
        return default
    value = params[key]
    if not isinstance(value, expected):
        names = expected.__name__ if isinstance(expected, type) else "/".join(t.__name__ for t in expected)
        raise ValueError(f"参数 {key} 类型必须是 {names}，实际为 {type(value).__name__}")
    return value


# ---------------------------------------------------------------------------
# 清洗算子（10 种）
# ---------------------------------------------------------------------------


@_register("trim", "clean", "裁剪首尾空白并把连续空白压成单个空格")
def _compile_trim(params: dict[str, Any]) -> CleanFn:
    if params:
        raise ValueError("trim 不接受参数")

    def apply(value: Any) -> Any:
        return clean_text(value)

    return apply


@_register("truncate", "clean", "字符串截断，max_length 必填，可配省略后缀 suffix")
def _compile_truncate(params: dict[str, Any]) -> CleanFn:
    max_length = _require(params, "max_length", int)
    if max_length <= 0:
        raise ValueError("参数 max_length 必须是正整数")
    suffix = _optional(params, "suffix", str, "")
    if len(suffix) >= max_length:
        raise ValueError("参数 suffix 长度必须小于 max_length")

    def apply(value: Any) -> str:
        text = _as_text(value)
        if len(text) <= max_length:
            return text
        return text[: max_length - len(suffix)] + suffix

    return apply


@_register("regex_replace", "clean", "正则替换：pattern 必填，replacement 可带反向引用")
def _compile_regex_replace(params: dict[str, Any]) -> CleanFn:
    pattern = _require(params, "pattern", str)
    replacement = _optional(params, "replacement", str, "")
    count = _optional(params, "count", int, 0)
    flags = _compile_regex_flags(params.get("flags"))
    try:
        regex = re.compile(pattern, flags)
    except re.error as exc:
        raise ValueError(f"正则表达式非法: {exc}") from exc
    if count < 0:
        raise ValueError("参数 count 不能为负数")

    def apply(value: Any) -> str:
        return regex.sub(replacement, _as_text(value), count=count)

    return apply


def _compile_regex_flags(raw: Any) -> int:
    if not raw:
        return 0
    if not isinstance(raw, list) or not all(isinstance(item, str) for item in raw):
        raise ValueError("参数 flags 必须是字符串列表，可选: ignorecase, multiline, dotall")
    mapping = {
        "ignorecase": re.IGNORECASE,
        "multiline": re.MULTILINE,
        "dotall": re.DOTALL,
    }
    flags = 0
    for item in raw:
        if item not in mapping:
            raise ValueError(f"未知正则 flag: {item}，可选: {', '.join(mapping)}")
        flags |= mapping[item]
    return flags


@_register("remove_substring", "clean", "删除所有出现的子串 substr（区分大小写）")
def _compile_remove_substring(params: dict[str, Any]) -> CleanFn:
    substr = _require(params, "substr", str)
    if not substr:
        raise ValueError("参数 substr 不能为空字符串")

    def apply(value: Any) -> str:
        return _as_text(value).replace(substr, "")

    return apply


@_register("case", "clean", "大小写转换：mode=lower|upper|title")
def _compile_case(params: dict[str, Any]) -> CleanFn:
    mode = _optional(params, "mode", str, "lower")
    if mode not in ("lower", "upper", "title"):
        raise ValueError("参数 mode 必须是 lower、upper 或 title")

    def apply(value: Any) -> str:
        text = _as_text(value)
        if mode == "lower":
            return text.lower()
        if mode == "upper":
            return text.upper()
        return text.title()

    return apply


@_register("strip_html", "clean", "移除 HTML 标签并保留段落边界")
def _compile_strip_html(params: dict[str, Any]) -> CleanFn:
    if params:
        raise ValueError("strip_html 不接受参数")

    def apply(value: Any) -> str:
        return html_to_text(value)

    return apply


@_register("multiline_clean", "clean", "保留段落换行的多行文本清洗")
def _compile_multiline_clean(params: dict[str, Any]) -> CleanFn:
    if params:
        raise ValueError("multiline_clean 不接受参数")

    def apply(value: Any) -> str:
        return clean_multiline(value)

    return apply


@_register("split_tags", "clean", "把分隔字符串/列表统一为去重后的标签列表")
def _compile_split_tags(params: dict[str, Any]) -> CleanFn:
    if params:
        raise ValueError("split_tags 不接受参数")

    def apply(value: Any) -> list[str]:
        return split_tags(value)

    return apply


@_register("join_tags", "clean", "把列表用 separator 拼接为字符串")
def _compile_join_tags(params: dict[str, Any]) -> CleanFn:
    separator = _optional(params, "separator", str, ",")

    def apply(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, (list, tuple)):
            return separator.join(str(item) for item in value)
        return _as_text(value)

    return apply


@_register("default_value", "clean", "值为空（None 或空白字符串）时替换为默认值")
def _compile_default_value(params: dict[str, Any]) -> CleanFn:
    if "value" not in params:
        raise ValueError("缺少必填参数: value")
    default = params["value"]
    if default is None:
        raise ValueError("参数 value 不能为 null")

    def apply(value: Any) -> Any:
        if value is None:
            return default
        if isinstance(value, str) and not value.strip():
            return default
        return value

    return apply


@_register("date_format", "clean", "日期格式转换：from_formats 可选，to_format 必填")
def _compile_date_format(params: dict[str, Any]) -> CleanFn:
    to_format = _require(params, "to_format", str)
    from_formats = _optional(params, "from_formats", list, None)
    if from_formats is not None:
        if not from_formats or not all(isinstance(item, str) and item for item in from_formats):
            raise ValueError("参数 from_formats 必须是非空字符串列表")
    # 启动期先验证输出格式本身合法（Linux 的 strftime 对未知指令较宽容，
    # 这里显式拦截非法写法，保证跨平台行为一致）。
    try:
        datetime(2000, 1, 1).strftime(to_format)
    except (ValueError, OSError) as exc:
        raise ValueError(f"输出日期格式 to_format 非法: {exc}") from exc
    directives = re.findall(r"%.", to_format)
    valid = {
        "%%", "%a", "%A", "%b", "%B", "%c", "%d", "%f", "%H", "%I", "%j",
        "%m", "%M", "%p", "%S", "%U", "%w", "%W", "%x", "%X", "%y", "%Y", "%z",
        "%Z", "%G", "%u", "%V",
    }
    invalid = [item for item in directives if item not in valid]
    if invalid:
        raise ValueError(f"输出日期格式 to_format 含非法指令: {', '.join(invalid)}")
    accepted = tuple(from_formats) if from_formats else DATETIME_FORMATS

    def apply(value: Any) -> str:
        text = clean_text(value)
        if not text:
            raise RuleExecutionError("date_format", "日期值为空")
        parsed = _parse_with_formats(text, accepted)
        if parsed is None:
            raise RuleExecutionError("date_format", f"无法解析日期: {text!r}")
        return parsed.strftime(to_format)

    return apply


def _parse_with_formats(text: str, formats: tuple[str, ...]) -> datetime | None:
    iso_candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        return datetime.fromisoformat(iso_candidate)
    except ValueError:
        pass
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------------------
# 质量校验算子（6 种）
# ---------------------------------------------------------------------------


@_register("required", "validate", "字段必须存在且非 null")
def _compile_required(params: dict[str, Any]) -> ValidateFn:
    message = _optional(params, "message", str, "字段缺失")

    def validate(value: Any) -> None:
        if value is None:
            raise ValidationFailure(message)

    return validate


@_register("non_empty", "validate", "字符串去空白后非空、列表至少一个元素")
def _compile_non_empty(params: dict[str, Any]) -> ValidateFn:
    message = _optional(params, "message", str, "字段为空")

    def validate(value: Any) -> None:
        if value is None:
            raise ValidationFailure(message)
        if isinstance(value, str) and not value.strip():
            raise ValidationFailure(message)
        if isinstance(value, (list, tuple, dict, set)) and len(value) == 0:
            raise ValidationFailure(message)

    return validate


@_register("length", "validate", "字符串/列表长度落在 [min, max] 区间")
def _compile_length(params: dict[str, Any]) -> ValidateFn:
    min_length = _optional(params, "min", int, 0)
    max_length = _optional(params, "max", int, None)
    if min_length < 0:
        raise ValueError("参数 min 不能为负数")
    if max_length is not None and max_length < min_length:
        raise ValueError("参数 max 不能小于 min")
    message = _optional(params, "message", str, "")

    def validate(value: Any) -> None:
        if value is None:
            raise ValidationFailure(message or "字段为空，无法校验长度")
        length = len(value)
        if length < min_length:
            raise ValidationFailure(message or f"长度 {length} 小于最小值 {min_length}")
        if max_length is not None and length > max_length:
            raise ValidationFailure(message or f"长度 {length} 超过最大值 {max_length}")

    return validate


@_register("enum", "validate", "枚举值校验：values 必填，支持列表逐项校验")
def _compile_enum(params: dict[str, Any]) -> ValidateFn:
    values = _require(params, "values", list)
    if not values:
        raise ValueError("参数 values 必须是非空列表")
    if not all(isinstance(item, (str, int, float, bool)) for item in values):
        raise ValueError("参数 values 的元素必须是标量（字符串/数字/布尔）")
    allowed = set(values)
    case_sensitive = _optional(params, "case_sensitive", bool, True)
    message = _optional(params, "message", str, "")
    if not case_sensitive and all(isinstance(item, str) for item in allowed):
        allowed_lower = {item.lower() for item in allowed}  # type: ignore[union-attr]

        def matches(item: Any) -> bool:
            return isinstance(item, str) and item.lower() in allowed_lower
    else:
        def matches(item: Any) -> bool:
            return item in allowed

    def validate(value: Any) -> None:
        if value is None:
            raise ValidationFailure(message or "字段为空，不在枚举范围内")
        candidates = value if isinstance(value, (list, tuple)) else [value]
        for item in candidates:
            if not matches(item):
                raise ValidationFailure(message or f"值 {item!r} 不在允许的枚举范围内")

    return validate


@_register("regex", "validate", "正则匹配校验：pattern 必填，可配 invert 反向匹配")
def _compile_regex_validate(params: dict[str, Any]) -> ValidateFn:
    pattern = _require(params, "pattern", str)
    invert = _optional(params, "invert", bool, False)
    flags = _compile_regex_flags(params.get("flags"))
    message = _optional(params, "message", str, "")
    try:
        regex = re.compile(pattern, flags)
    except re.error as exc:
        raise ValueError(f"正则表达式非法: {exc}") from exc

    def validate(value: Any) -> None:
        if value is None:
            raise ValidationFailure(message or "字段为空，无法执行正则校验")
        text = value if isinstance(value, str) else str(value)
        matched = regex.search(text) is not None
        if matched == invert:
            expect = "不匹配" if invert else "匹配"
            raise ValidationFailure(message or f"值 {text!r} 未{expect}正则 {pattern!r}")

    return validate


@_register("date_range", "validate", "日期范围校验：min/max 为 ISO 日期，均可选")
def _compile_date_range(params: dict[str, Any]) -> ValidateFn:
    min_text = _optional(params, "min", str, None)
    max_text = _optional(params, "max", str, None)
    if min_text is None and max_text is None:
        raise ValueError("min 与 max 至少配置一个")
    min_dt = _parse_bound(min_text, "min")
    max_dt = _parse_bound(max_text, "max")
    if min_dt is not None and max_dt is not None and min_dt > max_dt:
        raise ValueError("参数 min 不能晚于 max")
    message = _optional(params, "message", str, "")

    def validate(value: Any) -> None:
        parsed = value if isinstance(value, datetime) else parse_datetime(value)
        if parsed is None:
            raise ValidationFailure(message or f"日期无法解析: {value!r}")
        naive = parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
        if min_dt is not None and naive < min_dt:
            raise ValidationFailure(message or f"日期 {naive.date()} 早于最小值 {min_dt.date()}")
        if max_dt is not None and naive > max_dt:
            raise ValidationFailure(message or f"日期 {naive.date()} 晚于最大值 {max_dt.date()}")

    return validate


def _parse_bound(text: str | None, label: str) -> datetime | None:
    if text is None:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"参数 {label} 必须是 ISO 日期（如 2024-01-01）: {exc}") from exc
