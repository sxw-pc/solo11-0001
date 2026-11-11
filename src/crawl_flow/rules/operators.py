"""规则算子：清洗（转换字段值）与校验（检查字段值）两类。

每个算子在配置加载阶段通过 ``prepare`` 完成参数校验与正则预编译，
返回一个可直接调用的闭包，运行期不再做任何配置解析，保证执行开销极小。
所有算子都是纯函数，不引入任何第三方规则引擎依赖。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Literal

from crawl_flow.errors import ConfigError

# 注意：本模块不在顶层导入 crawl_flow.pipeline.normalize——
# pipeline 包初始化会引入 pipeline.run，而 run 又依赖 rules.engine，
# 顶层导入会形成循环。归一化工具函数在 prepare/执行期惰性导入，
# 此时所有模块均已加载完毕。

RuleKind = Literal["clean", "validate"]
ListMode = Literal["each", "whole"]

PreparedRule = Callable[[Any], Any]


class CleanFailed(Exception):
    """清洗规则无法处理当前值（引擎会保留原值并记录）。"""


class RuleViolation(Exception):
    """校验规则未通过。"""


@dataclass(frozen=True)
class Operator:
    """一个可调用的规则算子。

    ``list_mode="each"`` 时，引擎对列表值逐元素应用；
    ``"whole"`` 时把整个值（含列表）交给算子。
    """

    name: str
    kind: RuleKind
    list_mode: ListMode
    description: str
    prepare: Callable[[dict[str, Any], str], PreparedRule]


def _reject_unknown(params: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise ConfigError(f"{where}: 不支持的参数 {', '.join(unknown)}")


def _require_int(params: dict[str, Any], key: str, where: str, *, minimum: int) -> int:
    raw = params.get(key)
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ConfigError(f"{where}: 参数 {key} 必须是整数")
    if raw < minimum:
        raise ConfigError(f"{where}: 参数 {key} 必须 >= {minimum}")
    return raw


def _optional_number(
    params: dict[str, Any], key: str, where: str
) -> float | None:
    raw = params.get(key)
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ConfigError(f"{where}: 参数 {key} 必须是数字")
    return float(raw)


def _compile_regex(raw: Any, where: str, *, key: str) -> re.Pattern[str]:
    if not isinstance(raw, str) or not raw:
        raise ConfigError(f"{where}: 参数 {key} 必须是非空字符串")
    try:
        return re.compile(raw)
    except re.error as exc:
        raise ConfigError(f"{where}: 正则表达式无效 {key}={raw!r}: {exc}") from exc


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


# ---------------------------------------------------------------------------
# 清洗算子
# ---------------------------------------------------------------------------


def _prepare_strip(params: dict[str, Any], where: str) -> PreparedRule:
    from crawl_flow.pipeline.normalize import clean_multiline, clean_text

    _reject_unknown(params, {"multiline"}, where)
    multiline = params.get("multiline", False)
    if not isinstance(multiline, bool):
        raise ConfigError(f"{where}: 参数 multiline 必须是布尔值")

    def apply(value: Any) -> str:
        return clean_multiline(value) if multiline else clean_text(value)

    return apply


def _prepare_truncate(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"max", "suffix"}, where)
    max_len = _require_int(params, "max", where, minimum=1)
    suffix = params.get("suffix", "")
    if not isinstance(suffix, str):
        raise ConfigError(f"{where}: 参数 suffix 必须是字符串")

    def apply(value: Any) -> Any:
        if value is None:
            return value
        text = str(value)
        if len(text) <= max_len:
            return text
        keep = max_len - len(suffix)
        if keep < 0:
            raise CleanFailed(f"suffix 长度超过 max={max_len}")
        return text[:keep] + suffix

    return apply


def _prepare_regex_replace(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"pattern", "repl", "count"}, where)
    pattern = _compile_regex(params.get("pattern"), where, key="pattern")
    repl = params.get("repl", "")
    if not isinstance(repl, str):
        raise ConfigError(f"{where}: 参数 repl 必须是字符串")
    count = params.get("count", 0)
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ConfigError(f"{where}: 参数 count 必须是非负整数")

    def apply(value: Any) -> Any:
        if value is None:
            return value
        return pattern.sub(repl, str(value), count=count)

    return apply


def _prepare_date_format(params: dict[str, Any], where: str) -> PreparedRule:
    from crawl_flow.pipeline.normalize import DATETIME_FORMATS, clean_text

    _reject_unknown(params, {"output", "input_formats"}, where)
    output = params.get("output", "iso")
    if not isinstance(output, str) or not output:
        raise ConfigError(f"{where}: 参数 output 必须是非空字符串（strftime 格式或 iso）")
    input_formats = params.get("input_formats")
    formats: tuple[str, ...] = DATETIME_FORMATS
    if input_formats is not None:
        if not isinstance(input_formats, list) or not all(
            isinstance(item, str) and item for item in input_formats
        ):
            raise ConfigError(f"{where}: 参数 input_formats 必须是非空字符串列表")
        formats = tuple(input_formats) + DATETIME_FORMATS

    def apply(value: Any) -> Any:
        if _is_empty(value):
            return value
        if isinstance(value, datetime):
            parsed = value
        else:
            text = clean_text(value)
            parsed = None
            iso_candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
            try:
                parsed = datetime.fromisoformat(iso_candidate)
            except ValueError:
                for fmt in formats:
                    try:
                        parsed = datetime.strptime(text, fmt)
                        break
                    except ValueError:
                        continue
        if parsed is None:
            raise CleanFailed(f"无法解析日期: {value!r}")
        return parsed.isoformat() if output == "iso" else parsed.strftime(output)

    return apply


def _prepare_case(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"mode"}, where)
    mode = params.get("mode")
    if mode not in ("lower", "upper", "title"):
        raise ConfigError(f"{where}: 参数 mode 必须是 lower/upper/title")

    def apply(value: Any) -> Any:
        if value is None:
            return value
        text = str(value)
        if mode == "lower":
            return text.lower()
        if mode == "upper":
            return text.upper()
        return text.title()

    return apply


def _prepare_default(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"value"}, where)
    if "value" not in params:
        raise ConfigError(f"{where}: 缺少必填参数 value")
    fallback = params["value"]

    def apply(value: Any) -> Any:
        return fallback if _is_empty(value) else value

    return apply


def _prepare_type_cast(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"to"}, where)
    target = params.get("to")
    if target not in ("str", "int", "float", "bool"):
        raise ConfigError(f"{where}: 参数 to 必须是 str/int/float/bool")

    def apply(value: Any) -> Any:
        if value is None:
            return value
        try:
            if target == "str":
                return str(value)
            if target == "int":
                return int(str(value).strip())
            if target == "float":
                return float(str(value).strip())
            text = str(value).strip().lower()
            if text in ("true", "1", "yes", "y", "on"):
                return True
            if text in ("false", "0", "no", "n", "off"):
                return False
            raise ValueError(f"无法识别的布尔值: {value!r}")
        except (TypeError, ValueError) as exc:
            raise CleanFailed(f"类型转换失败 {value!r} -> {target}: {exc}") from exc

    return apply


def _prepare_map_values(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"mapping", "on_miss"}, where)
    mapping = params.get("mapping")
    if not isinstance(mapping, dict) or not mapping:
        raise ConfigError(f"{where}: 参数 mapping 必须是非空映射")
    on_miss = params.get("on_miss", "keep")
    if on_miss not in ("keep", "null", "fail"):
        raise ConfigError(f"{where}: 参数 on_miss 必须是 keep/null/fail")
    table = {str(key): val for key, val in mapping.items()}

    def apply(value: Any) -> Any:
        if value is None:
            return value
        key = str(value)
        if key in table:
            return table[key]
        if on_miss == "keep":
            return value
        if on_miss == "null":
            return None
        raise CleanFailed(f"值不在映射表中: {key!r}")

    return apply


def _prepare_html_strip(params: dict[str, Any], where: str) -> PreparedRule:
    from crawl_flow.pipeline.normalize import html_to_text

    _reject_unknown(params, set(), where)

    def apply(value: Any) -> Any:
        if value is None:
            return value
        return html_to_text(value)

    return apply


# ---------------------------------------------------------------------------
# 校验算子
# ---------------------------------------------------------------------------


def _prepare_required(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, set(), where)

    def apply(value: Any) -> None:
        if _is_empty(value):
            raise RuleViolation("必填字段为空")

    return apply


def _prepare_enum(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"values"}, where)
    values = params.get("values")
    if not isinstance(values, list) or not values:
        raise ConfigError(f"{where}: 参数 values 必须是非空列表")
    allowed = {str(item) for item in values}

    def apply(value: Any) -> None:
        if str(value) not in allowed:
            raise RuleViolation(f"值 {value!r} 不在枚举 {sorted(allowed)} 中")

    return apply


def _prepare_length(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"min", "max"}, where)
    min_len = params.get("min")
    max_len = params.get("max")
    if min_len is None and max_len is None:
        raise ConfigError(f"{where}: 至少需要参数 min 或 max")
    if min_len is not None:
        min_len = _require_int(params, "min", where, minimum=0)
    if max_len is not None:
        max_len = _require_int(params, "max", where, minimum=0)
    if min_len is not None and max_len is not None and min_len > max_len:
        raise ConfigError(f"{where}: min 不能大于 max")

    def apply(value: Any) -> None:
        size = len(value) if isinstance(value, (str, list, tuple, set, dict)) else len(str(value))
        if min_len is not None and size < min_len:
            raise RuleViolation(f"长度 {size} 小于下限 {min_len}")
        if max_len is not None and size > max_len:
            raise RuleViolation(f"长度 {size} 超过上限 {max_len}")

    return apply


def _prepare_pattern(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"regex"}, where)
    pattern = _compile_regex(params.get("regex"), where, key="regex")

    def apply(value: Any) -> None:
        if pattern.search(str(value)) is None:
            raise RuleViolation(f"值 {value!r} 不匹配正则 {pattern.pattern!r}")

    return apply


def _prepare_range(params: dict[str, Any], where: str) -> PreparedRule:
    _reject_unknown(params, {"min", "max"}, where)
    min_val = _optional_number(params, "min", where)
    max_val = _optional_number(params, "max", where)
    if min_val is None and max_val is None:
        raise ConfigError(f"{where}: 至少需要参数 min 或 max")
    if min_val is not None and max_val is not None and min_val > max_val:
        raise ConfigError(f"{where}: min 不能大于 max")

    def apply(value: Any) -> None:
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise RuleViolation(f"值 {value!r} 不是数字") from exc
        if min_val is not None and number < min_val:
            raise RuleViolation(f"值 {number} 小于下限 {min_val}")
        if max_val is not None and number > max_val:
            raise RuleViolation(f"值 {number} 超过上限 {max_val}")

    return apply


def _prepare_date_parseable(params: dict[str, Any], where: str) -> PreparedRule:
    from crawl_flow.pipeline.normalize import parse_datetime

    _reject_unknown(params, set(), where)

    def apply(value: Any) -> None:
        if isinstance(value, datetime):
            return
        if parse_datetime(value) is None:
            raise RuleViolation(f"值 {value!r} 不是可识别的日期")

    return apply


# ---------------------------------------------------------------------------
# 注册表
# ---------------------------------------------------------------------------

OPERATORS: dict[str, Operator] = {
    "strip": Operator("strip", "clean", "each", "裁剪空白并压缩连续空白", _prepare_strip),
    "truncate": Operator("truncate", "clean", "each", "按最大长度截断字符串", _prepare_truncate),
    "regex_replace": Operator("regex_replace", "clean", "each", "正则替换", _prepare_regex_replace),
    "date_format": Operator("date_format", "clean", "whole", "日期解析并输出统一格式", _prepare_date_format),
    "case": Operator("case", "clean", "each", "大小写转换", _prepare_case),
    "default": Operator("default", "clean", "whole", "空值填充默认值", _prepare_default),
    "type_cast": Operator("type_cast", "clean", "each", "类型转换 str/int/float/bool", _prepare_type_cast),
    "map_values": Operator("map_values", "clean", "each", "按映射表改写值", _prepare_map_values),
    "html_strip": Operator("html_strip", "clean", "whole", "去除 HTML 标签", _prepare_html_strip),
    "required": Operator("required", "validate", "whole", "必填校验", _prepare_required),
    "enum": Operator("enum", "validate", "each", "枚举值校验", _prepare_enum),
    "length": Operator("length", "validate", "whole", "长度范围校验", _prepare_length),
    "pattern": Operator("pattern", "validate", "each", "正则匹配校验", _prepare_pattern),
    "range": Operator("range", "validate", "each", "数值范围校验", _prepare_range),
    "date_parseable": Operator("date_parseable", "validate", "whole", "日期可解析校验", _prepare_date_parseable),
}


def get_operator(name: str, where: str) -> Operator:
    """按名称取算子，未知名称抛带位置信息的 :class:`ConfigError`。"""

    operator = OPERATORS.get(name)
    if operator is None:
        available = ", ".join(sorted(OPERATORS))
        raise ConfigError(f"{where}: 未知规则 {name!r}，可用规则: {available}")
    return operator
