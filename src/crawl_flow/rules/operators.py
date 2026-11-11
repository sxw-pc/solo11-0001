"""规则算子：字段清洗与质量校验原语，全部基于标准库实现。

每个算子由「参数模型 + 执行函数」组成，注册到 :data:`OPERATORS`。
执行函数接收当前字段值与已校验参数，返回新值；校验类算子原样返回
输入值。值不满足规则时抛 :class:`RuleFailed`，由引擎捕获并计入质量分；
参数本身非法（如正则无法编译）在启动校验阶段就会被参数模型拒绝。

不引入任何第三方规则引擎依赖，单条规则执行在微秒级。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

# 与 pipeline.normalize 保持一致的常见日期格式；规则引擎独立维护一份，
# 避免与采集管道互相依赖。
DEFAULT_DATE_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%d %H:%M",
    "%Y/%m/%d",
    "%Y.%m.%d",
    "%Y年%m月%d日 %H:%M:%S",
    "%Y年%m月%d日 %H:%M",
    "%Y年%m月%d日",
)

ISO_OUTPUT = "iso"
_SPACE_RUN = re.compile(r"\s+")


class RuleFailed(Exception):
    """单条规则未命中（值不满足规则），引擎捕获后按权重扣减质量分。"""


@lru_cache(maxsize=None)
def _compile_regex(pattern: str) -> re.Pattern[str]:
    """编译正则并缓存；配置中的正则数量有限，缓存无上限。"""

    return re.compile(pattern)


def _validate_regex(pattern: str) -> str:
    try:
        _compile_regex(pattern)
    except re.error as exc:
        raise ValueError(f"正则表达式无法编译: {exc}") from exc
    return pattern


def _validate_date_format(fmt: str) -> str:
    """通过「格式化再解析」的往返校验日期格式串，启动期暴露拼写错误。"""

    try:
        sample = datetime(2000, 1, 2, 3, 4, 5).strftime(fmt)
        datetime.strptime(sample, fmt)
    except ValueError as exc:
        raise ValueError(f"无效的日期格式 {fmt!r}: {exc}") from exc
    return fmt


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) == 0
    return False


def _to_text(value: Any) -> str:
    return "" if value is None else str(value)


class _Params(BaseModel):
    """算子参数基类：禁止未知键，配置拼写错误在启动校验时直接报错。"""

    model_config = ConfigDict(extra="forbid")


# ---------------------------------------------------------------------------
# 清洗算子（改变字段值）
# ---------------------------------------------------------------------------


class StripParams(_Params):
    collapse: bool = True


def _strip(value: Any, params: StripParams) -> str:
    text = _to_text(value).strip()
    return _SPACE_RUN.sub(" ", text) if params.collapse else text


class TruncateParams(_Params):
    max_length: int = Field(ge=1)
    suffix: str = ""

    @model_validator(mode="after")
    def _check_suffix(self) -> "TruncateParams":
        if len(self.suffix) >= self.max_length:
            raise ValueError("suffix 长度必须小于 max_length")
        return self


def _truncate(value: Any, params: TruncateParams) -> str:
    text = _to_text(value)
    if len(text) <= params.max_length:
        return text
    return text[: params.max_length - len(params.suffix)] + params.suffix


class RegexReplaceParams(_Params):
    pattern: str
    replacement: str = ""
    count: int = Field(default=0, ge=0)

    @field_validator("pattern")
    @classmethod
    def _check_pattern(cls, value: str) -> str:
        return _validate_regex(value)


def _regex_replace(value: Any, params: RegexReplaceParams) -> str:
    return _compile_regex(params.pattern).sub(
        params.replacement, _to_text(value), count=params.count
    )


class DateFormatParams(_Params):
    formats: list[str] | None = None  # None → 内置常见格式 + ISO 8601
    output: str = "%Y-%m-%d %H:%M:%S"  # 或 "iso"（ISO 8601 输出）

    @field_validator("formats")
    @classmethod
    def _check_formats(cls, value: list[str] | None) -> list[str] | None:
        if value is not None:
            if not value:
                raise ValueError("formats 不能为空列表")
            for fmt in value:
                _validate_date_format(fmt)
        return value

    @field_validator("output")
    @classmethod
    def _check_output(cls, value: str) -> str:
        if value != ISO_OUTPUT:
            _validate_date_format(value)
        return value


def _parse_date(text: str, formats: list[str] | None) -> datetime | None:
    iso_candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        return datetime.fromisoformat(iso_candidate)
    except ValueError:
        pass
    for fmt in formats or DEFAULT_DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _date_format(value: Any, params: DateFormatParams) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = _to_text(value).strip()
        if not text:
            raise RuleFailed("值为空，无法转换日期")
        parsed = _parse_date(text, params.formats)
        if parsed is None:
            raise RuleFailed(f"无法解析日期: {text!r}")
    if params.output == ISO_OUTPUT:
        return parsed.isoformat()
    return parsed.strftime(params.output)


class DefaultParams(_Params):
    value: Any = Field(...)


def _default(value: Any, params: DefaultParams) -> Any:
    return params.value if _is_empty(value) else value


class TypeCastParams(_Params):
    to: Literal["str", "int", "float", "bool", "list"]


_TRUE_WORDS = {"true", "yes", "1", "on", "y"}
_FALSE_WORDS = {"false", "no", "0", "off", "n"}


def _type_cast(value: Any, params: TypeCastParams) -> Any:
    target = params.to
    if target == "str":
        return _to_text(value)
    if target == "list":
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, (tuple, set)):
            return list(value)
        return [value]
    if isinstance(value, bool):
        if target == "bool":
            return value
        raise RuleFailed(f"布尔值不能转换为 {target}")
    if target == "int":
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            if value.is_integer():
                return int(value)
            raise RuleFailed(f"非整数值，无法转换为 int: {value}")
        text = _to_text(value).strip()
        try:
            return int(text)
        except ValueError:
            raise RuleFailed(f"无法转换为 int: {text!r}") from None
    if target == "float":
        if isinstance(value, (int, float)):
            return float(value)
        text = _to_text(value).strip()
        try:
            return float(text)
        except ValueError:
            raise RuleFailed(f"无法转换为 float: {text!r}") from None
    # target == "bool"
    text = _to_text(value).strip().lower()
    if text in _TRUE_WORDS:
        return True
    if text in _FALSE_WORDS:
        return False
    raise RuleFailed(f"无法转换为 bool: {_to_text(value)!r}")


class MapValuesParams(_Params):
    mapping: dict[str, Any]
    on_missing: Literal["keep", "default", "fail"] = "keep"
    default: Any = None

    @field_validator("mapping", mode="before")
    @classmethod
    def _stringify_keys(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): item for key, item in value.items()}
        return value

    @field_validator("mapping")
    @classmethod
    def _check_mapping(cls, value: dict[str, Any]) -> dict[str, Any]:
        if not value:
            raise ValueError("mapping 不能为空")
        return value


def _map_values(value: Any, params: MapValuesParams) -> Any:
    key = _to_text(value)
    if key in params.mapping:
        return params.mapping[key]
    if params.on_missing == "keep":
        return value
    if params.on_missing == "default":
        return params.default
    raise RuleFailed(f"值不在映射表中: {key!r}")


# ---------------------------------------------------------------------------
# 校验算子（不改变字段值，只判定通过/失败）
# ---------------------------------------------------------------------------


def _required(value: Any, params: _Params) -> Any:
    if _is_empty(value):
        raise RuleFailed("必填字段缺失或为空")
    return value


class EnumParams(_Params):
    values: list[Any] = Field(min_length=1)
    case_sensitive: bool = True


def _enum(value: Any, params: EnumParams) -> Any:
    text = _to_text(value)
    candidates = [str(item) for item in params.values]
    if not params.case_sensitive:
        text = text.lower()
        candidates = [item.lower() for item in candidates]
    if text not in candidates:
        raise RuleFailed(f"值 {text!r} 不在枚举范围内")
    return value


class LengthParams(_Params):
    min: int | None = Field(default=None, ge=0)
    max: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _check_bounds(self) -> "LengthParams":
        if self.min is None and self.max is None:
            raise ValueError("min 与 max 至少配置一个")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("min 不能大于 max")
        return self


def _length(value: Any, params: LengthParams) -> Any:
    if isinstance(value, (list, tuple, set, dict)):
        size = len(value)
    else:
        size = len(_to_text(value))
    if params.min is not None and size < params.min:
        raise RuleFailed(f"长度 {size} 小于下限 {params.min}")
    if params.max is not None and size > params.max:
        raise RuleFailed(f"长度 {size} 超过上限 {params.max}")
    return value


class PatternParams(_Params):
    pattern: str
    must_match: bool = True

    @field_validator("pattern")
    @classmethod
    def _check_pattern(cls, value: str) -> str:
        return _validate_regex(value)


def _pattern(value: Any, params: PatternParams) -> Any:
    text = _to_text(value)
    matched = _compile_regex(params.pattern).search(text) is not None
    if matched != params.must_match:
        expectation = "应匹配" if params.must_match else "不应匹配"
        raise RuleFailed(f"值 {text!r} {expectation}正则 {params.pattern!r}")
    return value


class RangeParams(_Params):
    min: float | None = None
    max: float | None = None

    @model_validator(mode="after")
    def _check_bounds(self) -> "RangeParams":
        if self.min is None and self.max is None:
            raise ValueError("min 与 max 至少配置一个")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("min 不能大于 max")
        return self


def _range(value: Any, params: RangeParams) -> Any:
    text = _to_text(value).strip()
    try:
        number = float(text)
    except ValueError:
        raise RuleFailed(f"不是数值: {text!r}") from None
    if params.min is not None and number < params.min:
        raise RuleFailed(f"数值 {number} 小于下限 {params.min}")
    if params.max is not None and number > params.max:
        raise RuleFailed(f"数值 {number} 超过上限 {params.max}")
    return value


# ---------------------------------------------------------------------------
# 算子注册表
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class OperatorSpec:
    """算子元数据：名称、类型（清洗/校验）、参数模型与执行函数。"""

    name: str
    kind: Literal["clean", "validate"]
    params_model: type[_Params]
    func: Callable[[Any, Any], Any]
    description: str


_OPERATOR_LIST: tuple[OperatorSpec, ...] = (
    OperatorSpec("strip", "clean", StripParams, _strip, "裁剪首尾空白，可选压缩连续空白"),
    OperatorSpec("truncate", "clean", TruncateParams, _truncate, "按最大长度截断字符串"),
    OperatorSpec("regex_replace", "clean", RegexReplaceParams, _regex_replace, "正则替换"),
    OperatorSpec("date_format", "clean", DateFormatParams, _date_format, "日期格式转换"),
    OperatorSpec("default", "clean", DefaultParams, _default, "空值填充默认值"),
    OperatorSpec("type_cast", "clean", TypeCastParams, _type_cast, "类型转换 str/int/float/bool/list"),
    OperatorSpec("map_values", "clean", MapValuesParams, _map_values, "按映射表改写值"),
    OperatorSpec("required", "validate", _Params, _required, "必填校验"),
    OperatorSpec("enum", "validate", EnumParams, _enum, "枚举值校验"),
    OperatorSpec("length", "validate", LengthParams, _length, "长度范围校验"),
    OperatorSpec("pattern", "validate", PatternParams, _pattern, "正则匹配校验"),
    OperatorSpec("range", "validate", RangeParams, _range, "数值范围校验"),
)

OPERATORS: dict[str, OperatorSpec] = {spec.name: spec for spec in _OPERATOR_LIST}
