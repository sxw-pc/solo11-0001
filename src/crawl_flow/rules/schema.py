"""规则引擎配置模型（pydantic v2，``extra="forbid"``）。

配置文件结构（``config/rules.yaml``）::

    version: 1
    quality: { threshold: 60, on_validation_fail: mark }
    field_defaults:
      rules: [...]
    fields:
      title:
        weight: 1.5
        type_hint: text
        rules: [ {type: trim}, {type: truncate, params: {max_length: 200}} ]
        validations: [ {type: required}, {type: enum, params: {values: [a, b]}} ]
    sources:
      demo_api:
        quality: { threshold: 70 }
        fields: { title: { rules: [...] } }

结构校验由 pydantic 完成；规则参数的语义校验（正则能否编译、枚举值是否
合法等）由 ``crawl_flow.rules.operators`` 的 ``compile_*`` 在加载期执行，
错误汇总后统一抛 :class:`RuleConfigError`，保证启动时一次性暴露所有语法问题。
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

#: 规则引擎可以处理的标准记录字段（与 models.Record 对应，去掉来源标识与时间戳）。
STANDARD_FIELDS: tuple[str, ...] = (
    "title",
    "category",
    "tags",
    "published_at",
    "content",
    "url",
)

#: 字段值形态提示，决定默认清洗逻辑与取值/写回方式。
FieldTypeHint = Literal["text", "multiline", "html", "tags", "datetime"]

#: 清洗规则运行期失败的降级策略。
CleanOnFail = Literal["keep", "default", "abort_field"]

#: 质量校验失败的处理模式。
ValidationAction = Literal["skip", "mark", "warn"]


class RuleSpec(BaseModel):
    """单条清洗规则。"""

    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1)
    name: str | None = Field(default=None, min_length=1)
    enabled: bool = True
    params: dict = Field(default_factory=dict)
    #: keep=保留清洗前原值；default=回退默认清洗；abort_field=该字段回退默认清洗
    on_fail: CleanOnFail = "keep"

    @field_validator("type")
    @classmethod
    def _strip_type(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("规则 type 不能为空字符串")
        return text


class ValidationSpec(BaseModel):
    """单条质量校验规则。"""

    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1)
    name: str | None = Field(default=None, min_length=1)
    enabled: bool = True
    params: dict = Field(default_factory=dict)
    #: 校验失败时本条规则的处理模式；None 表示沿用来源/全局默认。
    on_fail: ValidationAction | None = None
    #: 本条规则在质量评分中的权重；None 表示用 default_weight。
    weight: float | None = Field(default=None, gt=0)

    @field_validator("type")
    @classmethod
    def _strip_type(cls, value: str) -> str:
        text = value.strip()
        if not text:
            raise ValueError("校验 type 不能为空字符串")
        return text


class FieldRuleSet(BaseModel):
    """单个字段的清洗规则链与校验规则集合。"""

    model_config = ConfigDict(extra="forbid")

    type_hint: FieldTypeHint = "text"
    weight: float | None = Field(default=None, gt=0)
    rules: list[RuleSpec] = Field(default_factory=list)
    validations: list[ValidationSpec] = Field(default_factory=list)


class QualityConfig(BaseModel):
    """质量评分与校验失败处理的全局/来源级配置。"""

    model_config = ConfigDict(extra="forbid")

    threshold: float = Field(default=60.0, ge=0, le=100)
    default_weight: float = Field(default=1.0, gt=0)
    on_validation_fail: ValidationAction = "mark"
    #: 评分低于阈值时是否在记录上打低质量标记。
    low_quality_flag: bool = True


class RuleProfile(BaseModel):
    """一层可合并的规则配置（全局层与来源层同构）。"""

    model_config = ConfigDict(extra="forbid")

    quality: QualityConfig | None = None
    field_defaults: FieldRuleSet | None = None
    # 字段名的语义校验放在 loader 中做，以便和规则参数错误一次性汇总。
    fields: dict[str, FieldRuleSet] = Field(default_factory=dict)


class RulesConfig(BaseModel):
    """规则引擎配置文件的顶层模型。"""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    quality: QualityConfig | None = None
    field_defaults: FieldRuleSet | None = None
    fields: dict[str, FieldRuleSet] = Field(default_factory=dict)
    sources: dict[str, RuleProfile] = Field(default_factory=dict)
