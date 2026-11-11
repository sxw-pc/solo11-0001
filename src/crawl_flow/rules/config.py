"""规则配置：读取 ``config/rules.yaml`` 并用 pydantic 做启动时校验。

配置结构（与 ``sources.yaml`` 一样的配置驱动模式）::

    version: 1
    quality_threshold: 60        # 质量分低于阈值触发 on_failure
    on_failure: mark             # skip=丢弃 | mark=保留并标记 | alert=保留仅告警
    defaults:                    # 所有源共用；源级同名字段整体覆盖
      fields:
        title:
          - rule: strip
          - rule: truncate
            max: 200
    sources:                     # 按来源独立配置
      demo_api:
        quality_threshold: 70    # 可选，覆盖全局
        on_failure: alert        # 可选，覆盖全局
        fields:
          category:
            - rule: enum
              values: [环境, 技术]

每条规则的公共字段：``rule``（算子名）、``order``（执行顺序，小的先跑）、
``severity``（error/warning，影响扣分）、``weight``（计分权重）、
``enabled``（可临时关闭）；其余键作为算子参数，在加载时校验。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from crawl_flow.errors import ConfigError

FailureAction = Literal["skip", "mark", "alert"]

# 规则可作用的标准字段（field_map 归一化之后的字段名）。
STANDARD_FIELDS: frozenset[str] = frozenset(
    {"title", "category", "tags", "published_at", "content", "url"}
)

# 规则公共字段，其余键视为算子参数。
_RESERVED_KEYS = {"rule", "order", "severity", "weight", "enabled"}


class RuleSpec(BaseModel):
    """单条规则：公共字段 + 算子参数（额外键）。"""

    model_config = ConfigDict(extra="allow")

    rule: str = Field(min_length=1)
    order: int = 100
    severity: Literal["error", "warning"] = "error"
    weight: float = Field(default=1.0, gt=0, le=100)
    enabled: bool = True

    def operator_params(self) -> dict[str, Any]:
        """提取算子参数（公共字段以外的键）。"""

        extra = self.model_extra or {}
        return {key: value for key, value in extra.items() if key not in _RESERVED_KEYS}


class SourceRules(BaseModel):
    """单个源的规则配置：可覆盖全局阈值与失败处理模式。"""

    model_config = ConfigDict(extra="forbid")

    quality_threshold: int | None = Field(default=None, ge=0, le=100)
    on_failure: FailureAction | None = None
    fields: dict[str, list[RuleSpec]] = Field(default_factory=dict)


class DefaultsSection(BaseModel):
    """所有源共用的字段规则。"""

    model_config = ConfigDict(extra="forbid")

    fields: dict[str, list[RuleSpec]] = Field(default_factory=dict)


class RuleFileConfig(BaseModel):
    """规则文件顶层结构。"""

    model_config = ConfigDict(extra="forbid")

    version: int = 1
    quality_threshold: int = Field(default=60, ge=0, le=100)
    on_failure: FailureAction = "mark"
    defaults: DefaultsSection = Field(default_factory=DefaultsSection)
    sources: dict[str, SourceRules] = Field(default_factory=dict)


def _check_field_names(fields: dict[str, Any], where: str) -> None:
    for field_name in fields:
        if field_name not in STANDARD_FIELDS:
            available = ", ".join(sorted(STANDARD_FIELDS))
            raise ConfigError(
                f"{where}: 未知字段 {field_name!r}，可用字段: {available}"
            )


def validate_rule_config(config: RuleFileConfig) -> None:
    """对加载后的配置做结构校验（字段名等），算子参数由引擎编译时校验。"""

    _check_field_names(config.defaults.fields, "defaults.fields")
    for source_name, source_rules in config.sources.items():
        _check_field_names(source_rules.fields, f"sources.{source_name}.fields")


def load_rule_config(path: str | Path) -> RuleFileConfig:
    """读取并校验规则配置，任何问题统一抛 :class:`ConfigError`。"""

    config_path = Path(path)
    if not config_path.is_file():
        raise ConfigError(f"规则配置文件不存在: {config_path}")
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"规则配置 YAML 解析失败: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"规则配置文件读取失败: {exc}") from exc

    if payload is None:
        raise ConfigError(f"规则配置为空: {config_path}")
    if not isinstance(payload, dict):
        raise ConfigError("规则配置顶层必须是映射（quality_threshold/defaults/sources 等）")

    try:
        config = RuleFileConfig.model_validate(payload)
    except ValidationError as exc:
        raise ConfigError(f"规则配置校验失败: {exc}") from exc
    validate_rule_config(config)
    return config
