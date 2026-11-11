"""规则配置模型与加载：读取 ``config/rules.yaml`` 并用 pydantic 校验。

配置结构（全部可选，缺省即不启用规则引擎）：

.. code-block:: yaml

    defaults:                 # 全局默认
      threshold: 60           # 低质量阈值（0-100）
      on_quality_failure: mark  # 质量校验失败处理：skip / mark / warn
    default:                  # 未单独配置的源使用的兜底规则
      fields: {...}
    sources:                  # 按源独立配置
      demo_api:
        threshold: 60
        on_quality_failure: mark
        fields:
          title:
            rules:            # 规则按列表顺序执行
              - name: strip
              - name: truncate
                params: {max_length: 100}
                weight: 10
                on_failure: warn   # 单条规则可覆盖失败处理模式

加载只做结构校验；算子名称与参数的语义校验在 :class:`RuleEngine`
编译阶段完成，两者都在启动时执行，运行期不会再暴露配置错误。
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from crawl_flow.errors import RuleConfigError

FailureMode = Literal["skip", "mark", "warn"]
"""质量校验失败的处理模式：跳过不入库 / 标记低质量 / 仅告警。"""

DEFAULT_THRESHOLD = 60
DEFAULT_ON_QUALITY_FAILURE: FailureMode = "mark"
DEFAULT_RULE_WEIGHT = 10


class RuleConfig(BaseModel):
    """单条规则：算子名 + 参数 + 扣分权重 + 失败处理模式覆盖。"""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    params: dict = Field(default_factory=dict)
    weight: int = Field(default=DEFAULT_RULE_WEIGHT, ge=0, le=100)
    on_failure: FailureMode | None = None


class FieldRules(BaseModel):
    """单个字段的规则链，按 ``rules`` 列表顺序执行。"""

    model_config = ConfigDict(extra="forbid")

    rules: list[RuleConfig] = Field(min_length=1)


class SourceRules(BaseModel):
    """单个源（或 default 兜底段）的规则集。"""

    model_config = ConfigDict(extra="forbid")

    threshold: int | None = Field(default=None, ge=0, le=100)
    on_quality_failure: FailureMode | None = None
    fields: dict[str, FieldRules] = Field(default_factory=dict)


class DefaultsConfig(BaseModel):
    """全局默认：被各源的同名配置覆盖。"""

    model_config = ConfigDict(extra="forbid")

    threshold: int = Field(default=DEFAULT_THRESHOLD, ge=0, le=100)
    on_quality_failure: FailureMode = DEFAULT_ON_QUALITY_FAILURE


class RuleSetConfig(BaseModel):
    """规则配置顶层模型。"""

    model_config = ConfigDict(extra="forbid")

    defaults: DefaultsConfig = Field(default_factory=DefaultsConfig)
    default: SourceRules | None = None
    sources: dict[str, SourceRules] = Field(default_factory=dict)


def load_rules(path: str | Path) -> RuleSetConfig:
    """读取并校验规则配置，任何问题统一抛 :class:`RuleConfigError`。"""

    config_path = Path(path)
    if not config_path.is_file():
        raise RuleConfigError(f"规则配置文件不存在: {config_path}")
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise RuleConfigError(f"规则配置 YAML 解析失败: {exc}") from exc
    except OSError as exc:
        raise RuleConfigError(f"规则配置文件读取失败: {exc}") from exc

    if not isinstance(payload, dict):
        raise RuleConfigError("规则配置顶层必须是映射（defaults / default / sources）")
    try:
        return RuleSetConfig.model_validate(payload)
    except ValidationError as exc:
        raise RuleConfigError(f"规则配置校验失败: {exc}") from exc
