"""配置加载：读取 ``config/sources.yaml`` 并用 pydantic 校验。"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from crawl_flow.errors import ConfigError

SourceType = Literal["api", "html"]

DEFAULT_PATHS: dict[str, tuple[str, str]] = {
    "api": ("/api/items", "/api/items/{id}"),
    "html": ("/list", "/item/{id}"),
}

# 归一化 Record 上可用于关联键 / 宽表取值的标量或列表字段
JOIN_FIELD_CHOICES = ("external_id", "title", "category", "url", "published_at")
WIDE_FIELD_CHOICES = (
    "source",
    "external_id",
    "title",
    "category",
    "tags",
    "published_at",
    "content",
    "url",
    "fetched_at",
)
ConflictPolicy = Literal["merge", "skip"]
CoerceMode = Literal["string", "integer", "datetime"]
FieldStrategy = Literal["first", "concat", "list"]


class FieldMapping(BaseModel):
    """源字段 → 标准字段的映射。值写源数据里的键名。"""

    model_config = ConfigDict(extra="forbid")

    title: str = "title"
    category: str = "category"
    tags: str = "tags"
    published_at: str = "published_at"
    content: str = "content"
    url: str | None = None


class JoinKey(BaseModel):
    """一条跨源关联键：本源与关联源使用同一归一化字段做等值匹配。"""

    model_config = ConfigDict(extra="forbid")

    local_field: str = Field(description="本源 Record 上的字段名")
    remote_field: str = Field(description="关联源 Record 上的字段名")
    # string：统一转字符串（默认，兼容数字/日期等不同存储类型）；
    # integer：双方都必须可转整数，否则记 type_mismatch；
    # datetime：双方都必须可解析为时间，否则记 type_mismatch。
    coerce: CoerceMode = "string"

    @field_validator("local_field", "remote_field")
    @classmethod
    def _check_field(cls, value: str) -> str:
        if value not in JOIN_FIELD_CHOICES:
            allowed = ", ".join(JOIN_FIELD_CHOICES)
            raise ValueError(f"关联键字段必须是以下之一: {allowed}")
        return value


class WideFieldRule(BaseModel):
    """宽表单个字段的生成规则。

    - first：按 sources 顺序取第一个非空值
    - concat：把各源标量值用 separator 拼接（去重、保序）
    - list：收集各源值成数组（标签等列表字段自动摊平、去重、保序）
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, pattern=r"^[A-Za-z][A-Za-z0-9_]*$")
    sources: list[str] = Field(min_length=1)
    strategy: FieldStrategy = "first"
    field: str | None = Field(default=None, description="取值字段，默认与 name 相同")
    separator: str = " | "
    default: str | None = None

    @field_validator("sources")
    @classmethod
    def _check_sources(cls, value: list[str]) -> list[str]:
        if not value or any(not item.strip() for item in value):
            raise ValueError("sources 不能为空字符串")
        if len(value) != len(set(value)):
            raise ValueError("sources 不能重复")
        return value

    @field_validator("field")
    @classmethod
    def _check_field(cls, value: str | None) -> str | None:
        if value is not None and value not in WIDE_FIELD_CHOICES:
            allowed = ", ".join(WIDE_FIELD_CHOICES)
            raise ValueError(f"宽表取值字段必须是以下之一: {allowed}")
        return value


class WideTableRule(BaseModel):
    """宽表生成规则：写入表名、字段规则与多命中冲突策略。"""

    model_config = ConfigDict(extra="forbid")

    table: str = Field(min_length=1, pattern=r"^[A-Za-z][A-Za-z0-9_]*$")
    fields: list[WideFieldRule] = Field(min_length=1)
    # merge：全部命中合并为一行，status=conflict 并留痕；
    # skip：不生成宽表行，仅写 conflict 事件（原始单源数据保留）。
    conflict_policy: ConflictPolicy = "merge"

    @field_validator("fields")
    @classmethod
    def _check_unique_names(cls, value: list[WideFieldRule]) -> list[WideFieldRule]:
        names = [rule.name for rule in value]
        if len(names) != len(set(names)):
            raise ValueError("宽表字段名不能重复")
        return value


class CorrelationConfig(BaseModel):
    """跨源关联配置。默认关闭，仅在源配置中显式声明时生效。"""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    remote_source: str = Field(min_length=1)
    join_keys: list[JoinKey] = Field(min_length=1)
    wide_table: WideTableRule


class SourceConfig(BaseModel):
    """单个源的配置。"""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    type: SourceType
    base_url: str
    enabled: bool = True
    page_size: int = Field(default=5, ge=1, le=100)
    timeout_seconds: float = Field(default=10.0, gt=0, le=120)
    list_path: str | None = None
    detail_path: str | None = None
    field_map: FieldMapping = Field(default_factory=FieldMapping)
    # 默认 None：不配置关联规则时，源的执行逻辑与单源管道完全一致。
    correlation: CorrelationConfig | None = None

    @field_validator("base_url")
    @classmethod
    def _check_base_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("base_url 必须以 http:// 或 https:// 开头")
        return value.rstrip("/")

    @model_validator(mode="after")
    def _fill_paths(self) -> "SourceConfig":
        list_default, detail_default = DEFAULT_PATHS[self.type]
        if self.list_path is None:
            self.list_path = list_default
        if self.detail_path is None:
            self.detail_path = detail_default
        if "{id}" not in self.detail_path:
            raise ValueError("detail_path 必须包含 {id} 占位符")
        return self


def load_sources(path: str | Path) -> list[SourceConfig]:
    """读取并校验源配置，任何问题统一抛 :class:`ConfigError`。"""

    config_path = Path(path)
    if not config_path.is_file():
        raise ConfigError(f"配置文件不存在: {config_path}")
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"YAML 解析失败: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"配置文件读取失败: {exc}") from exc

    if not isinstance(payload, dict) or "sources" not in payload:
        raise ConfigError("配置顶层必须是包含 sources 列表的映射")
    raw_sources = payload["sources"]
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ConfigError("sources 必须是非空列表")

    sources: list[SourceConfig] = []
    for index, item in enumerate(raw_sources, start=1):
        try:
            sources.append(SourceConfig.model_validate(item))
        except ValidationError as exc:
            raise ConfigError(f"sources[{index}] 校验失败: {exc}") from exc

    names = [source.name for source in sources]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ConfigError(f"源名称重复: {', '.join(duplicates)}")

    _validate_correlations(sources)
    return sources


def _validate_correlations(sources: list[SourceConfig]) -> None:
    """跨源校验关联配置：远端源存在、字段规则只引用本/远端源。"""

    by_name = {source.name: source for source in sources}
    for source in sources:
        correlation = source.correlation
        if correlation is None or not correlation.enabled:
            continue
        remote_name = correlation.remote_source
        if remote_name == source.name:
            raise ConfigError(
                f"sources 关联配置失败: 源 {source.name} 不能关联自身"
            )
        if remote_name not in by_name:
            raise ConfigError(
                f"sources 关联配置失败: 源 {source.name} 引用了不存在的关联源 "
                f"{remote_name}"
            )
        allowed_sources = {source.name, remote_name}
        for rule in correlation.wide_table.fields:
            unknown = [name for name in rule.sources if name not in allowed_sources]
            if unknown:
                raise ConfigError(
                    f"sources 关联配置失败: 源 {source.name} 宽表字段 {rule.name} "
                    f"引用了未参与关联的源 {', '.join(unknown)}"
                )
            effective_field = rule.field or rule.name
            if effective_field not in WIDE_FIELD_CHOICES:
                raise ConfigError(
                    f"sources 关联配置失败: 源 {source.name} 宽表字段 {rule.name} "
                    f"的取值字段 {effective_field} 不在允许列表内"
                )


def select_sources(
    sources: list[SourceConfig],
    *,
    name: str | None = None,
    all_sources: bool = False,
) -> list[SourceConfig]:
    """按 CLI 参数选择要跑的源。

    ``--all`` 只跑 enabled 源；显式 ``--source`` 允许运行被禁用的源（便于排障）。
    """

    if all_sources and name:
        raise ConfigError("--source 与 --all 不能同时使用")
    if all_sources:
        selected = [source for source in sources if source.enabled]
        if not selected:
            raise ConfigError("没有 enabled 的源可运行")
        return selected
    if not name:
        raise ConfigError("必须指定 --source 或 --all")
    for source in sources:
        if source.name == name:
            return [source]
    available = ", ".join(source.name for source in sources)
    raise ConfigError(f"未找到源 {name}，可用源: {available}")
