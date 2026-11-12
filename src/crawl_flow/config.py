"""配置加载：读取 ``config/sources.yaml`` 并用 pydantic 校验。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from crawl_flow.errors import ConfigError

SourceType = Literal["api", "html"]

DEFAULT_PATHS: dict[str, tuple[str, str]] = {
    "api": ("/api/items", "/api/items/{id}"),
    "html": ("/list", "/item/{id}"),
}

# 差异对比允许的标准字段（与 ``FieldMapping`` / ``Record`` 的字段一致）。
ALLOWED_DIFF_FIELDS: tuple[str, ...] = (
    "title",
    "category",
    "tags",
    "published_at",
    "content",
    "url",
)
# 默认只对比三个核心字段，避免正文等大字段在无意间扩大对比成本。
DEFAULT_DIFF_FIELDS: tuple[str, ...] = ("title", "content", "published_at")

_DIFF_FIELD_SEPARATOR = re.compile(r"[,，、|;；\s]+")


def normalize_diff_fields(value: str | list[str] | tuple[str, ...]) -> list[str]:
    """把逗号分隔字符串或列表统一成合法的对比字段列表。

    空项会被忽略，重复字段只保留一次；出现未知字段直接抛 ``ValueError``，
    由配置层转换成 :class:`ConfigError`。
    """

    if isinstance(value, str):
        candidates: list[Any] = [part for part in _DIFF_FIELD_SEPARATOR.split(value)]
    elif isinstance(value, (list, tuple)):
        candidates = list(value)
    else:
        raise ValueError("对比字段必须是字符串或列表")

    fields: list[str] = []
    for candidate in candidates:
        if not isinstance(candidate, str):
            raise ValueError(f"对比字段必须是字符串: {candidate!r}")
        name = candidate.strip()
        if not name:
            continue
        if name not in ALLOWED_DIFF_FIELDS:
            allowed = ", ".join(ALLOWED_DIFF_FIELDS)
            raise ValueError(f"不支持的对比字段 {name!r}，可选: {allowed}")
        if name not in fields:
            fields.append(name)

    if not fields:
        raise ValueError("对比字段不能为空")
    return fields


class DiffConfig(BaseModel):
    """差异对比配置（配置文件顶层 ``diff`` 段）。"""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    fields: list[str] = Field(
        default_factory=lambda: list(DEFAULT_DIFF_FIELDS),
        alias="compare_fields",
        description="参与对比的标准字段，默认 title/content/published_at",
    )

    @field_validator("fields", mode="before")
    @classmethod
    def _check_fields(cls, value: Any) -> Any:
        if value is None:
            return list(DEFAULT_DIFF_FIELDS)
        return normalize_diff_fields(value)


class FieldMapping(BaseModel):
    """源字段 → 标准字段的映射。值写源数据里的键名。"""

    model_config = ConfigDict(extra="forbid")

    title: str = "title"
    category: str = "category"
    tags: str = "tags"
    published_at: str = "published_at"
    content: str = "content"
    url: str | None = None


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


@dataclass(frozen=True)
class ConfigBundle:
    """配置文件解析结果：源列表 + 顶层差异对比配置。"""

    sources: list[SourceConfig]
    diff: DiffConfig


def load_config(path: str | Path) -> ConfigBundle:
    """读取源配置与顶层 ``diff`` 段；任何问题统一抛 :class:`ConfigError`。"""

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

    raw_diff = payload.get("diff")
    if raw_diff is None:
        raw_diff = {}
    if not isinstance(raw_diff, dict):
        raise ConfigError("diff 必须是一个映射")
    # 同时接受 compare_fields（推荐）与 fields 两种写法，避免配置歧义。
    if "fields" in raw_diff and "compare_fields" in raw_diff:
        raise ConfigError("diff.compare_fields 与 diff.fields 只能二选一")
    try:
        diff = DiffConfig.model_validate(raw_diff)
    except ValidationError as exc:
        raise ConfigError(f"diff 校验失败: {exc}") from exc

    return ConfigBundle(sources=sources, diff=diff)


def load_sources(path: str | Path) -> list[SourceConfig]:
    """只读取源列表（保留原有接口，供现有调用方使用）。"""

    return load_config(path).sources


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
