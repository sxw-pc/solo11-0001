"""配置加载：读取 ``config/sources.yaml`` 并用 pydantic 校验。"""

from __future__ import annotations

from collections.abc import Mapping
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

#: HTML 详情页五类字段的全局默认 CSS 选择器。
DEFAULT_SELECTORS: dict[str, str] = {
    "title": "h1.title",
    "category": "span.category",
    "tags": ".tags",
    "date": "span.date",
    "content": "div.content",
}

SELECTOR_FIELDS: tuple[str, ...] = tuple(DEFAULT_SELECTORS)


class FieldMapping(BaseModel):
    """源字段 → 标准字段的映射。值写源数据里的键名。"""

    model_config = ConfigDict(extra="forbid")

    title: str = "title"
    category: str = "category"
    tags: str = "tags"
    published_at: str = "published_at"
    content: str = "content"
    url: str | None = None


class SelectorConfig(BaseModel):
    """HTML 源的 CSS 选择器覆盖。

    字段为 ``None``（缺省）时沿用 :data:`DEFAULT_SELECTORS` 里的全局默认；
    显式传入 ``null``、非字符串或空白字符串会在配置加载阶段直接报错。
    """

    model_config = ConfigDict(extra="forbid")

    title: str | None = None
    category: str | None = None
    tags: str | None = None
    date: str | None = None
    content: str | None = None

    @field_validator("*", mode="before")
    @classmethod
    def _check_selector_value(cls, value: Any, info: Any) -> Any:
        field_name = info.field_name
        if value is None:
            # 缺省（不传）与显式 null 在 pydantic 中都会走到这里，
            # 再用 model_fields_set 区分二者。
            return None
        if not isinstance(value, str):
            raise ValueError(
                f"selectors.{field_name} 必须是 CSS 选择器字符串，"
                f"实际类型为 {type(value).__name__}"
            )
        if not value.strip():
            raise ValueError(f"selectors.{field_name} 不能为空字符串")
        return value

    @model_validator(mode="after")
    def _reject_explicit_null(self) -> "SelectorConfig":
        for field in SELECTOR_FIELDS:
            if field in self.model_fields_set and getattr(self, field) is None:
                raise ValueError(f"selectors.{field} 缺失：请配置选择器或省略该字段以使用默认值")
        return self

    def resolved(self) -> dict[str, str]:
        """合并全局默认，返回五项最终生效的选择器。"""

        return {
            field: getattr(self, field) or DEFAULT_SELECTORS[field]
            for field in SELECTOR_FIELDS
        }


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
    selectors: SelectorConfig = Field(default_factory=SelectorConfig)

    @field_validator("selectors", mode="before")
    @classmethod
    def _check_selectors_section(cls, value: Any) -> Any:
        if value is None:
            raise ValueError("selectors 缺失：请配置为选择器映射，或省略该字段以使用默认选择器")
        if not isinstance(value, Mapping):
            raise ValueError(
                f"selectors 必须是选择器映射（mapping），实际类型为 {type(value).__name__}"
            )
        return value

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
        name = item.get("name") if isinstance(item, dict) else None
        location = (
            f"sources[{index}](name={name})"
            if isinstance(name, str) and name
            else f"sources[{index}]"
        )
        try:
            sources.append(SourceConfig.model_validate(item))
        except ValidationError as exc:
            raise ConfigError(
                f"{location} 校验失败: {_format_validation_error(exc)}"
            ) from exc

    names = [source.name for source in sources]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ConfigError(f"源名称重复: {', '.join(duplicates)}")
    return sources


def _format_validation_error(exc: ValidationError) -> str:
    """把 pydantic 校验错误压成 ``字段路径: 原因`` 的可读字符串。"""

    details: list[str] = []
    for error in exc.errors():
        path = ".".join(str(part) for part in error["loc"]) or "<root>"
        message = error["msg"].removeprefix("Value error, ")
        if error["type"] == "extra_forbidden":
            message = f"未知配置键 {path}，请检查拼写或移除该字段"
            path = "<root>"
        details.append(f"{path}: {message}" if path != "<root>" else message)
    return "; ".join(details)


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
