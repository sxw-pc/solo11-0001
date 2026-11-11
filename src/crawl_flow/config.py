"""配置加载：读取 ``config/sources.yaml`` 并用 pydantic 校验。"""

from __future__ import annotations

import os
import re
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

# 仅支持 ``${NAME}`` 形式；变量名遵循常见 shell 标识符规则，保证跨平台一致。
ENV_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# 这些字段参与适配器注册 / 身份识别，写成枚举或严格标识符，禁止环境变量占位。
PLACEHOLDER_FORBIDDEN_FIELDS = ("name", "type")


def _resolve_env_string(value: str, *, path: str, environ: Mapping[str, str]) -> str:
    """替换字符串中的全部 ``${ENV_VAR}``；缺失变量抛 :class:`ConfigError`。"""

    def replace(match: re.Match[str]) -> str:
        variable = match.group(1)
        if variable not in environ:
            raise ConfigError(f"环境变量缺失: {variable}（字段 {path}）")
        return environ[variable]

    return ENV_PLACEHOLDER.sub(replace, value)


def _expand_env_placeholders(
    value: Any,
    *,
    path: str,
    environ: Mapping[str, str],
) -> Any:
    """递归展开配置值中的环境变量占位，字典键不参与展开。"""

    if isinstance(value, str):
        return _resolve_env_string(value, path=path, environ=environ)
    if isinstance(value, list):
        return [
            _expand_env_placeholders(item, path=f"{path}[{index}]", environ=environ)
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        return {
            key: _expand_env_placeholders(
                item,
                path=f"{path}.{key}" if path else str(key),
                environ=environ,
            )
            for key, item in value.items()
        }
    return value


def _check_placeholder_allowed(item: Any, *, index: int) -> None:
    """枚举 / 标识符字段禁止占位符，避免破坏适配器注册与源身份。"""

    if not isinstance(item, dict):
        return
    for field in PLACEHOLDER_FORBIDDEN_FIELDS:
        raw_value = item.get(field)
        if isinstance(raw_value, str) and ENV_PLACEHOLDER.search(raw_value):
            raise ConfigError(
                f"禁止在枚举字段 sources[{index}].{field} 中使用环境变量占位符"
            )


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


def load_sources(
    path: str | Path,
    *,
    environ: Mapping[str, str] | None = None,
) -> list[SourceConfig]:
    """读取并校验源配置，任何问题统一抛 :class:`ConfigError`。

    字符串字段支持 ``${ENV_VAR}`` 环境变量占位；``name`` / ``type`` 等
    枚举与标识符字段禁止使用占位符。
    """

    env = os.environ if environ is None else environ
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
        _check_placeholder_allowed(item, index=index)
        resolved_item = _expand_env_placeholders(
            item,
            path=f"sources[{index}]",
            environ=env,
        )
        try:
            sources.append(SourceConfig.model_validate(resolved_item))
        except ValidationError as exc:
            raise ConfigError(f"sources[{index}] 校验失败: {exc}") from exc

    names = [source.name for source in sources]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ConfigError(f"源名称重复: {', '.join(duplicates)}")
    return sources


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
