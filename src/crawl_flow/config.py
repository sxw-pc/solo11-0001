"""配置加载：读取 ``config/sources.yaml`` 并用 pydantic 校验。"""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from crawl_flow.errors import ConfigError

SourceType = Literal["api", "html"]

DEFAULT_PATHS: dict[str, tuple[str, str]] = {
    "api": ("/api/items", "/api/items/{id}"),
    "html": ("/list", "/item/{id}"),
}

SUPPORTED_PROXY_SCHEMES = ("http", "https", "socks5")


class FieldMapping(BaseModel):
    """源字段 → 标准字段的映射。值写源数据里的键名。"""

    model_config = ConfigDict(extra="forbid")

    title: str = "title"
    category: str = "category"
    tags: str = "tags"
    published_at: str = "published_at"
    content: str = "content"
    url: str | None = None


class ProxyConfig(BaseModel):
    """单个源的代理池配置。

    - ``urls``：代理地址列表，协议仅支持 http/https/socks5，至少一个、不可重复；
    - ``failure_threshold``：同一代理连续失败达到该次数后进入冷却；
    - ``cooldown_seconds``：冷却时长，到期后代理自动恢复可用。
    """

    model_config = ConfigDict(extra="forbid")

    urls: list[str] = Field(min_length=1)
    failure_threshold: int = Field(default=3, ge=1, le=100)
    cooldown_seconds: float = Field(default=60.0, gt=0, le=86400)

    @field_validator("urls")
    @classmethod
    def _check_urls(cls, value: list[str]) -> list[str]:
        normalized: list[str] = []
        for index, raw in enumerate(value):
            if not isinstance(raw, str) or not raw.strip():
                raise ValueError(f"proxies.urls[{index}] 不能为空")
            address = raw.strip()
            parsed = urlparse(address)
            if parsed.scheme not in SUPPORTED_PROXY_SCHEMES:
                raise ValueError(
                    f"proxies.urls[{index}] 协议必须是 "
                    f"{'/'.join(SUPPORTED_PROXY_SCHEMES)} 之一: {address}"
                )
            if not parsed.hostname or not parsed.port:
                raise ValueError(
                    f"proxies.urls[{index}] 必须包含主机名与端口: {address}"
                )
            normalized.append(address)
        if len(set(normalized)) != len(normalized):
            raise ValueError("proxies.urls 中存在重复代理地址")
        return normalized


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
    proxies: ProxyConfig | None = None

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
