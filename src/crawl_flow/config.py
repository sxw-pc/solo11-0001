"""配置加载：读取 ``config/sources.yaml`` 并用 pydantic 校验。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from crawl_flow.errors import ConfigError

SourceType = Literal["api", "html"]

DEFAULT_PATHS: dict[str, tuple[str, str]] = {
    "api": ("/api/items", "/api/items/{id}"),
    "html": ("/list", "/item/{id}"),
}

DEFAULT_NOTIFY_TIMEOUT_SECONDS = 5.0
DEFAULT_NOTIFY_RETRIES = 2
DEFAULT_NOTIFY_RETRY_BACKOFF_SECONDS = 0.5
DEFAULT_ALERT_FAILURE_RATE = 0.1


class FieldMapping(BaseModel):
    """源字段 → 标准字段的映射。值写源数据里的键名。"""

    model_config = ConfigDict(extra="forbid")

    title: str = "title"
    category: str = "category"
    tags: str = "tags"
    published_at: str = "published_at"
    content: str = "content"
    url: str | None = None


def _validate_webhook(value: str) -> str:
    """webhook 地址必须是 http(s) URL；密钥只允许出现在 URL 里（日志另行脱敏）。"""

    if not value or not value.strip():
        raise ValueError("webhook 地址不能为空")
    candidate = value.strip()
    parts = urlsplit(candidate)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise ValueError("webhook 必须是合法的 http(s) URL")
    return candidate


class SourceNotifyConfig(BaseModel):
    """单源通知覆盖：配置后该源只向这里的 webhook 推送，不再走全局地址。"""

    model_config = ConfigDict(extra="forbid")

    webhooks: list[str] = Field(min_length=1)

    @field_validator("webhooks")
    @classmethod
    def _check_webhooks(cls, value: list[str]) -> list[str]:
        return [_validate_webhook(item) for item in value]


class NotifySettings(BaseModel):
    """采集完成后的 IM webhook 通知配置（顶层 ``notify`` 段）。"""

    model_config = ConfigDict(extra="forbid")

    webhooks: list[str] = Field(default_factory=list)
    timeout_seconds: float = Field(
        default=DEFAULT_NOTIFY_TIMEOUT_SECONDS, gt=0, le=120
    )
    retries: int = Field(default=DEFAULT_NOTIFY_RETRIES, ge=0, le=10)
    retry_backoff_seconds: float = Field(
        default=DEFAULT_NOTIFY_RETRY_BACKOFF_SECONDS, ge=0, le=60
    )
    alert_failure_rate: float = Field(
        default=DEFAULT_ALERT_FAILURE_RATE, ge=0.0, le=1.0
    )

    @field_validator("webhooks")
    @classmethod
    def _check_webhooks(cls, value: list[str]) -> list[str]:
        return [_validate_webhook(item) for item in value]


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
    notify: SourceNotifyConfig | None = None

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
class LoadedConfig:
    """一次配置加载的结果：源列表 + 通知设置。"""

    sources: list[SourceConfig]
    notify: NotifySettings


def load_config(path: str | Path) -> LoadedConfig:
    """读取并校验源配置与通知配置，任何问题统一抛 :class:`ConfigError`。"""

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

    raw_notify = payload.get("notify")
    if raw_notify is None:
        notify = NotifySettings()
    elif isinstance(raw_notify, dict):
        try:
            notify = NotifySettings.model_validate(raw_notify)
        except ValidationError as exc:
            raise ConfigError(f"notify 校验失败: {exc}") from exc
    else:
        raise ConfigError("notify 必须是映射（mapping）")

    return LoadedConfig(sources=sources, notify=notify)


def load_sources(path: str | Path) -> list[SourceConfig]:
    """兼容旧调用方：只取源列表。"""

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
