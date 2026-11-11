"""配置加载：读取 ``config/sources.yaml`` 并用 pydantic 校验。"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from crawl_flow.errors import ConfigError

SourceType = Literal["api", "html"]
SyncTargetType = Literal["webhook", "file"]

DEFAULT_PATHS: dict[str, tuple[str, str]] = {
    "api": ("/api/items", "/api/items/{id}"),
    "html": ("/list", "/item/{id}"),
}


class FieldMapping(BaseModel):
    """源字段 → 标准字段的映射。值写源数据里的键名。"""

    model_config = ConfigDict(extra="forbid")

    title: str = "title"
    category: str = "category"
    tags: str = "tags"
    published_at: str = "published_at"
    content: str = "content"
    url: str | None = None


class SyncTargetConfig(BaseModel):
    """单个外部同步目标：采集完成后把记录/原始响应异步投递过去。

    - ``webhook``：POST JSON 到 ``url``（覆盖连接失败/超时/4xx/5xx 等异常链路）
    - ``file``：以 JSONL 追加到 ``path`` 目录（交接目录、共享挂载等场景）
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    type: SyncTargetType
    enabled: bool = True
    timeout_seconds: float = Field(default=5.0, gt=0, le=60)
    url: str | None = None
    path: str | None = None

    @model_validator(mode="after")
    def _check_type_fields(self) -> "SyncTargetConfig":
        if self.type == "webhook":
            if not self.url:
                raise ValueError("webhook 类型的同步目标必须配置 url")
            if not self.url.startswith(("http://", "https://")):
                raise ValueError("webhook url 必须以 http:// 或 https:// 开头")
        if self.type == "file" and not self.path:
            raise ValueError("file 类型的同步目标必须配置 path")
        return self


class SyncSettings(BaseModel):
    """全局同步策略（配置文件顶层可选的 ``sync:`` 段）。"""

    model_config = ConfigDict(extra="forbid")

    max_workers: int = Field(default=2, ge=1, le=16)
    max_attempts: int = Field(default=3, ge=1, le=10)
    backoff_seconds: float = Field(default=0.2, ge=0, le=30)


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
    sync_targets: list[SyncTargetConfig] = Field(default_factory=list)

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
        target_names = [target.name for target in self.sync_targets]
        duplicates = sorted({name for name in target_names if target_names.count(name) > 1})
        if duplicates:
            raise ValueError(f"同步目标名称重复: {', '.join(duplicates)}")
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


def load_sync_settings(path: str | Path) -> SyncSettings:
    """读取配置文件顶层可选的 ``sync:`` 段；缺省时返回默认策略。

    与 :func:`load_sources` 读取同一文件；文件不存在/解析失败同样抛
    :class:`ConfigError`，``sync`` 段缺省或为空映射时使用默认值。
    """

    config_path = Path(path)
    if not config_path.is_file():
        raise ConfigError(f"配置文件不存在: {config_path}")
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"YAML 解析失败: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"配置文件读取失败: {exc}") from exc

    if not isinstance(payload, dict):
        raise ConfigError("配置顶层必须是包含 sources 列表的映射")
    raw_sync = payload.get("sync")
    if raw_sync is None:
        return SyncSettings()
    try:
        return SyncSettings.model_validate(raw_sync)
    except ValidationError as exc:
        raise ConfigError(f"sync 段校验失败: {exc}") from exc


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
