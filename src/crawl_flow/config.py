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
    # 请求头规则：不配置时沿用 Fetcher 默认值；headers 会覆盖同名默认头。
    user_agent: str | None = Field(default=None, min_length=1)
    headers: dict[str, str] = Field(default_factory=dict)
    list_path: str | None = None
    detail_path: str | None = None
    field_map: FieldMapping = Field(default_factory=FieldMapping)
    # 健康探测配置；probe_timeout_seconds 省略时默认取 timeout_seconds 的一半。
    probe_enabled: bool = False
    probe_timeout_seconds: float | None = Field(default=None, gt=0, le=120)

    @field_validator("headers")
    @classmethod
    def _check_headers(cls, value: dict[str, str]) -> dict[str, str]:
        for key, header_value in value.items():
            if not str(key).strip():
                raise ValueError("headers 的键不能为空")
            if "\n" in str(key) or "\n" in str(header_value):
                raise ValueError("headers 的键和值不能包含换行")
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

    def effective_probe_timeout(self) -> float:
        """探测超时：显式配置优先，否则为正常采集超时的一半。"""

        if self.probe_timeout_seconds is not None:
            return self.probe_timeout_seconds
        return self.timeout_seconds / 2


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
