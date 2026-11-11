"""配置加载：读取 ``config/sources.yaml`` 并用 pydantic 校验。"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from crawl_flow.errors import ConfigError

SourceType = Literal["api", "html"]
RuleType = Literal["word", "regex"]
WatchField = Literal["title", "content", "category", "tags"]

DEFAULT_PATHS: dict[str, tuple[str, str]] = {
    "api": ("/api/items", "/api/items/{id}"),
    "html": ("/list", "/item/{id}"),
}

DEFAULT_WATCH_FIELDS: tuple[str, ...] = ("title", "content")


class FieldMapping(BaseModel):
    """源字段 → 标准字段的映射。值写源数据里的键名。"""

    model_config = ConfigDict(extra="forbid")

    title: str = "title"
    category: str = "category"
    tags: str = "tags"
    published_at: str = "published_at"
    content: str = "content"
    url: str | None = None


class WatchRule(BaseModel):
    """单条布控规则：普通词（word，默认大小写不敏感）或正则（regex）。

    实例化时立即编译 pattern，非法正则在配置加载阶段就抛错，
    不会等到采集运行到一半才暴露。
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    type: RuleType = "word"
    pattern: str = Field(min_length=1, max_length=1024)
    ignore_case: bool | None = None
    fields: list[WatchField] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _compile_pattern(self) -> "WatchRule":
        ignore_case = self.ignore_case if self.ignore_case is not None else self.type == "word"
        flags = re.IGNORECASE if ignore_case else 0
        raw_pattern = self.pattern if self.type == "regex" else re.escape(self.pattern)
        try:
            re.compile(raw_pattern, flags)
        except re.error as exc:
            raise ValueError(f"布控规则 {self.name!r} 正则非法: {exc}") from exc
        return self

    def effective_ignore_case(self) -> bool:
        """word 默认不区分大小写；regex 默认区分，可被 ignore_case 覆盖。"""

        return self.ignore_case if self.ignore_case is not None else self.type == "word"


class WatchRuleSet(BaseModel):
    """一组布控规则；源级配置只允许声明 rules。"""

    model_config = ConfigDict(extra="forbid")

    rules: list[WatchRule] = Field(default_factory=list)


class WatchSettings(WatchRuleSet):
    """全局布控设置：规则列表 + 引擎参数。"""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    context_chars: int = Field(default=40, ge=1, le=1000)
    timeout_seconds: float = Field(default=1.0, gt=0, le=30.0)


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
    watch: WatchRuleSet | None = None

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


class AppConfig(BaseModel):
    """整份应用配置：源列表 + 全局布控设置。"""

    model_config = ConfigDict(extra="forbid")

    sources: list[SourceConfig] = Field(min_length=1)
    watch: WatchSettings = Field(default_factory=WatchSettings)

    @model_validator(mode="after")
    def _unique_watch_rule_names(self) -> "AppConfig":
        seen: dict[str, str] = {}
        duplicates: list[str] = []

        def _check(rules: list[WatchRule], scope: str) -> None:
            for rule in rules:
                previous = seen.get(rule.name)
                if previous is not None:
                    if rule.name not in duplicates:
                        duplicates.append(rule.name)
                else:
                    seen[rule.name] = scope

        _check(self.watch.rules, "全局")
        for source in self.sources:
            if source.watch is not None:
                _check(source.watch.rules, f"源 {source.name}")
        if duplicates:
            raise ValueError(f"布控规则名称重复（规则名需全局唯一）: {', '.join(duplicates)}")
        return self


def _read_yaml(path: str | Path) -> dict:
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
    return payload


def load_app_config(path: str | Path) -> AppConfig:
    """读取并校验整份配置（源 + 布控规则）。

    正则规则在此阶段编译，非法规则、未知字段、规则名重复都会抛
    :class:`ConfigError`，保证启动即失败。
    """

    payload = _read_yaml(path)
    try:
        config = AppConfig.model_validate(payload)
    except ValidationError as exc:
        raise ConfigError(f"配置校验失败: {exc}") from exc

    names = [source.name for source in config.sources]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ConfigError(f"源名称重复: {', '.join(duplicates)}")
    return config


def load_sources(path: str | Path) -> list[SourceConfig]:
    """读取并校验源配置，任何问题统一抛 :class:`ConfigError`。"""

    return load_app_config(path).sources


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
