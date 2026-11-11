"""调度器配置模型：全局、单源两个维度的并发/速率配置与域名级限流规则。"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from crawl_flow.errors import ConfigError

DomainMatch = Literal["exact", "suffix"]


class DomainRateRule(BaseModel):
    """域名级限流规则。

    - ``match=exact``：仅对完全相同的 host 生效（忽略大小写）；
    - ``match=suffix``：对子域也生效，例如 ``example.com`` 同时覆盖
      ``api.example.com``；规则按「最长后缀优先」匹配，最具体的规则胜出。
    """

    model_config = ConfigDict(extra="forbid")

    domain: str = Field(min_length=1)
    qps: float = Field(gt=0, le=10_000)
    match: DomainMatch = "exact"

    @field_validator("domain")
    @classmethod
    def _normalize_domain(cls, value: str) -> str:
        domain = value.strip().lower()
        if not domain or "/" in domain or ":" in domain:
            raise ValueError("domain 只能是主机名，不能含协议或路径")
        return domain


class SourceScheduleConfig(BaseModel):
    """单源调度配置；缺省字段表示沿用全局配置。"""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, pattern=r"^[A-Za-z0-9_-]+$")
    max_concurrency: int | None = Field(default=None, ge=1, le=10_000)
    qps: float | None = Field(default=None, gt=0, le=10_000)
    weight: int = Field(default=1, ge=1, le=10_000)


class SchedulerConfig(BaseModel):
    """调度器总配置。

    - ``max_concurrency``：全局并发上限（所有源在途请求总数）；
    - ``qps``：全局 QPS 上限，内部折算成 1000ms 滑动窗口；
    - ``default_source_concurrency`` / ``default_source_qps``：源级缺省值；
    - ``sources``：单源覆盖项，``weight`` 决定加权配额比例（默认 1）；
    - ``domain_rules``：域名级限流，触发后对应域名请求排队等待而非失败。
    """

    model_config = ConfigDict(extra="forbid")

    max_concurrency: int = Field(default=8, ge=1, le=10_000)
    qps: float | None = Field(default=None, gt=0, le=100_000)
    default_source_concurrency: int = Field(default=2, ge=1, le=10_000)
    default_source_qps: float | None = Field(default=None, gt=0, le=100_000)
    sources: list[SourceScheduleConfig] = Field(default_factory=list)
    domain_rules: list[DomainRateRule] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_consistency(self) -> "SchedulerConfig":
        for source in self.sources:
            if (
                source.max_concurrency is not None
                and source.max_concurrency > self.max_concurrency
            ):
                raise ValueError(
                    f"源 {source.name} 的 max_concurrency={source.max_concurrency} "
                    f"不能超过全局 max_concurrency={self.max_concurrency}"
                )
        names = [item.name for item in self.sources]
        duplicates = sorted({name for name in names if names.count(name) > 1})
        if duplicates:
            raise ValueError(f"调度源配置重复: {', '.join(duplicates)}")
        domains = [rule.domain for rule in self.domain_rules]
        duplicate_domains = sorted(
            {domain for domain in domains if domains.count(domain) > 1}
        )
        if duplicate_domains:
            raise ValueError(f"域名限流规则重复: {', '.join(duplicate_domains)}")
        return self

    def source_overrides(self, name: str) -> SourceScheduleConfig:
        """返回源的调度覆盖项；不存在时构造一份默认权重的配置。"""

        for item in self.sources:
            if item.name == name:
                return item
        return SourceScheduleConfig(name=name)

    def effective_source_concurrency(self, name: str) -> int:
        override = self.source_overrides(name)
        return min(
            override.max_concurrency or self.default_source_concurrency,
            self.max_concurrency,
        )

    def effective_source_qps(self, name: str) -> float | None:
        override = self.source_overrides(name)
        qps = override.qps if override.qps is not None else self.default_source_qps
        if qps is None:
            return None
        if self.qps is not None:
            return min(qps, self.qps)
        return qps


def load_scheduler_config(payload: dict[str, object]) -> SchedulerConfig:
    """从已解析的 YAML/字典构造调度配置，错误统一转 :class:`ConfigError`。"""

    try:
        return SchedulerConfig.model_validate(payload)
    except ValueError as exc:
        raise ConfigError(f"调度配置校验失败: {exc}") from exc


def load_scheduler_yaml(path: str | Path) -> SchedulerConfig:
    """读取调度配置 YAML；文件不存在或没有 ``scheduler`` 段时返回默认配置。"""

    from pathlib import Path

    import yaml

    config_path = Path(path)
    if not config_path.is_file():
        return SchedulerConfig()
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"调度配置 YAML 解析失败: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"调度配置读取失败: {exc}") from exc
    if payload is None:
        return SchedulerConfig()
    if not isinstance(payload, dict):
        raise ConfigError("调度配置顶层必须是映射")
    section = payload.get("scheduler", payload)
    if section is None:
        return SchedulerConfig()
    if not isinstance(section, dict):
        raise ConfigError("scheduler 配置段必须是映射")
    return load_scheduler_config(section)
