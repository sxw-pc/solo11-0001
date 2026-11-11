"""请求调度与限流模块（单进程、可插拔）。

公开接口：

- :class:`SchedulerConfig` / :class:`SourceScheduleConfig` /
  :class:`DomainRateRule`：调度配置模型；
- :class:`SlidingWindowRateLimiter` / :class:`RateLimitConfig`：
  毫秒精度滑动窗口限流器；
- :class:`DomainRateRegistry`：域名级限流规则注册表；
- :class:`RequestScheduler`：加权 DRR 调度池，支持两级并发、多级 QPS、
  老化防饥饿与自动降级；
- :class:`ScheduledFetcher`：与 ``Fetcher`` 同接口的可插拔代理；
- :func:`build_scheduler_config`：从 YAML 字典构造配置；
- :func:`create_scheduler` / :func:`wrap_fetcher`：便捷工厂。
"""

from __future__ import annotations

from typing import Any

from crawl_flow.scheduler.client import ScheduledFetcher
from crawl_flow.scheduler.config import (
    DomainRateRule,
    SchedulerConfig,
    SourceScheduleConfig,
    load_scheduler_config,
    load_scheduler_yaml,
)
from crawl_flow.scheduler.domain_rules import DomainRateRegistry, extract_host
from crawl_flow.scheduler.pool import PendingRequest, RequestScheduler
from crawl_flow.scheduler.rate_limit import (
    RateLimitConfig,
    SlidingWindowRateLimiter,
    now_ms,
)


def build_scheduler_config(payload: dict[str, Any] | None) -> SchedulerConfig:
    """从 YAML 解析出的字典构造调度配置；``None`` 表示全默认值。"""

    if payload is None:
        return SchedulerConfig()
    return load_scheduler_config(payload)


def create_scheduler(
    config: SchedulerConfig,
    transport: Any,
    *,
    logger: Any = None,
) -> RequestScheduler:
    """基于配置与底层传输（``Fetcher``）创建调度池。"""

    return RequestScheduler(config, transport=transport, logger=logger)


def wrap_fetcher(
    source: str,
    scheduler: RequestScheduler,
    direct_fetcher: Any,
    *,
    logger: Any = None,
) -> ScheduledFetcher:
    """把底层 ``Fetcher`` 包装成走调度池的代理（适配器无感知）。"""

    return ScheduledFetcher(
        source, scheduler, direct_fetcher, logger=logger
    )


__all__ = [
    "DomainRateRegistry",
    "DomainRateRule",
    "PendingRequest",
    "RateLimitConfig",
    "RequestScheduler",
    "ScheduledFetcher",
    "SchedulerConfig",
    "SlidingWindowRateLimiter",
    "SourceScheduleConfig",
    "build_scheduler_config",
    "create_scheduler",
    "extract_host",
    "load_scheduler_config",
    "load_scheduler_yaml",
    "now_ms",
    "wrap_fetcher",
]
