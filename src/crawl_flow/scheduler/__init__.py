"""轻量定时编排与指标模块（纯标准库 + 现有 SQLite 存储，无重型框架）。

子模块 ``engine`` 依赖 ``config``，而 ``config`` 又需要 ``scheduler.cron``
校验 cron 表达式，因此这里用 PEP 562 的模块级 ``__getattr__`` 懒加载，
避免包初始化阶段产生循环导入。
"""

from __future__ import annotations

import importlib
from typing import Any

_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "CronParseError": ("crawl_flow.scheduler.cron", "CronParseError"),
    "CronSpec": ("crawl_flow.scheduler.cron", "CronSpec"),
    "parse_cron": ("crawl_flow.scheduler.cron", "parse_cron"),
    "JobPlan": ("crawl_flow.scheduler.engine", "JobPlan"),
    "Scheduler": ("crawl_flow.scheduler.engine", "Scheduler"),
    "build_job_plans": ("crawl_flow.scheduler.engine", "build_job_plans"),
    "CollectionResult": ("crawl_flow.scheduler.runner", "CollectionResult"),
    "RunRecorder": ("crawl_flow.scheduler.runner", "RunRecorder"),
    "run_collection": ("crawl_flow.scheduler.runner", "run_collection"),
    "MetricsServer": ("crawl_flow.scheduler.server", "MetricsServer"),
    "ensure_internal_host": ("crawl_flow.scheduler.server", "ensure_internal_host"),
    "JobRun": ("crawl_flow.scheduler.store", "JobRun"),
    "JobStats": ("crawl_flow.scheduler.store", "JobStats"),
    "ScheduledJob": ("crawl_flow.scheduler.store", "ScheduledJob"),
    "SchedulerStore": ("crawl_flow.scheduler.store", "SchedulerStore"),
}

__all__ = list(_LAZY_EXPORTS)


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target
    value = getattr(importlib.import_module(module_name), attr)
    globals()[name] = value
    return value
