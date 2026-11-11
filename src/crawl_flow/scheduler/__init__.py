"""定时调度：cron 解析、单进程调度器、执行与指标记录。

``service`` / ``runner`` 依赖 ``config``，为避免循环导入采用惰性导出。
"""

from crawl_flow.scheduler.cron import CronError, CronSchedule

__all__ = ["CronError", "CronSchedule", "SchedulerService", "execute_and_record"]


def __getattr__(name: str):  # PEP 562 惰性导入
    if name == "SchedulerService":
        from crawl_flow.scheduler.service import SchedulerService

        return SchedulerService
    if name == "execute_and_record":
        from crawl_flow.scheduler.runner import execute_and_record

        return execute_and_record
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
