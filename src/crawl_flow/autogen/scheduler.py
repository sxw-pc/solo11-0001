"""适配校验的轻量周期调度器。

定位是「库内调度器」而非独立任务平台：一个后台守护线程按固定间隔
（默认 1 小时）对登记的适配器逐个跑 :class:`AdapterValidator`。

资源约束：

- 校验请求全部走共享 :class:`RateLimiter` 且带 ``PRIORITY_VALIDATION``，
  与正常采集共享同一份访问预算，不抢占正常业务任务；
- 单个适配器校验异常被隔离，只记录失败原因，不影响后续周期与其他适配器；
- 支持 ``run_once`` 便于测试与外部调度系统（cron / k8s CronJob）接管。
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone

from crawl_flow.autogen.constants import DEFAULT_VALIDATION_INTERVAL_SECONDS
from crawl_flow.autogen.spec import AdapterSpec
from crawl_flow.autogen.validator import (
    AdapterValidator,
    DriftFinding,
    ValidationReport,
)

LOGGER_NAME = "crawl_flow.autogen.scheduler"


@dataclass
class ValidationJob:
    name: str
    spec: AdapterSpec
    entry_url: str | None = None
    last_report: ValidationReport | None = None
    last_run_at: str | None = None
    history: list[ValidationReport] = field(default_factory=list)


class ValidationScheduler:
    """后台周期校验调度器（守护线程，进程退出即停）。"""

    def __init__(
        self,
        validator: AdapterValidator | None = None,
        *,
        interval_seconds: int = DEFAULT_VALIDATION_INTERVAL_SECONDS,
        on_result: "callable[[str, ValidationReport], None] | None" = None,
    ) -> None:
        if interval_seconds < 60:
            raise ValueError("校验周期不得短于 60 秒，避免对目标站点造成压力")
        self._validator = validator or AdapterValidator()
        self._interval = interval_seconds
        self._on_result = on_result
        self._jobs: dict[str, ValidationJob] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._log = logging.getLogger(LOGGER_NAME)

    def register(self, name: str, spec: AdapterSpec, *, entry_url: str | None = None) -> None:
        with self._lock:
            self._jobs[name] = ValidationJob(name=name, spec=spec, entry_url=entry_url)

    def unregister(self, name: str) -> None:
        with self._lock:
            self._jobs.pop(name, None)

    def run_once(self) -> list[ValidationReport]:
        """对全部登记适配器跑一轮校验，返回报告列表。"""

        with self._lock:
            jobs = list(self._jobs.values())
        reports: list[ValidationReport] = []
        for job in jobs:
            try:
                report = self._validator.validate(job.spec, entry_url=job.entry_url)
            except Exception as exc:  # noqa: BLE001 - 单任务隔离
                self._log.exception("validation job crashed name=%s", job.name)
                report = ValidationReport(entry_url=job.entry_url or "-", status="broken")
                report.findings.append(
                    DriftFinding(
                        component="scheduler",
                        expected_selector="-",
                        detail=f"校验任务异常: {exc}",
                    )
                )
            job.last_report = report
            job.last_run_at = datetime.now(timezone.utc).isoformat()
            job.history.append(report)
            reports.append(report)
            if self._on_result is not None:
                self._on_result(job.name, report)
            if report.changed:
                self._log.warning(
                    "adapter drift detected name=%s status=%s findings=%d",
                    job.name,
                    report.status,
                    len(report.findings),
                )
        return reports

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="autogen-validation", daemon=True)
        self._thread.start()

    def stop(self, *, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def _loop(self) -> None:
        while not self._stop.is_set():
            self.run_once()
            # 用 wait 而非 sleep，stop() 可以立即唤醒
            self._stop.wait(self._interval)

    @property
    def jobs(self) -> dict[str, ValidationJob]:
        with self._lock:
            return dict(self._jobs)
