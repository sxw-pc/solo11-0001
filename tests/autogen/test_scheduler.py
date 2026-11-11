"""周期校验调度器：周期约束、单任务隔离、漂移回调。"""

from __future__ import annotations

import pytest

from crawl_flow.autogen.scheduler import ValidationScheduler
from crawl_flow.autogen.validator import ValidationReport


class _StubValidator:
    def __init__(self, reports: list[ValidationReport]) -> None:
        self._reports = reports
        self.calls = 0

    def validate(self, spec, *, entry_url=None) -> ValidationReport:
        report = self._reports[min(self.calls, len(self._reports) - 1)]
        self.calls += 1
        return report


def _spec():
    from crawl_flow.autogen.spec import AdapterSpec

    return AdapterSpec.from_dict(
        {
            "base_url": "http://127.0.0.1:9999",
            "list": {
                "list_path": "/posts",
                "item_selector": "article.card",
                "link_selector": "a",
                "id_pattern": r"/posts/([^/?#]+)(?:[?#]|$)",
                "pagination": "next_link",
                "next_selector": "a.next",
            },
            "detail": {
                "detail_path": "/posts/{id}",
                "scope_selector": "article.entry",
                "fields": {"title": {"selector": "h1", "mode": "text"}},
            },
        }
    )


def test_interval_shorter_than_60s_rejected() -> None:
    with pytest.raises(ValueError):
        ValidationScheduler(interval_seconds=30)


def test_run_once_isolates_job_failure() -> None:
    class _Boom:
        def validate(self, spec, *, entry_url=None):
            raise RuntimeError("network down")

    scheduler = ValidationScheduler(validator=_Boom(), interval_seconds=3600)  # type: ignore[arg-type]
    scheduler.register("broken", _spec())
    reports = scheduler.run_once()
    assert len(reports) == 1
    assert reports[0].status == "broken"
    assert any(f.component == "scheduler" for f in reports[0].findings)


def test_drift_callback_fires_on_change() -> None:
    drifted = ValidationReport(entry_url="http://x", status="drifted")
    results: list[tuple[str, str]] = []
    scheduler = ValidationScheduler(
        validator=_StubValidator([drifted]),
        interval_seconds=3600,
        on_result=lambda name, report: results.append((name, report.status)),
    )
    scheduler.register("blog", _spec())
    scheduler.run_once()
    assert results == [("blog", "drifted")]
    assert scheduler.jobs["blog"].last_report is drifted


def test_scheduler_start_stop() -> None:
    import time

    healthy = ValidationReport(entry_url="http://x", status="healthy")
    scheduler = ValidationScheduler(validator=_StubValidator([healthy]), interval_seconds=3600)
    scheduler.register("blog", _spec())
    scheduler.start()
    time.sleep(0.2)
    assert scheduler._thread is not None and scheduler._thread.is_alive()  # noqa: SLF001
    scheduler.stop(timeout=2)
    assert not scheduler._thread.is_alive()  # noqa: SLF001
