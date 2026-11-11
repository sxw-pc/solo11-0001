"""预算与采集管道集成：真实 HTTP 请求计数、熔断时机、dry-run 估算。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from crawl_flow.budget import (
    BudgetExceededError,
    BudgetLedger,
    BudgetLimits,
    DryRunBudgetMeter,
    SqliteBudgetMeter,
)
from crawl_flow.fetch.client import Fetcher
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore


def test_every_http_request_is_metered(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    meter: SqliteBudgetMeter,
) -> None:
    # limit=2 仍会翻完所有列表页（4 页：3 页有数据 + 1 页空页终止），
    # 共 4 个列表请求 + 2 个详情请求 = 6 次真实请求，逐次记账。
    source = make_source("demo_api", daily_request_limit=100)
    meter.configure_source(BudgetLimits.from_source(source))
    with Fetcher(meter=meter) as fetcher:
        summary = run_source(source, db, raw_store, fetcher=fetcher, limit=2, meter=meter)

    assert summary.budget_requests == 6
    assert summary.budget_bytes > 0
    assert not summary.budget_broken
    snapshot = meter.snapshot("demo_api")
    assert snapshot.request_count == 6
    assert snapshot.response_bytes == summary.budget_bytes


def test_request_cap_breaks_before_ninth_request_and_keeps_items(
    make_source: Any,
    tmp_path: Path,
) -> None:
    """验收场景：每日 8 次请求，第 9 次请求发出前熔断，已完成条目不丢。"""

    source = make_source("demo_api", daily_request_limit=8)
    ledger = BudgetLedger(tmp_path / "budget.db")
    meter = SqliteBudgetMeter(ledger)
    meter.configure_source(BudgetLimits.from_source(source))
    database = Database(tmp_path / "crawl.db")
    database.init()
    store = RawStore(tmp_path / "raw")

    try:
        with Fetcher(meter=meter) as fetcher:
            summary = run_source(source, database, store, fetcher=fetcher, meter=meter)
    finally:
        ledger.close()
        database.close()

    # 第 1 页列表(1) + 5 个详情(5) + 第 2 页列表(1) + 1 个详情(1) = 8 次请求，
    # 下一次详情请求在发出前熔断。
    assert summary.budget_broken
    assert summary.budget_break_reason is not None
    assert "请求次数上限" in summary.budget_break_reason
    assert summary.budget_requests == 8
    assert summary.ok == 6  # 5 + 1，熔断前已完成的条目全部入库

    # 重新打开数据库独立核对：6 条记录已持久化，不依赖运行中的连接。
    checker = Database(tmp_path / "crawl.db")
    try:
        assert checker.count_records("demo_api") == 6
    finally:
        checker.close()


def test_second_run_same_day_rejects_immediately(
    make_source: Any,
    tmp_path: Path,
) -> None:
    """验收场景：当天第二轮一上来就被拒绝，不再发出任何请求。"""

    source = make_source("demo_api", daily_request_limit=8)
    db_path = tmp_path / "crawl.db"
    ledger_path = tmp_path / "budget.db"

    ledger = BudgetLedger(ledger_path)
    meter = SqliteBudgetMeter(ledger)
    limits = BudgetLimits.from_source(source)
    meter.configure_source(limits)
    database = Database(db_path)
    database.init()
    store = RawStore(tmp_path / "raw")
    with Fetcher(meter=meter) as fetcher:
        first = run_source(source, database, store, fetcher=fetcher, meter=meter)
    assert first.budget_broken and first.budget_requests == 8
    ledger.close()
    database.close()

    # 新进程视角：重新打开账本，计数仍是 8。
    second_ledger = BudgetLedger(ledger_path)
    second_meter = SqliteBudgetMeter(second_ledger)
    second_meter.configure_source(limits)
    second_db = Database(db_path)
    second_db.init()
    try:
        with Fetcher(meter=second_meter) as fetcher:
            second = run_source(
                source, second_db, store, fetcher=fetcher, meter=second_meter
            )
    finally:
        second_ledger.close()
        second_db.close()

    assert second.budget_broken
    # 本轮没有新增请求；汇总展示的是账本全天累计（仍为 8）。
    assert second.budget_requests == 8
    assert second.ok == 0  # 一个条目都没处理

    verifier = BudgetLedger(ledger_path)
    try:
        assert verifier.get_counters("demo_api").request_count == 8
        assert verifier.get_counters("demo_api").response_bytes > 0
    finally:
        verifier.close()


def test_dry_run_only_estimates_and_never_writes_ledger(
    make_source: Any,
    tmp_path: Path,
) -> None:
    source = make_source("demo_api", daily_request_limit=3)
    ledger_path = tmp_path / "budget.db"
    assert not ledger_path.exists()

    meter = DryRunBudgetMeter()
    meter.configure_source(BudgetLimits.from_source(source))
    database = Database(tmp_path / "crawl.db")
    database.init()
    store = RawStore(tmp_path / "raw")
    with Fetcher(meter=meter) as fetcher:
        summary = run_source(
            source, database, store, fetcher=fetcher, dry_run=True, meter=meter
        )
    database.close()

    # 演练照样跑完整流程并估算出会超限，但不熔断、不建账本文件。
    assert summary.dry_run
    assert summary.budget_broken
    assert summary.budget_requests > 3
    assert summary.budget_bytes > 0
    assert not ledger_path.exists()
    estimate = meter.estimate("demo_api")
    assert estimate.would_break


def test_meter_without_budget_keeps_legacy_behavior(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    """不注入记账器时，Fetcher 与管道行为与改造前完全一致。"""

    source = make_source("demo_api")
    with Fetcher() as fetcher:
        summary = run_source(source, db, raw_store, fetcher=fetcher, limit=3)
    assert (summary.ok, summary.failed) == (3, 0)
    assert summary.budget_requests == 0
    assert summary.budget_bytes == 0


def test_byte_budget_break_is_reported(
    make_source: Any,
    tmp_path: Path,
) -> None:
    # 1 字节上限：第一个响应记账后立刻触顶，第二次请求前熔断。
    source = make_source("demo_api", daily_request_limit=100, daily_bytes_limit=1)
    ledger = BudgetLedger(tmp_path / "budget-bytes.db")
    meter = SqliteBudgetMeter(ledger)
    meter.configure_source(BudgetLimits.from_source(source))
    database = Database(tmp_path / "crawl.db")
    database.init()
    store = RawStore(tmp_path / "raw")
    try:
        with Fetcher(meter=meter) as fetcher:
            summary = run_source(source, database, store, fetcher=fetcher, meter=meter)
    finally:
        ledger.close()
        database.close()

    assert summary.budget_broken
    assert "响应字节上限" in (summary.budget_break_reason or "")
    # 只有首个列表页请求成功，后续全部被拦。
    assert summary.budget_requests == 1
    assert summary.ok == 0


def test_budget_exceeded_propagates_from_fetcher_directly(
    make_source: Any,
    meter: SqliteBudgetMeter,
) -> None:
    source = make_source("demo_api", daily_request_limit=1)
    meter.configure_source(BudgetLimits.from_source(source))
    meter.bind_source("demo_api")
    with Fetcher(meter=meter) as fetcher:
        fetcher.get(f"{source.base_url}/api/items")  # 第 1 次放行
        with pytest.raises(BudgetExceededError):
            fetcher.get(f"{source.base_url}/api/items")  # 第 2 次发出前熔断
    meter.unbind_source()
    assert meter.snapshot("demo_api").request_count == 1


def test_other_source_unaffected_when_one_breaks(
    make_source: Any,
    tmp_path: Path,
) -> None:
    capped = make_source("capped", daily_request_limit=2)
    healthy = make_source("healthy", daily_request_limit=100)
    ledger = BudgetLedger(tmp_path / "budget.db")
    meter = SqliteBudgetMeter(ledger)
    meter.configure_source(BudgetLimits.from_source(capped))
    meter.configure_source(BudgetLimits.from_source(healthy))
    database = Database(tmp_path / "crawl.db")
    database.init()
    store = RawStore(tmp_path / "raw")

    try:
        with Fetcher(meter=meter) as fetcher:
            broken_summary = run_source(
                capped, database, store, fetcher=fetcher, meter=meter
            )
            healthy_summary = run_source(
                healthy, database, store, fetcher=fetcher, limit=1, meter=meter
            )
    finally:
        ledger.close()
        database.close()

    assert broken_summary.budget_broken
    assert not healthy_summary.budget_broken
    assert healthy_summary.ok == 1
    # 两个源各记各的账：capped 停在自己的上限，healthy 有独立计数。
    checker = BudgetLedger(tmp_path / "budget.db")
    try:
        assert checker.get_counters("capped").request_count == 2
        assert checker.get_counters("healthy").request_count >= 2
    finally:
        checker.close()
