"""预算账本单元测试：原子记账、UTC 归零、并发安全与损坏拒绝启动。"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Any

import pytest

from crawl_flow.budget import BudgetCorruptError, BudgetExceededError, BudgetLedger, BudgetLimits


def test_reserve_request_increments_and_persists(
    ledger: BudgetLedger, make_limits: Any
) -> None:
    limits = make_limits(requests=5)
    counters = ledger.reserve_request(limits)
    assert counters.request_count == 1
    assert counters.response_bytes == 0

    # 重新打开同一文件，计数必须已落盘。
    ledger_path = ledger.path
    ledger.close()
    reopened = BudgetLedger(ledger_path)
    try:
        assert reopened.get_counters(limits.source).request_count == 1
    finally:
        reopened.close()


def test_request_limit_blocks_ninth_request_before_increment(
    ledger: BudgetLedger, make_limits: Any
) -> None:
    limits = make_limits(requests=8)
    for _ in range(8):
        ledger.reserve_request(limits)

    snapshot = ledger.get_counters(limits.source)
    assert snapshot.request_count == 8

    with pytest.raises(BudgetExceededError) as exc_info:
        ledger.reserve_request(limits)
    assert exc_info.value.metric == "requests"
    assert exc_info.value.used == 8
    assert exc_info.value.limit == 8
    # 熔断那次不能计数：账本仍然停在 8。
    assert ledger.get_counters(limits.source).request_count == 8


def test_unlimited_source_never_breaks(ledger: BudgetLedger, make_limits: Any) -> None:
    limits = make_limits()  # 两个上限均为 None
    for _ in range(100):
        counters = ledger.reserve_request(limits)
    assert counters.request_count == 100
    ledger.add_response_bytes(limits.source, 4096)
    assert ledger.get_counters(limits.source).response_bytes == 4096
    assert not ledger.is_byte_limit_reached(limits)


def test_bytes_accumulation_and_byte_limit_blocks_next_request(
    ledger: BudgetLedger, make_limits: Any
) -> None:
    limits = make_limits(requests=100, bytes_=1000)
    ledger.reserve_request(limits)
    ledger.add_response_bytes(limits.source, 600)
    ledger.reserve_request(limits)
    ledger.add_response_bytes(limits.source, 400)

    assert ledger.is_byte_limit_reached(limits)
    with pytest.raises(BudgetExceededError) as exc_info:
        ledger.reserve_request(limits)
    assert exc_info.value.metric == "bytes"
    # 被拦截的请求不计数。
    assert ledger.get_counters(limits.source).request_count == 2


def test_utc_day_partition_resets_naturally(
    ledger: BudgetLedger, make_limits: Any
) -> None:
    limits = make_limits(requests=8)
    ledger.reserve_request(limits, usage_date="2026-11-11")
    ledger.reserve_request(limits, usage_date="2026-11-11")
    next_day = ledger.reserve_request(limits, usage_date="2026-11-12")

    assert next_day.request_count == 1
    assert next_day.usage_date == "2026-11-12"
    # 旧一天的数据保留备查。
    old = ledger.get_counters(limits.source, usage_date="2026-11-11")
    assert old.request_count == 2
    rows = ledger.list_counters(usage_date="2026-11-12")
    assert len(rows) == 1
    assert rows[0].request_count == 1


def test_concurrent_reservations_never_overshoot(tmp_path: Path, make_limits: Any) -> None:
    """两个线程（各自独立连接，等价两个进程）并发预留，计数不重不漏不超额。"""

    db_path = tmp_path / "concurrent.db"
    limits = BudgetLimits(source="race", daily_request_limit=50, daily_bytes_limit=None)
    thread_count = 5
    per_thread = 20

    def worker() -> None:
        local_ledger = BudgetLedger(db_path)
        try:
            for _ in range(per_thread):
                try:
                    local_ledger.reserve_request(limits)
                except BudgetExceededError:
                    return
        finally:
            local_ledger.close()

    threads = [threading.Thread(target=worker) for _ in range(thread_count)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    verifier = BudgetLedger(db_path)
    try:
        counters = verifier.get_counters("race")
        assert counters.request_count == 50  # 恰好停在上限，无重无漏
        with pytest.raises(BudgetExceededError):
            verifier.reserve_request(limits)
    finally:
        verifier.close()


def test_corrupt_garbage_file_refuses_to_open(tmp_path: Path) -> None:
    db_path = tmp_path / "garbage.db"
    db_path.write_bytes(b"this is definitely not a sqlite database" * 20)
    with pytest.raises(BudgetCorruptError):
        BudgetLedger(db_path)


def test_wrong_schema_refuses_to_open(tmp_path: Path) -> None:
    db_path = tmp_path / "wrong_schema.db"
    # 外部以错误结构抢先建了同名表。
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "CREATE TABLE budget_usage ("
            "source TEXT PRIMARY KEY, request_count INTEGER)"
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(BudgetCorruptError, match="表结构"):
        BudgetLedger(db_path)


def test_tampered_row_values_refuse_to_open(tmp_path: Path, make_limits: Any) -> None:
    db_path = tmp_path / "tampered.db"
    good_ledger = BudgetLedger(db_path)
    limits = make_limits(requests=10)
    good_ledger.reserve_request(limits)
    good_ledger.close()

    # 绕过 CHECK 直接把计数改成负值（模拟文件被篡改/页损坏）。
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA ignore_check_constraints = ON")
        conn.execute(
            "UPDATE budget_usage SET request_count = -5 WHERE source = ?",
            (limits.source,),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(BudgetCorruptError):
        BudgetLedger(db_path)


def test_tampered_text_count_refuses_to_open(tmp_path: Path, make_limits: Any) -> None:
    db_path = tmp_path / "tampered_text.db"
    good_ledger = BudgetLedger(db_path)
    limits = make_limits(requests=10)
    good_ledger.reserve_request(limits)
    good_ledger.close()

    conn = sqlite3.connect(db_path)
    try:
        # sqlite 动态类型允许把 TEXT 写进 INTEGER 列。
        conn.execute(
            "UPDATE budget_usage SET response_bytes = 'many' WHERE source = ?",
            (limits.source,),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(BudgetCorruptError, match="response_bytes"):
        BudgetLedger(db_path)


def test_tampered_date_refuses_to_open(tmp_path: Path, make_limits: Any) -> None:
    db_path = tmp_path / "tampered_date.db"
    good_ledger = BudgetLedger(db_path)
    limits = make_limits(requests=10)
    good_ledger.reserve_request(limits)
    good_ledger.close()

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            "UPDATE budget_usage SET usage_date = 'not-a-date' WHERE source = ?",
            (limits.source,),
        )
        conn.commit()
    finally:
        conn.close()

    with pytest.raises(BudgetCorruptError, match="usage_date"):
        BudgetLedger(db_path)


def test_multiple_sources_have_independent_budgets(
    ledger: BudgetLedger, make_limits: Any
) -> None:
    alpha = BudgetLimits(source="alpha", daily_request_limit=2, daily_bytes_limit=None)
    beta = BudgetLimits(source="beta", daily_request_limit=100, daily_bytes_limit=None)

    ledger.reserve_request(alpha)
    ledger.reserve_request(alpha)
    ledger.reserve_request(beta)

    with pytest.raises(BudgetExceededError) as exc_info:
        ledger.reserve_request(alpha)
    assert exc_info.value.source == "alpha"
    # beta 不受 alpha 熔断影响。
    assert ledger.reserve_request(beta).request_count == 2
    rows = {row.source: row for row in ledger.list_counters()}
    assert rows["alpha"].request_count == 2
    assert rows["beta"].request_count == 2
