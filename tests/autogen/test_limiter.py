"""共享限速器：最小间隔、优先级排队、老化防饿死。"""

from __future__ import annotations

import threading
import time

from crawl_flow.autogen.limiter import (
    PRIORITY_CRAWL,
    PRIORITY_VALIDATION,
    RateLimiter,
)


def test_min_interval_default_is_two_seconds() -> None:
    assert RateLimiter().min_interval == 2.0


def test_requests_are_spaced_by_min_interval() -> None:
    limiter = RateLimiter(min_interval_seconds=0.2)
    started = time.monotonic()
    for _ in range(3):
        limiter.acquire()
        limiter.release()
    assert time.monotonic() - started >= 0.4  # 3 次请求至少 2 个间隔


def test_higher_priority_dispatched_first_when_queued() -> None:
    """确定性优先级测试：占住通道 → 校验票先排队 → 采集票后排队 → 释放后采集先走。"""

    limiter = RateLimiter(min_interval_seconds=0.0)
    limiter.acquire(priority=PRIORITY_CRAWL)  # 占住通道

    results: list[int] = []
    lock = threading.Lock()
    validation_started = threading.Event()

    def wait_and_record(priority: int, started: threading.Event | None = None) -> None:
        if started is not None:
            started.set()
        limiter.acquire(priority=priority)
        with lock:
            results.append(priority)
        limiter.release()

    t_validation = threading.Thread(
        target=wait_and_record, args=(PRIORITY_VALIDATION, validation_started)
    )
    t_validation.start()
    validation_started.wait(timeout=2)
    time.sleep(0.05)  # 确保校验票已在队列中

    t_crawl = threading.Thread(target=wait_and_record, args=(PRIORITY_CRAWL,))
    t_crawl.start()
    time.sleep(0.05)  # 采集票也已入队，此时通道仍被占用

    limiter.release()  # 按优先级派发：采集票应先获得通道
    t_crawl.join(timeout=5)
    t_validation.join(timeout=5)

    assert results == [PRIORITY_CRAWL, PRIORITY_VALIDATION]


def test_aging_makes_old_validation_ticket_outrank_new_crawl_ticket() -> None:
    from crawl_flow.autogen.limiter import _Ticket

    now = time.monotonic()
    old_validation = _Ticket(  # noqa: SLF001 - 测试内部构造
        priority=PRIORITY_VALIDATION, seq=1, queued_at=now - 50.0, event=threading.Event()
    )
    new_crawl = _Ticket(  # noqa: SLF001
        priority=PRIORITY_CRAWL, seq=2, queued_at=now, event=threading.Event()
    )
    # 老化 50s（5 档）后校验票有效优先级 10-5=5，仍高于新采集票 0，采集优先
    assert new_crawl.effective_priority(now) < old_validation.effective_priority(now)

    very_old = _Ticket(  # noqa: SLF001
        priority=PRIORITY_VALIDATION, seq=3, queued_at=now - 120.0, event=threading.Event()
    )
    # 老化 120s（12 档）后反超，避免无限饿死
    assert very_old.effective_priority(now) < new_crawl.effective_priority(now)


def test_ticket_priority_dataclass_accessible() -> None:
    # 回归：_Ticket 必须是模块级可构造的数据类
    from crawl_flow.autogen.limiter import _Ticket

    ticket = _Ticket(
        priority=PRIORITY_CRAWL, seq=0, queued_at=0.0, event=threading.Event()
    )
    assert ticket.priority == PRIORITY_CRAWL


def test_negative_interval_rejected() -> None:
    import pytest

    with pytest.raises(ValueError):
        RateLimiter(min_interval_seconds=-1)
