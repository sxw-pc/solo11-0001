"""进程内共享限速器：所有自动爬取（生成 + 校验）共用同一份访问预算。

设计要点：

- 默认最小请求间隔 2 秒（每 2 秒至多 1 次请求），间隔可按场景下调（测试用 0）。
- 优先级队列：正常采集 ``PRIORITY_CRAWL`` 排在适配校验
  ``PRIORITY_VALIDATION`` 前面，校验任务**不能抢占**正常业务任务的资源；
  同时对排队中的票做「队龄老化」，避免校验在持续采集流量下被无限饿死。
- 线程安全；限速器不发请求，只负责发放「放行令牌」，便于单元测试。
"""

from __future__ import annotations

import itertools
import threading
import time
from dataclasses import dataclass, field

from crawl_flow.autogen.constants import (
    DEFAULT_MIN_INTERVAL_SECONDS,
    PRIORITY_CRAWL,
    PRIORITY_VALIDATION,
)

# 每排队 10 秒，有效优先级提升 1 个档位（采集=0，校验=10），
# 因此校验票在持续采集流量下最多等待约 100 秒即可反超，避免无限饿死。
_AGING_STEP_SECONDS = 10.0


@dataclass
class _Ticket:
    priority: int
    seq: int
    queued_at: float
    event: threading.Event = field(compare=False)

    def effective_priority(self, now: float) -> float:
        age_bonus = (now - self.queued_at) // _AGING_STEP_SECONDS
        return self.priority - age_bonus


class RateLimiter:
    """最小间隔 + 优先级排队（含老化）的全局限速器。"""

    def __init__(self, *, min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds 不能为负")
        self.min_interval = float(min_interval_seconds)
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._tickets: dict[int, _Ticket] = {}
        self._counter = itertools.count()
        self._last_release: float | None = None
        self._serving = False
        self._timer: threading.Timer | None = None
        # 观测用：累计放行请求数。
        self.released = 0

    def acquire(self, *, priority: int = PRIORITY_CRAWL) -> None:
        """排队等待放行。数字越小优先级越高（采集优先于校验）。"""

        seq = next(self._counter)
        ticket = _Ticket(
            priority=priority,
            seq=seq,
            queued_at=time.monotonic(),
            event=threading.Event(),
        )
        with self._cond:
            self._tickets[seq] = ticket
            # 有请求在途时只入队，等 release 统一按优先级派发，
            # 避免先到的低优先级请求在竞争中「插队」。
            if not self._serving:
                self._pump_locked()
            self._cond.wait_for(lambda: ticket.event.is_set())

    def _pick_next_locked(self) -> _Ticket | None:
        if not self._tickets:
            return None
        now = time.monotonic()
        return min(
            self._tickets.values(),
            key=lambda t: (t.effective_priority(now), t.seq),
        )

    def _pump_locked(self) -> None:
        """条件满足时唤醒队首；任何时刻最多一个请求处于被放行状态。"""

        if self._serving or not self._tickets:
            return
        now = time.monotonic()
        if self._last_release is not None and (now - self._last_release) < self.min_interval:
            if self._timer is None:
                delay = self.min_interval - (now - self._last_release)
                timer = threading.Timer(delay, self._on_tick)
                timer.daemon = True
                self._timer = timer
                timer.start()
            return
        if self._timer is not None:
            self._timer = None
        ticket = self._pick_next_locked()
        self._tickets.pop(ticket.seq, None)
        self._serving = True
        self._last_release = now
        self.released += 1
        ticket.event.set()
        self._cond.notify_all()

    def _on_tick(self) -> None:
        with self._cond:
            self._timer = None
            self._pump_locked()

    def release(self) -> None:
        """请求完成后归还通道，触发下一个排队者（按优先级 + 老化）。"""

        with self._cond:
            if not self._serving:
                return
            self._serving = False
            self._pump_locked()

    def stats(self) -> dict[str, float | int]:
        with self._lock:
            return {
                "released": self.released,
                "queued": len(self._tickets),
                "min_interval_seconds": self.min_interval,
            }


# 模块级单例：同进程内生成器与所有校验任务共享。
_GLOBAL_LIMITER: RateLimiter | None = None
_GLOBAL_LOCK = threading.Lock()


def get_global_limiter() -> RateLimiter:
    global _GLOBAL_LIMITER
    if _GLOBAL_LIMITER is None:
        with _GLOBAL_LOCK:
            if _GLOBAL_LIMITER is None:
                _GLOBAL_LIMITER = RateLimiter()
    return _GLOBAL_LIMITER


def reset_global_limiter(min_interval_seconds: float = DEFAULT_MIN_INTERVAL_SECONDS) -> RateLimiter:
    """测试 / 显式配置时重置全局限速器。"""

    global _GLOBAL_LIMITER
    with _GLOBAL_LOCK:
        _GLOBAL_LIMITER = RateLimiter(min_interval_seconds=min_interval_seconds)
        return _GLOBAL_LIMITER


__all__ = [
    "RateLimiter",
    "get_global_limiter",
    "reset_global_limiter",
    "PRIORITY_CRAWL",
    "PRIORITY_VALIDATION",
]
