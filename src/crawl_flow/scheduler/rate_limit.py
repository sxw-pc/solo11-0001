"""毫秒精度滑动窗口限流器。

所有时间戳取 :func:`time.monotonic_ns`（单调时钟，不受系统时间回拨影响），
内部换算成毫秒整数。限流计数精确到毫秒：以「当前时刻 - 窗口长度」为下界，
清理窗口外的请求时间戳后再判断是否放行。

线程安全：所有状态变更都在实例锁内完成，条件变量用于在容量恢复时唤醒等待者。
``acquire``/``try_acquire`` 返回一个令牌（计入窗口的毫秒时间戳），
``release(token)`` 精确移除该令牌，跨线程使用也不会误删其他请求的配额。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass

NS_PER_MS = 1_000_000
# 已滑出窗口、release 时找不到令牌的占位返回。
TOKEN_EXPIRED = -1


def now_ms() -> int:
    """单调时钟的当前毫秒时间戳。"""

    return time.monotonic_ns() // NS_PER_MS


@dataclass(frozen=True)
class RateLimitConfig:
    """速率限制配置。

    ``rate`` 为每个滚动窗口允许的请求数，``window_ms`` 为窗口长度（毫秒）。
    例如 ``rate=10, window_ms=1000`` 表示任意 1 秒窗口内最多 10 个请求。
    """

    rate: int
    window_ms: int

    def __post_init__(self) -> None:
        if self.rate <= 0:
            raise ValueError("rate 必须为正整数")
        if self.window_ms <= 0:
            raise ValueError("window_ms 必须为正整数")

    @property
    def per_second(self) -> float:
        """折算的每秒请求数（QPS）。"""

        return self.rate * 1000.0 / self.window_ms


class SlidingWindowRateLimiter:
    """滑动窗口限流器（线程安全）。

    - :meth:`try_acquire` 非阻塞尝试一次配额，返回令牌或 ``None``；
    - :meth:`acquire` 阻塞直到拿到配额，支持被关闭时快速失败；
    - 窗口以毫秒为单位滚动，时间戳保留在 :class:`collections.deque` 中，
      过期时间戳在每次判断时惰性清理。
    """

    def __init__(self, config: RateLimitConfig, *, name: str = "rate-limiter") -> None:
        self._config = config
        self._name = name
        self._timestamps: deque[int] = deque()
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._closed = False

    @property
    def config(self) -> RateLimitConfig:
        return self._config

    @property
    def name(self) -> str:
        return self._name

    def _evict_locked(self, current: int) -> None:
        boundary = current - self._config.window_ms
        timestamps = self._timestamps
        while timestamps and timestamps[0] <= boundary:
            timestamps.popleft()

    def try_acquire(self) -> int | None:
        """尝试获取一个配额；成功返回令牌，窗口已满或已关闭返回 ``None``。"""

        current = now_ms()
        with self._cond:
            if self._closed:
                return None
            self._evict_locked(current)
            if len(self._timestamps) >= self._config.rate:
                return None
            self._timestamps.append(current)
            return current

    def acquire(self, *, timeout: float | None = None) -> int | None:
        """阻塞获取配额，成功返回令牌；超时或限流器关闭返回 ``None``。

        等待基于条件变量，配额恢复（窗口滑出旧请求）或限流器关闭时被唤醒。
        """

        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while True:
                if self._closed:
                    return None
                current = now_ms()
                self._evict_locked(current)
                if len(self._timestamps) < self._config.rate:
                    self._timestamps.append(current)
                    return current
                wait_seconds = self._wait_seconds_locked(current)
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return None
                    wait_seconds = min(wait_seconds, remaining)
                self._cond.wait(timeout=wait_seconds)

    def _wait_seconds_locked(self, current: int) -> float:
        # 等到最早一个时间戳滑出窗口，再加 1ms 保证边界严格。
        oldest = self._timestamps[0]
        return max(0.0, (oldest + self._config.window_ms - current) / 1000.0) + 0.001

    def release(self, token: int) -> None:
        """精确归还一个令牌；令牌已滑出窗口时为空操作。"""

        if token is None or token == TOKEN_EXPIRED:
            return
        with self._cond:
            try:
                self._timestamps.remove(token)
            except ValueError:
                # 令牌已随窗口滚动被清理，无需再释放。
                pass
            else:
                self._cond.notify()

    def snapshot(self) -> dict[str, int]:
        """当前窗口内的计数快照（用于观测与测试断言）。"""

        current = now_ms()
        with self._cond:
            self._evict_locked(current)
            return {
                "in_window": len(self._timestamps),
                "limit": self._config.rate,
                "window_ms": self._config.window_ms,
            }

    def close(self) -> None:
        """关闭限流器，唤醒全部等待者（等待中的 acquire 立即返回 None）。"""

        with self._cond:
            self._closed = True
            self._cond.notify_all()

    @property
    def closed(self) -> bool:
        with self._cond:
            return self._closed
