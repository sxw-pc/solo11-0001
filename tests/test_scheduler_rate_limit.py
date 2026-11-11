"""毫秒滑动窗口限流器与域名规则注册表的单元测试。"""

from __future__ import annotations

import threading
import time

import pytest

from crawl_flow.scheduler.config import DomainRateRule
from crawl_flow.scheduler.domain_rules import DomainRateRegistry, extract_host
from crawl_flow.scheduler.rate_limit import (
    RateLimitConfig,
    SlidingWindowRateLimiter,
    now_ms,
)


def test_rate_config_rejects_invalid_values() -> None:
    with pytest.raises(ValueError):
        RateLimitConfig(rate=0, window_ms=1000)
    with pytest.raises(ValueError):
        RateLimitConfig(rate=1, window_ms=0)


def test_sliding_window_blocks_until_window_slides() -> None:
    limiter = SlidingWindowRateLimiter(
        RateLimitConfig(rate=5, window_ms=300), name="t"
    )
    tokens = [limiter.try_acquire() for _ in range(5)]
    assert all(token is not None for token in tokens)
    # 窗口已满：第 6 个请求立即被拒绝。
    assert limiter.try_acquire() is None

    # 窗口滑出后自动恢复，不需要任何手动释放。
    time.sleep(0.32)
    assert limiter.try_acquire() is not None


def test_acquire_blocks_and_recovers() -> None:
    limiter = SlidingWindowRateLimiter(
        RateLimitConfig(rate=2, window_ms=200), name="t"
    )
    assert limiter.try_acquire() is not None
    assert limiter.try_acquire() is not None

    started = time.monotonic()
    token = limiter.acquire(timeout=2.0)
    elapsed = time.monotonic() - started
    assert token is not None
    # 至少等到窗口滚动（约 200ms），且不能超过一个窗口太多。
    assert 0.18 <= elapsed < 0.6


def test_acquire_timeout_returns_none() -> None:
    limiter = SlidingWindowRateLimiter(
        RateLimitConfig(rate=1, window_ms=1000), name="t"
    )
    assert limiter.try_acquire() is not None
    assert limiter.acquire(timeout=0.05) is None


def test_release_removes_exact_token_under_contention() -> None:
    """跨线程释放必须只移除自己的令牌，不能误删其他请求的配额。"""

    limiter = SlidingWindowRateLimiter(
        RateLimitConfig(rate=3, window_ms=5000), name="t"
    )
    first = limiter.try_acquire()
    second = limiter.try_acquire()
    third = limiter.try_acquire()
    assert limiter.try_acquire() is None

    limiter.release(second)  # 释放中间令牌
    assert limiter.snapshot()["in_window"] == 2
    replacement = limiter.try_acquire()
    assert replacement is not None
    assert limiter.try_acquire() is None  # 仍然严格 3 个

    limiter.release(first)
    limiter.release(third)
    limiter.release(replacement)
    assert limiter.snapshot()["in_window"] == 0
    assert limiter.try_acquire() is not None


def test_release_expired_token_is_noop() -> None:
    limiter = SlidingWindowRateLimiter(
        RateLimitConfig(rate=1, window_ms=50), name="t"
    )
    token = limiter.try_acquire()
    time.sleep(0.07)
    limiter.release(token)  # 令牌已随窗口过期，释放不应抛错
    assert limiter.try_acquire() is not None


def test_close_wakes_waiting_acquirers() -> None:
    limiter = SlidingWindowRateLimiter(
        RateLimitConfig(rate=1, window_ms=5000), name="t"
    )
    assert limiter.try_acquire() is not None
    result: int | None = -1

    def _wait() -> None:
        nonlocal result
        result = limiter.acquire(timeout=5.0)

    thread = threading.Thread(target=_wait)
    thread.start()
    time.sleep(0.05)
    limiter.close()
    thread.join(timeout=2.0)
    assert result is None
    assert limiter.closed


def test_now_ms_is_monotonic_millisecond_precision() -> None:
    first = now_ms()
    time.sleep(0.02)
    second = now_ms()
    assert second - first >= 15  # 至少 15ms，证明计数精度到毫秒


def test_rate_limiter_qps_within_five_percent_strict() -> None:
    """同源时钟精确验证：10 QPS 下任意滑动 1 秒窗口不超过阈值的 5%。"""

    qps = 10
    limiter = SlidingWindowRateLimiter(
        RateLimitConfig(rate=qps, window_ms=1000), name="strict"
    )
    granted: list[int] = []
    deadline = time.monotonic() + 3.0
    while time.monotonic() < deadline and len(granted) < 30:
        token = limiter.acquire(timeout=2.0)
        assert token is not None
        granted.append(token)

    # 令牌时间戳与窗口判断使用同一个单调毫秒时钟，无跨线程测量噪声。
    left = 0
    max_window = 0
    for right in range(len(granted)):
        while granted[right] - granted[left] >= 1000:
            left += 1
        max_window = max(max_window, right - left + 1)
    assert max_window <= round(qps * 1.05)


def test_extract_host() -> None:
    assert extract_host("https://API.Example.com:8443/path?x=1") == "api.example.com"
    assert extract_host("http://127.0.0.1:8010/x") == "127.0.0.1"
    assert extract_host("not-a-url") == ""


def test_domain_registry_exact_and_suffix_matching() -> None:
    registry = DomainRateRegistry(
        [
            DomainRateRule(domain="example.com", qps=10, match="suffix"),
            DomainRateRule(domain="api.example.com", qps=2, match="exact"),
            DomainRateRule(domain="slow.example.org", qps=1, match="exact"),
        ]
    )
    # exact 优先于 suffix。
    assert registry.match("https://api.example.com/x").domain == "api.example.com"
    # 子域命中 suffix 规则。
    assert registry.match("https://www.example.com/x").domain == "example.com"
    assert registry.match("https://example.com/x").domain == "example.com"
    # 不能越过点边界误匹配。
    assert registry.match("https://notexample.com/x") is None
    assert registry.match("https://other.org/x") is None

    snapshot = {item["domain"]: item for item in registry.rules_snapshot()}
    assert set(snapshot) == {"example.com", "api.example.com", "slow.example.org"}


def test_domain_registry_longest_suffix_wins() -> None:
    registry = DomainRateRegistry(
        [
            DomainRateRule(domain="example.com", qps=10, match="suffix"),
            DomainRateRule(domain="cdn.example.com", qps=3, match="suffix"),
        ]
    )
    matched = registry.match("https://img.cdn.example.com/a")
    assert matched is not None and matched.domain == "cdn.example.com"


def test_domain_registry_sub_qps_uses_widened_window() -> None:
    # 0.5 QPS → 每 2000ms 一个请求。
    registry = DomainRateRegistry(
        [DomainRateRule(domain="slow.test", qps=0.5, match="exact")]
    )
    url = "https://slow.test/a"
    first_matched, first_token = registry.try_acquire_token(url)
    assert first_matched and first_token is not None
    matched, second = registry.try_acquire_token(url)
    assert matched and second is None  # 第二个请求被排队而不是放行


def test_domain_registry_unmatched_url_passes() -> None:
    registry = DomainRateRegistry(
        [DomainRateRule(domain="limited.test", qps=1, match="exact")]
    )
    matched, token = registry.try_acquire_token("https://free.example.net/")
    assert matched is False and token is None
