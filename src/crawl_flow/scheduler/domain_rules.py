"""域名级限流规则注册表与匹配器。

规则来自 :class:`~crawl_flow.scheduler.config.DomainRateRule`，每个生效域名
持有一个独立的毫秒级滑动窗口限流器。请求发出前必须通过对应域名的限流器；
窗口已满时调用方阻塞等待，**不返回失败**。

匹配规则：
- ``exact``：host 完全相等（忽略大小写）；
- ``suffix``：host 等于规则域名或是其子域（``.`` 边界）；
- 多条规则同时命中时，最长（最具体）的域名胜出。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from urllib.parse import urlsplit

from crawl_flow.scheduler.config import DomainRateRule
from crawl_flow.scheduler.rate_limit import (
    RateLimitConfig,
    SlidingWindowRateLimiter,
)


def extract_host(url: str) -> str:
    """从 URL 中取出小写 host；无法解析时返回空串。"""

    try:
        host = urlsplit(url).hostname
    except ValueError:
        return ""
    return host.lower() if host else ""


@dataclass(frozen=True)
class _MatchedRule:
    domain: str
    limiter: SlidingWindowRateLimiter


class DomainRateRegistry:
    """域名 → 限流器的注册表（线程安全，注册后规则不可变）。"""

    def __init__(self, rules: list[DomainRateRule] | None = None) -> None:
        self._lock = threading.Lock()
        # 按域名长度倒序，匹配时第一个命中的就是最具体规则。
        self._exact: dict[str, _MatchedRule] = {}
        self._suffix: list[tuple[str, _MatchedRule]] = []
        for rule in rules or []:
            self._register(rule)

    def _register(self, rule: DomainRateRule) -> None:
        # QPS 折算到 1000ms 窗口；不足 1 QPS 时放大窗口保证至少 1 个配额。
        rate, window_ms = _qps_to_window(rule.qps)
        limiter = SlidingWindowRateLimiter(
            RateLimitConfig(rate=rate, window_ms=window_ms),
            name=f"domain:{rule.domain}",
        )
        entry = _MatchedRule(domain=rule.domain, limiter=limiter)
        if rule.match == "exact":
            self._exact[rule.domain] = entry
        else:
            self._suffix.append((rule.domain, entry))
            self._suffix.sort(key=lambda item: len(item[0]), reverse=True)

    def match(self, url: str) -> _MatchedRule | None:
        """返回 URL 命中的最具体规则；未命中返回 ``None``。"""

        host = extract_host(url)
        if not host:
            return None
        with self._lock:
            exact = self._exact.get(host)
            if exact is not None:
                return exact
            for domain, entry in self._suffix:
                if host == domain or host.endswith(f".{domain}"):
                    return entry
        return None

    def acquire(self, url: str, *, timeout: float | None = None) -> int | None:
        """针对 URL 对应域名获取一个配额，返回限流令牌。

        没有命中规则时直接放行并返回 ``None``（无需释放）；命中且窗口已满时
        阻塞等待；限流器关闭或超时返回 ``None``（由调用方决定降级，不直接
        抛失败）。
        """

        matched = self.match(url)
        if matched is None:
            return None
        return matched.limiter.acquire(timeout=timeout)

    def try_acquire(self, url: str) -> int | None:
        """非阻塞版本：未命中规则返回 ``None``（放行），命中返回令牌或 ``None``。"""

        matched = self.match(url)
        if matched is None:
            return None
        return matched.limiter.try_acquire()

    def try_acquire_token(self, url: str) -> tuple[bool, int | None]:
        """非阻塞申请，返回 ``(是否命中规则, 令牌)``。

        与 :meth:`try_acquire` 的区别：调用方需要区分「没有域名规则」
        （无需回滚）与「命中规则但窗口已满」（必须回滚已申请的上游令牌）。
        """

        matched = self.match(url)
        if matched is None:
            return False, None
        return True, matched.limiter.try_acquire()

    def release(self, url: str, token: int) -> None:
        """归还 URL 对应域名的配额（请求未实际发出时使用）。"""

        matched = self.match(url)
        if matched is not None:
            matched.limiter.release(token)

    def rules_snapshot(self) -> list[dict[str, object]]:
        """导出当前规则与窗口计数，供观测与测试使用。"""

        with self._lock:
            entries = list(self._exact.values()) + [
                entry for _, entry in self._suffix
            ]
        return [
            {
                "domain": entry.domain,
                "limiter": entry.limiter.name,
                **entry.limiter.snapshot(),
            }
            for entry in entries
        ]

    def close(self) -> None:
        with self._lock:
            entries = list(self._exact.values()) + [
                entry for _, entry in self._suffix
            ]
        for entry in entries:
            entry.limiter.close()


def _qps_to_window(qps: float) -> tuple[int, int]:
    """把 QPS 折算成 (窗口内配额数, 窗口毫秒)。

    qps >= 1：固定 1000ms 窗口，配额四舍五入取整（至少 1）；
    qps < 1：配额固定 1，窗口放大到能容纳该速率的毫秒数。
    """

    if qps >= 1:
        return max(1, round(qps)), 1000
    return 1, max(1, round(1000 / qps))
