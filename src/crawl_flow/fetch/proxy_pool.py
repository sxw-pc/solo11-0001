"""代理池：维护每个代理的连续失败计数与冷却状态。

冷却判定基于单调时钟（:func:`time.monotonic`），测试可注入自定义时钟。
代理本身不感知 HTTP 语义，失败/成功由 :class:`~crawl_flow.fetch.client.Fetcher`
根据传输异常与可重试状态码（403、5xx）上报。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ProxyState:
    """单个代理对外可见的状态快照。"""

    url: str
    consecutive_failures: int
    cooled_down_until: float | None

    def is_available(self, now: float) -> bool:
        return self.cooled_down_until is None or now >= self.cooled_down_until


class ProxyPool:
    """按配置顺序轮转代理，故障代理在冷却期内被跳过。

    - 连续失败次数达到 ``failure_threshold`` 时代理进入冷却，冷却
      ``cooldown_seconds`` 后自动恢复；
    - 一次成功会清零连续失败计数；
    - 所有代理都在冷却中时，返回最早恢复的那个（由调用方兜底尝试），
      避免一次瞬时故障让整轮采集彻底无代理可用。
    """

    def __init__(
        self,
        urls: list[str],
        *,
        failure_threshold: int,
        cooldown_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not urls:
            raise ValueError("ProxyPool 至少需要一个代理地址")
        self._urls = list(urls)
        self._failure_threshold = failure_threshold
        self._cooldown_seconds = cooldown_seconds
        self._clock = clock
        self._lock = threading.Lock()
        self._failures: dict[str, int] = {url: 0 for url in urls}
        self._cooled_down_until: dict[str, float] = {}

    @property
    def urls(self) -> list[str]:
        return list(self._urls)

    def acquire_order(self) -> list[str]:
        """返回本次请求的代理尝试顺序。

        可用代理按配置顺序排在前面；若全部冷却中，末尾补上最早恢复的代理。
        """

        now = self._clock()
        with self._lock:
            available = [
                url
                for url in self._urls
                if self._cooled_down_until.get(url) is None
                or self._cooled_down_until[url] <= now
            ]
            if available:
                return available
            return [min(self._urls, key=lambda url: self._cooled_down_until[url])]

    def report_success(self, url: str) -> None:
        with self._lock:
            self._failures[url] = 0
            self._cooled_down_until.pop(url, None)

    def report_failure(self, url: str) -> None:
        """上报一次失败；达到阈值时代理进入冷却并重置连续计数。"""

        now = self._clock()
        with self._lock:
            if url not in self._failures:
                return
            self._failures[url] += 1
            if self._failures[url] >= self._failure_threshold:
                self._cooled_down_until[url] = now + self._cooldown_seconds
                self._failures[url] = 0

    def state(self, url: str) -> ProxyState:
        with self._lock:
            return ProxyState(
                url=url,
                consecutive_failures=self._failures[url],
                cooled_down_until=self._cooled_down_until.get(url),
            )

    def states(self) -> list[ProxyState]:
        return [self.state(url) for url in self._urls]
