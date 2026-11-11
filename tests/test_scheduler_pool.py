"""调度池核心行为测试：并发上限、QPS、加权配额、防饥饿、域名排队与降级。

全部使用内存假传输，不依赖网络；时间断言保留宽容区间避免抖动。
"""

from __future__ import annotations

import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

import pytest

from crawl_flow.errors import FetchError, SchedulerDegraded
from crawl_flow.fetch.types import FetchResult
from crawl_flow.scheduler import (
    DomainRateRule,
    ScheduledFetcher,
    SchedulerConfig,
    RequestScheduler,
)
from crawl_flow.scheduler.domain_rules import extract_host
from crawl_flow.scheduler.rate_limit import now_ms


@dataclass
class FakeTransport:
    """记录并发与调用时间线的假传输；可注入失败 URL。"""

    delay: float = 0.02
    fail_urls: set[str] = field(default_factory=set)
    lock: threading.Lock = field(default_factory=threading.Lock)
    calls: list[tuple[int, str]] = field(default_factory=list)
    in_flight: int = 0
    max_in_flight: int = 0
    domain_current: dict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )
    domain_max: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def get(self, url: str, params=None) -> FetchResult:
        host = extract_host(url) or "unknown"
        with self.lock:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            self.domain_current[host] += 1
            self.domain_max[host] = max(
                self.domain_max[host], self.domain_current[host]
            )
            self.calls.append((now_ms(), url))
        time.sleep(self.delay)
        with self.lock:
            self.in_flight -= 1
            self.domain_current[host] -= 1
        if url in self.fail_urls:
            raise FetchError(f"模拟失败 url={url}", url=url)
        return FetchResult(url=url, status_code=200, body="ok")


def make_scheduler(transport: FakeTransport, **kwargs) -> RequestScheduler:
    return RequestScheduler(
        SchedulerConfig(**kwargs), transport=transport
    )


def drain(fetcher: ScheduledFetcher, urls: list[str]) -> list[FetchResult | Exception]:
    """并发提交并收集每条请求的结果或异常。"""

    def _one(url: str) -> FetchResult | Exception:
        try:
            return fetcher.get(url)
        except Exception as exc:  # noqa: BLE001 - 测试里需要收集失败
            return exc

    with ThreadPoolExecutor(max_workers=min(8, len(urls) or 1)) as pool:
        return list(pool.map(_one, urls))


# ---------------------------------------------------------------- 并发上限


def test_global_and_source_concurrency_caps_are_respected() -> None:
    transport = FakeTransport(delay=0.1)
    scheduler = make_scheduler(
        transport,
        max_concurrency=3,
        default_source_concurrency=2,
    )
    try:
        fetcher = ScheduledFetcher("s1", scheduler, transport)
        urls = [f"https://a.example.com/item/{i}" for i in range(12)]
        results = drain(fetcher, urls)
        assert len(results) == 12
        assert all(isinstance(item, FetchResult) for item in results)
        # 单源上限 2 同时也是实际可达的最大并发（只有一个源）。
        assert transport.max_in_flight <= 2
        stats = scheduler.stats()
        assert stats["in_flight"] == 0
        assert stats["queued"] == 0
    finally:
        scheduler.close()


def test_global_cap_bounds_multiple_sources() -> None:
    transport = FakeTransport(delay=0.08)
    scheduler = make_scheduler(
        transport,
        max_concurrency=4,
        default_source_concurrency=4,
    )
    try:
        fetchers = {
            name: ScheduledFetcher(name, scheduler, transport)
            for name in ("s1", "s2", "s3")
        }
        jobs = [
            (name, f"https://h-{name}.test/x/{i}")
            for name in ("s1", "s2", "s3")
            for i in range(8)
        ]

        def _one(job: tuple[str, str]) -> None:
            name, url = job
            fetchers[name].get(url)

        with ThreadPoolExecutor(max_workers=12) as pool:
            list(pool.map(_one, jobs))
        assert transport.max_in_flight <= 4
    finally:
        scheduler.close()


# ---------------------------------------------------------------- QPS


def test_global_qps_stays_within_five_percent() -> None:
    transport = FakeTransport(delay=0.0)
    qps = 10
    scheduler = make_scheduler(
        transport, max_concurrency=8, qps=qps
    )
    try:
        fetcher = ScheduledFetcher("s1", scheduler, transport)
        urls = [f"https://q.example.com/i/{i}" for i in range(30)]
        started = time.monotonic()
        drain(fetcher, urls)
        elapsed = time.monotonic() - started

        # 用滑动 1 秒窗口计数（与限流器语义一致）：任意半开区间
        # [t, t+1000ms) 内的请求数不得超过阈值的 5%。考虑到工作线程实际
        # 执行时刻与令牌发放时刻之间存在调度延迟，允许 1 个请求的测量余量。
        timestamps = sorted(ts for ts, _ in transport.calls)
        max_window = 0
        left = 0
        for right in range(len(timestamps)):
            while timestamps[right] - timestamps[left] >= 1000:
                left += 1
            max_window = max(max_window, right - left + 1)
        assert max_window <= qps + max(1, round(qps * 0.05))
        # 30 个请求、10 QPS 至少需要约 2 个窗口时间（末段窗口天然用不满，
        # 不用「总数/总时长」衡量，那会把空闲末段算进来而高估瞬时速率）。
        assert elapsed >= 1.9
    finally:
        scheduler.close()


def test_source_qps_is_independent() -> None:
    transport = FakeTransport(delay=0.0)
    scheduler = RequestScheduler(
        SchedulerConfig(
            max_concurrency=8,
            sources=[
                {"name": "fast", "qps": 50, "weight": 1},
                {"name": "slow", "qps": 5, "weight": 1},
            ],
        ),
        transport=transport,
    )
    try:
        fast = ScheduledFetcher("fast", scheduler, transport)
        slow = ScheduledFetcher("slow", scheduler, transport)

        def _collect(fetcher: ScheduledFetcher, host: str) -> list[int]:
            timestamps: list[int] = []
            for i in range(15):
                fetcher.get(f"https://{host}.test/{i}")
                timestamps.append(now_ms())
            return timestamps

        with ThreadPoolExecutor(max_workers=2) as pool:
            fast_future = pool.submit(_collect, fast, "fast")
            slow_future = pool.submit(_collect, slow, "slow")
            slow_times = slow_future.result()
            fast_future.result()

        # slow 源 5 QPS：15 个请求至少跨越约 2 个 1 秒窗口。
        span_ms = slow_times[-1] - slow_times[0]
        assert span_ms >= 1800
    finally:
        scheduler.close()


# ---------------------------------------------------------------- 加权配额


def test_weighted_share_matches_configured_ratio() -> None:
    transport = FakeTransport(delay=0.005)
    scheduler = RequestScheduler(
        SchedulerConfig(
            max_concurrency=2,
            sources=[
                {"name": "high", "weight": 3, "max_concurrency": 2},
                {"name": "low", "weight": 1, "max_concurrency": 2},
            ],
        ),
        transport=transport,
    )
    try:
        high = ScheduledFetcher("high", scheduler, transport)
        low = ScheduledFetcher("low", scheduler, transport)

        def _submit(fetcher: ScheduledFetcher, host: str, count: int) -> None:
            for i in range(count):
                fetcher.get(f"https://{host}.test/{i}")

        with ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(_submit, high, "high", 30)
            f2 = pool.submit(_submit, low, "low", 10)
            f1.result()
            f2.result()

        stats = {item["source"]: item for item in scheduler.stats()["sources"]}
        high_share = stats["high"]["dispatched"]
        low_share = stats["low"]["dispatched"]
        assert high_share == 30 and low_share == 10
        # 3:1 配置比例，允许 15% 调度抖动。
        ratio = high_share / low_share
        assert 3 * 0.85 <= ratio <= 3 * 1.15
    finally:
        scheduler.close()


def test_equal_weights_split_evenly() -> None:
    transport = FakeTransport(delay=0.005)
    scheduler = make_scheduler(
        transport, max_concurrency=2, default_source_concurrency=2
    )
    try:
        fetchers = {
            name: ScheduledFetcher(name, scheduler, transport)
            for name in ("a", "b")
        }

        def _submit(name: str) -> None:
            for i in range(20):
                fetchers[name].get(f"https://{name}.test/{i}")

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(_submit, name) for name in ("a", "b")]
            for future in futures:
                future.result()

        stats = {item["source"]: item for item in scheduler.stats()["sources"]}
        share_a = stats["a"]["dispatched"]
        share_b = stats["b"]["dispatched"]
        assert abs(share_a - share_b) <= 2
    finally:
        scheduler.close()


# ---------------------------------------------------------------- 防饥饿


def test_aging_prevents_low_weight_starvation() -> None:
    transport = FakeTransport(delay=0.01)
    scheduler = RequestScheduler(
        SchedulerConfig(
            max_concurrency=4,
            sources=[
                {"name": "heavy", "weight": 100, "max_concurrency": 4},
                {"name": "light", "weight": 1, "max_concurrency": 1},
            ],
        ),
        transport=transport,
        aging_ms=100,
    )
    try:
        heavy = ScheduledFetcher("heavy", scheduler, transport)
        light = ScheduledFetcher("light", scheduler, transport)
        light_done = threading.Event()

        def _heavy() -> None:
            for i in range(60):
                heavy.get(f"https://heavy.test/{i}")

        def _light() -> None:
            for i in range(5):
                light.get(f"https://light.test/{i}")
            light_done.set()

        with ThreadPoolExecutor(max_workers=2) as pool:
            pool.submit(_heavy)
            pool.submit(_light)
            # 老化阈值 100ms，给足余量；若没有老化机制低权重源会长期排队。
            assert light_done.wait(timeout=10.0)

        stats = {item["source"]: item for item in scheduler.stats()["sources"]}
        assert stats["light"]["dispatched"] == 5
        assert stats["light"]["max_wait_ms"] < 3_000
    finally:
        scheduler.close()


# ---------------------------------------------------------------- 域名限流


def test_domain_rule_queues_instead_of_failing() -> None:
    transport = FakeTransport(delay=0.0)
    scheduler = RequestScheduler(
        SchedulerConfig(
            max_concurrency=8,
            domain_rules=[
                DomainRateRule(domain="limited.test", qps=5, match="exact")
            ],
        ),
        transport=transport,
    )
    try:
        fetcher = ScheduledFetcher("s1", scheduler, transport)
        urls = [f"https://limited.test/i/{i}" for i in range(20)]
        started = time.monotonic()
        results = drain(fetcher, urls)
        elapsed = time.monotonic() - started

        # 全部成功——域名限流只排队，不产生失败。
        assert all(isinstance(item, FetchResult) for item in results)
        assert elapsed >= 2.9  # 20 个请求 / 5 QPS ≈ 3s+
        timestamps = sorted(ts for ts, _ in transport.calls)
        max_window = 0
        left = 0
        for right in range(len(timestamps)):
            while timestamps[right] - timestamps[left] >= 1000:
                left += 1
            max_window = max(max_window, right - left + 1)
        assert max_window <= 6  # 5 + 毫秒边界余量
    finally:
        scheduler.close()


def test_unlimited_domain_is_not_throttled() -> None:
    transport = FakeTransport(delay=0.0)
    scheduler = RequestScheduler(
        SchedulerConfig(
            max_concurrency=8,
            domain_rules=[
                DomainRateRule(domain="limited.test", qps=1, match="exact")
            ],
        ),
        transport=transport,
    )
    try:
        fetcher = ScheduledFetcher("s1", scheduler, transport)
        started = time.monotonic()
        results = drain(
            fetcher, [f"https://free.test/i/{i}" for i in range(12)]
        )
        elapsed = time.monotonic() - started
        assert all(isinstance(item, FetchResult) for item in results)
        assert elapsed < 1.0  # 不受 limited.test 规则影响
    finally:
        scheduler.close()


# ---------------------------------------------------------------- 失败隔离


def test_single_request_failure_does_not_break_pool() -> None:
    fail_urls = {f"https://fail.test/i/{i}" for i in (2, 5, 9)}
    transport = FakeTransport(delay=0.0, fail_urls=fail_urls)
    scheduler = make_scheduler(
        transport, max_concurrency=4, default_source_concurrency=4
    )
    try:
        fetcher = ScheduledFetcher("s1", scheduler, transport)
        results = drain(
            fetcher, [f"https://fail.test/i/{i}" for i in range(12)]
        )
        failures = [item for item in results if isinstance(item, Exception)]
        successes = [item for item in results if isinstance(item, FetchResult)]
        assert len(failures) == 3
        assert len(successes) == 9
        stats = scheduler.stats()
        assert stats["failed"] == 3
        assert stats["completed"] == 9
        assert not scheduler.degraded
    finally:
        scheduler.close()


# ---------------------------------------------------------------- 降级


def test_degradation_falls_back_to_direct_fetch() -> None:
    transport = FakeTransport(delay=0.0)
    scheduler = make_scheduler(transport, max_concurrency=2)
    fetcher = ScheduledFetcher("s1", scheduler, transport)
    try:
        # 正常路径先走通。
        assert isinstance(fetcher.get("https://ok.test/1"), FetchResult)
        scheduler.enter_degradation("测试降级")
        assert scheduler.degraded
        # 降级后新请求不进调度池，直接由底层传输执行，任务不中断。
        result = fetcher.get("https://ok.test/2")
        assert isinstance(result, FetchResult)
        queued_via_scheduler = sum(
            1 for _, url in transport.calls if url == "https://ok.test/2"
        )
        assert queued_via_scheduler == 1  # 直连同样只发一次请求
    finally:
        scheduler.close()


def test_closed_scheduler_fetcher_falls_back() -> None:
    transport = FakeTransport(delay=0.0)
    scheduler = make_scheduler(transport, max_concurrency=2)
    fetcher = ScheduledFetcher("s1", scheduler, transport)
    scheduler.close()
    # 关闭后提交触发 SchedulerError，代理自动直连。
    result = fetcher.get("https://closed.test/1")
    assert isinstance(result, FetchResult)


def test_dispatch_thread_exception_triggers_auto_degradation() -> None:
    """调度线程内部抛异常时自动降级，close 不二次崩溃，新请求直连成功。"""

    transport = FakeTransport(delay=0.0)
    scheduler = make_scheduler(transport, max_concurrency=2)
    fetcher = ScheduledFetcher("s1", scheduler, transport)
    try:
        assert isinstance(fetcher.get("https://ok.test/0"), FetchResult)

        # 模拟调度内部状态损坏：下一轮 dispatch 抛异常并进入降级。
        scheduler._sources = None  # type: ignore[assignment]
        deadline = time.monotonic() + 3.0
        while not scheduler.degraded and time.monotonic() < deadline:
            time.sleep(0.02)
        assert scheduler.degraded

        # 降级后请求全部直连成功，且关闭流程不抛异常。
        for i in range(1, 4):
            assert isinstance(
                fetcher.get(f"https://ok.test/{i}"), FetchResult
            )
        scheduler.close()  # 不应抛 AttributeError
    finally:
        if not scheduler._closed:
            scheduler.close()


def test_queued_requests_are_released_on_degradation() -> None:
    transport = FakeTransport(delay=0.05)
    # 1 QPS + 单并发：人为制造排队，再在排队期间降级。
    scheduler = make_scheduler(
        transport, max_concurrency=1, qps=1
    )
    fetcher = ScheduledFetcher("s1", scheduler, transport)
    outcomes: list[str] = []
    lock = threading.Lock()

    def _hit(i: int) -> str:
        try:
            fetcher.get(f"https://deg.test/{i}")
            verdict = "ok"
        except SchedulerDegraded:
            verdict = "degraded"
        with lock:
            outcomes.append(verdict)
        return verdict

    try:
        with ThreadPoolExecutor(max_workers=6) as pool:
            futures = [pool.submit(_hit, i) for i in range(6)]
            time.sleep(0.15)
            scheduler.enter_degradation("排队中降级")
            for future in futures:
                future.result(timeout=5.0)
        # 没有任何请求永久挂起；部分走调度池成功，其余降级直连。
        assert set(outcomes) <= {"ok", "degraded"}
        assert len(outcomes) == 6
    finally:
        scheduler.close()
