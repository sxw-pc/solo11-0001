"""请求调度池：加权 DRR 调度、两级并发控制、多级速率限制与自动降级。

设计要点（单进程、无线程安全外部依赖）：

- **单调度线程 + 工作线程池**：一个 dispatcher 线程负责挑选就绪请求并发放
  全部限流令牌，``ThreadPoolExecutor`` 中的工作线程只执行实际请求；调度状态
  全部在同一把条件锁内变更，不存在竞态计数。
- **加权 DRR（亏空轮询）**：每个源维护 ``deficit``，调度轮询中按权重补充
  配额，长期看各源分到的请求数与 ``weight`` 成正比。
- **老化防饥饿**：队首请求等待超过阈值（默认 1s）的源被提前到队首并保证
  至少获得一次调度机会，低权重源不会长期拿不到配额。
- **两级并发**：全局 ``max_concurrency`` 与单源 ``max_concurrency`` 同时
  约束，实际在途请求数不可能超过任一配置值。
- **多级限流**：全局 QPS、单源 QPS、域名 QPS 均为毫秒滑动窗口；dispatcher
  非阻塞申请令牌，任一窗口已满则该请求留在队列中等待，**不返回失败**。
- **失败隔离**：单条请求的异常原样回传给提交者，调度池只计数不中断。
- **自动降级**：调度线程发生未预期异常时进入降级态，排队请求被标记为
  ``SchedulerDegraded``，由 :class:`ScheduledFetcher` 改直连串行执行，
  已经在途的请求不受影响。
"""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Protocol

from crawl_flow.errors import SchedulerDegraded, SchedulerError
from crawl_flow.scheduler.config import SchedulerConfig
from crawl_flow.scheduler.domain_rules import DomainRateRegistry
from crawl_flow.scheduler.rate_limit import (
    RateLimitConfig,
    SlidingWindowRateLimiter,
    now_ms,
)

LOGGER_NAME = "crawl_flow.scheduler"
# 队首等待超过该毫秒数即触发挥老化优先调度。
DEFAULT_AGING_MS = 1_000
# 空闲/等待令牌时调度线程的轮询节拍（秒）。
DISPATCH_TICK_SECONDS = 0.02
# 关闭时等待在途请求的最长时间。
DEFAULT_CLOSE_TIMEOUT_SECONDS = 30.0


class Transport(Protocol):
    """调度池执行请求所依赖的最小传输接口（Fetcher 天然满足）。"""

    def get(self, url: str, params: dict[str, object] | None = None) -> Any:
        """发起 GET，失败时抛异常，由提交者按原有规则处理。"""


@dataclass
class PendingRequest:
    """排队中的一次请求及其回传通道。"""

    request_id: int
    source: str
    url: str
    params: dict[str, object] | None
    enqueued_ms: int
    event: threading.Event = field(default_factory=threading.Event)
    result: Any = None
    error: BaseException | None = None
    degraded: bool = False


@dataclass
class _Tokens:
    """一次调度发放的全部限流令牌；请求未实际发出时逐档归还。"""

    global_token: int | None
    source_token: int | None
    domain_matched: bool
    domain_token: int | None


@dataclass
class SourceState:
    """单个源在调度池内的运行状态。"""

    name: str
    weight: int
    max_concurrency: int
    rate_limiter: SlidingWindowRateLimiter | None
    queue: deque[PendingRequest] = field(default_factory=deque)
    in_flight: int = 0
    deficit: int = 0
    total_queued: int = 0
    total_dispatched: int = 0
    max_wait_ms: int = 0

    @property
    def queued(self) -> int:
        return len(self.queue)


class RequestScheduler:
    """单进程请求调度器。

    用法::

        scheduler = RequestScheduler(config, transport=fetcher)
        scheduler.register_source("demo_api")
        request = scheduler.submit("demo_api", url, params)
        result = scheduler.execute(request)   # 阻塞等结果，异常原样抛出
        scheduler.close()
    """

    def __init__(
        self,
        config: SchedulerConfig,
        *,
        transport: Transport,
        logger: logging.Logger | None = None,
        aging_ms: int = DEFAULT_AGING_MS,
    ) -> None:
        self._config = config
        self._transport = transport
        self._log = logger or logging.getLogger(LOGGER_NAME)
        self._aging_ms = aging_ms

        if config.qps is not None:
            self._global_limiter: SlidingWindowRateLimiter | None = (
                SlidingWindowRateLimiter(
                    RateLimitConfig(rate=max(1, round(config.qps)), window_ms=1000),
                    name="global",
                )
            )
        else:
            self._global_limiter = None
        self._domains = DomainRateRegistry(config.domain_rules)

        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._sources: dict[str, SourceState] = {}
        self._in_flight_total = 0
        self._submitted_total = 0
        self._completed_total = 0
        self._failed_total = 0
        self._degraded_total = 0
        self._closed = False
        self._degraded = False
        self._degrade_reason: str | None = None
        self._next_id = 1
        self._rr_cursor = 0

        self._executor = ThreadPoolExecutor(
            max_workers=config.max_concurrency,
            thread_name_prefix="crawl-sched-worker",
        )
        self._dispatcher = threading.Thread(
            target=self._dispatch_loop,
            name="crawl-scheduler-dispatch",
            daemon=True,
        )
        self._dispatcher.start()

    # ------------------------------------------------------------------ 注册
    def register_source(
        self,
        name: str,
        *,
        weight: int | None = None,
        max_concurrency: int | None = None,
        qps: float | None = None,
    ) -> None:
        """注册源；重复注册为空操作。参数缺省时取 ``SchedulerConfig``。"""

        with self._cond:
            if name in self._sources:
                return
            override = self._config.source_overrides(name)
            effective_weight = weight if weight is not None else override.weight
            effective_concurrency = (
                max_concurrency
                if max_concurrency is not None
                else self._config.effective_source_concurrency(name)
            )
            effective_qps = (
                qps
                if qps is not None
                else self._config.effective_source_qps(name)
            )
            limiter = None
            if effective_qps is not None:
                limiter = SlidingWindowRateLimiter(
                    RateLimitConfig(
                        rate=max(1, round(effective_qps)), window_ms=1000
                    ),
                    name=f"source:{name}",
                )
            self._sources[name] = SourceState(
                name=name,
                weight=effective_weight,
                max_concurrency=effective_concurrency,
                rate_limiter=limiter,
            )
            self._cond.notify_all()

    # ------------------------------------------------------------------ 提交
    def submit(
        self,
        source: str,
        url: str,
        params: dict[str, object] | None = None,
    ) -> PendingRequest:
        """把一次请求排入队列。

        调度器已降级或已关闭时抛 :class:`SchedulerDegraded` /
        :class:`SchedulerError`，由调用方降级为直连，不中断采集任务。
        """

        with self._cond:
            if self._degraded:
                self._degraded_total += 1
                raise SchedulerDegraded(
                    self._degrade_reason or "调度器处于降级态"
                )
            if self._closed:
                raise SchedulerError("调度器已关闭")
            state = self._sources.get(source)
            if state is None:
                # 懒注册：未显式注册的源按配置文件缺省值接入。
                self.register_source(source)
                state = self._sources[source]
            request = PendingRequest(
                request_id=self._next_id,
                source=source,
                url=url,
                params=params,
                enqueued_ms=now_ms(),
            )
            self._next_id += 1
            state.queue.append(request)
            state.total_queued += 1
            self._submitted_total += 1
            self._cond.notify_all()
            return request

    def execute(self, request: PendingRequest) -> Any:
        """阻塞等待请求结果；传输层异常原样抛出，保持原有错误隔离语义。"""

        request.event.wait()
        if request.degraded:
            raise SchedulerDegraded(
                self._degrade_reason or "排队期间调度器降级"
            )
        if request.error is not None:
            raise request.error
        return request.result

    # ------------------------------------------------------------------ 调度
    def _dispatch_loop(self) -> None:
        try:
            while True:
                with self._cond:
                    if self._closed:
                        return
                dispatched = self._dispatch_pass()
                with self._cond:
                    if self._closed:
                        return
                    if not self._has_pending_locked():
                        self._cond.wait(timeout=DISPATCH_TICK_SECONDS)
                    elif dispatched == 0:
                        # 全部被并发槽或速率窗口挡住，按节拍等待令牌恢复。
                        self._cond.wait(timeout=DISPATCH_TICK_SECONDS)
        except Exception as exc:  # noqa: BLE001 - 调度线程兜底，必须触发降级
            self.enter_degradation(
                f"调度线程异常 {exc.__class__.__name__}: {exc}"
            )

    def _has_pending_locked(self) -> bool:
        return any(state.queue for state in self._sources.values())

    def _dispatch_pass(self) -> int:
        with self._cond:
            return self._dispatch_pass_locked()

    def _dispatch_pass_locked(self) -> int:
        names = sorted(self._sources)
        if not names:
            return 0
        # 轮转起点，避免排序靠前的源在同分时总是先拿配额。
        start = self._rr_cursor % len(names)
        self._rr_cursor = (self._rr_cursor + 1) % len(names)
        rotated = names[start:] + names[:start]

        current = now_ms()
        aged_names = {
            name
            for name in rotated
            if self._sources[name].queue
            and current - self._sources[name].queue[0].enqueued_ms >= self._aging_ms
        }
        # 老化源优先，其余保持轮转顺序。
        ordered = [n for n in rotated if n in aged_names] + [
            n for n in rotated if n not in aged_names
        ]

        dispatched = 0
        for name in ordered:
            state = self._sources[name]
            if not state.queue:
                # 标准 DRR：队列清空时亏空归零，下一轮从 weight 重新累计。
                state.deficit = 0
                continue
            state.deficit += state.weight
            if name in aged_names:
                # 老化保证至少尝试一次，硬约束（并发/速率）仍然生效。
                state.deficit = max(state.deficit, 1)

            while state.queue and state.deficit > 0:
                if self._in_flight_total >= self._config.max_concurrency:
                    return dispatched
                if state.in_flight >= state.max_concurrency:
                    break
                request = state.queue[0]
                tokens = self._try_acquire_tokens_locked(state, request.url)
                if tokens is None:
                    # 某档速率窗口已满：请求留队，deficit 保留，下轮优先。
                    break
                state.queue.popleft()
                state.deficit -= 1
                state.in_flight += 1
                self._in_flight_total += 1
                state.total_dispatched += 1
                wait_ms = now_ms() - request.enqueued_ms
                state.max_wait_ms = max(state.max_wait_ms, wait_ms)
                dispatched += 1
                self._executor.submit(self._run_request, request, tokens)
        return dispatched

    def _try_acquire_tokens_locked(
        self, state: SourceState, url: str
    ) -> _Tokens | None:
        """按 全局 → 单源 → 域名 的顺序非阻塞申请令牌，失败时逐档回滚。"""

        global_token = None
        if self._global_limiter is not None:
            global_token = self._global_limiter.try_acquire()
            if global_token is None:
                return None

        source_token = None
        if state.rate_limiter is not None:
            source_token = state.rate_limiter.try_acquire()
            if source_token is None:
                if self._global_limiter is not None:
                    self._global_limiter.release(global_token)
                return None

        domain_matched, domain_token = self._domains.try_acquire_token(url)
        if domain_matched and domain_token is None:
            if state.rate_limiter is not None:
                state.rate_limiter.release(source_token)
            if self._global_limiter is not None:
                self._global_limiter.release(global_token)
            return None

        return _Tokens(
            global_token=global_token,
            source_token=source_token,
            domain_matched=domain_matched,
            domain_token=domain_token,
        )

    # ------------------------------------------------------------------ 执行
    def _run_request(self, request: PendingRequest, tokens: _Tokens) -> None:
        try:
            try:
                request.result = self._transport.get(
                    request.url, request.params
                )
            except Exception as exc:  # noqa: BLE001 - 单条失败隔离，不允许扩散
                request.error = exc
                with self._cond:
                    self._failed_total += 1
                self._log.warning(
                    "scheduled request failed source=%s url=%s error=%s",
                    request.source,
                    request.url,
                    exc.__class__.__name__,
                )
            else:
                with self._cond:
                    self._completed_total += 1
        except BaseException:
            # 工作线程不能静默死亡，否则提交者会永久挂起。
            with self._cond:
                if request.error is None:
                    request.error = SchedulerError("调度工作线程异常终止")
                    self._failed_total += 1
            raise
        finally:
            request.event.set()
            with self._cond:
                state = (
                    self._sources.get(request.source)
                    if isinstance(self._sources, dict)
                    else None
                )
                if state is not None:
                    state.in_flight = max(0, state.in_flight - 1)
                self._in_flight_total = max(0, self._in_flight_total - 1)
                self._cond.notify_all()

    # ------------------------------------------------------------------ 降级
    def enter_degradation(self, reason: str) -> None:
        """进入降级态：排队请求全部标记降级，由提交方直连执行。"""

        with self._cond:
            if self._degraded or self._closed:
                return
            self._degraded = True
            self._degrade_reason = reason
            # 降级可能由内部状态损坏触发，这里对状态结构做防御性处理，
            # 保证「降级动作本身」不会再抛异常而吞掉真正的故障。
            sources = getattr(self, "_sources", None)
            pending: list[PendingRequest] = []
            if isinstance(sources, dict):
                for state in sources.values():
                    queue = getattr(state, "queue", None)
                    if queue is not None:
                        pending.extend(queue)
                        queue.clear()
            for request in pending:
                request.degraded = True
                request.event.set()
            self._degraded_total += len(pending)
            self._cond.notify_all()
        self._log.error("scheduler degraded reason=%s", reason)

    @property
    def degraded(self) -> bool:
        with self._cond:
            return self._degraded

    @property
    def degrade_reason(self) -> str | None:
        with self._cond:
            return self._degrade_reason

    @property
    def config(self) -> SchedulerConfig:
        return self._config

    # ------------------------------------------------------------------ 观测
    def stats(self) -> dict[str, Any]:
        """调度池快照：总量计数、各源队列/在途/等待与限流窗口状态。"""

        with self._cond:
            sources = [
                {
                    "source": state.name,
                    "weight": state.weight,
                    "max_concurrency": state.max_concurrency,
                    "in_flight": state.in_flight,
                    "queued": state.queued,
                    "dispatched": state.total_dispatched,
                    "max_wait_ms": state.max_wait_ms,
                    "rate_limit": (
                        state.rate_limiter.snapshot()
                        if state.rate_limiter is not None
                        else None
                    ),
                }
                for state in sorted(self._sources.values(), key=lambda s: s.name)
            ]
            return {
                "closed": self._closed,
                "degraded": self._degraded,
                "degrade_reason": self._degrade_reason,
                "submitted": self._submitted_total,
                "completed": self._completed_total,
                "failed": self._failed_total,
                "degraded_requests": self._degraded_total,
                "in_flight": self._in_flight_total,
                "queued": sum(state.queued for state in self._sources.values()),
                "global_rate_limit": (
                    self._global_limiter.snapshot()
                    if self._global_limiter is not None
                    else None
                ),
                "domain_rules": self._domains.rules_snapshot(),
                "sources": sources,
            }

    def wait_idle(self, *, timeout: float | None = None) -> bool:
        """等待所有排队请求派发完毕（不要求执行完），超时返回 ``False``。"""

        deadline = None if timeout is None else time.monotonic() + timeout
        with self._cond:
            while any(state.queue for state in self._sources.values()):
                if self._degraded or self._closed:
                    return False
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._cond.wait(timeout=remaining)
                else:
                    self._cond.wait(timeout=0.1)
            return True

    # ------------------------------------------------------------------ 关闭
    def close(self, *, timeout: float = DEFAULT_CLOSE_TIMEOUT_SECONDS) -> None:
        """关闭调度池：停止派发，等待在途请求结束，释放限流资源。

        正常采集流程中排队队列已为空；异常关闭时残留请求被标记降级，
        由提交方自行直连，不会永久阻塞。
        """

        with self._cond:
            if self._closed:
                return
            self._closed = True
            pending: list[PendingRequest] = []
            if isinstance(self._sources, dict):
                for state in self._sources.values():
                    queue = getattr(state, "queue", None)
                    if queue is not None:
                        pending.extend(queue)
                        queue.clear()
            for request in pending:
                request.degraded = True
                request.event.set()
            self._cond.notify_all()

        self._dispatcher.join(timeout=5.0)
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._in_flight_total > 0 and time.monotonic() < deadline:
                self._cond.wait(timeout=0.1)
        self._executor.shutdown(wait=True, cancel_futures=True)
        self._domains.close()
        if self._global_limiter is not None:
            self._global_limiter.close()
        with self._lock:
            if isinstance(self._sources, dict):
                for state in self._sources.values():
                    if state.rate_limiter is not None:
                        state.rate_limiter.close()
