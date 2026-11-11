"""请求记账器：把 HTTP 事件翻译成账本操作，并维护本轮估算。

两套实现：

- :class:`SqliteBudgetMeter`：正式运行使用，每次请求发出前在 SQLite
  账本里原子预留一次请求额度，每个响应回来后累加实际字节；任一额度
  触顶即抛 :class:`BudgetExceededError` 熔断。
- :class:`DryRunBudgetMeter`：``--dry-run`` 演练使用，只在内存里估算
  本次会消耗多少额度，不打开也不写入任何账本文件，更不会熔断。

记账器通过 :meth:`bind_source` 知道当前在跑哪个源（管道按源串行执行，
切换源时重新绑定），Fetcher 的 httpx 事件钩子只负责调用
``on_request`` / ``on_response``，不需要自己识别来源。
"""

from __future__ import annotations

from dataclasses import dataclass

from crawl_flow.budget.errors import BudgetExceededError
from crawl_flow.budget.ledger import BudgetCounters, BudgetLedger, utc_today
from crawl_flow.budget.limits import BudgetLimits


@dataclass
class SourceEstimate:
    """单个源在本轮运行中的额度估算/实际消耗。"""

    source: str
    limit: BudgetLimits
    requests: int = 0
    response_bytes: int = 0
    would_break: bool = False
    break_reason: str = ""


class _BaseMeter:
    """记账器公共状态：源上下文与本轮估算。"""

    def __init__(self) -> None:
        self._current_source: str | None = None
        self._limits: dict[str, BudgetLimits] = {}
        self._estimates: dict[str, SourceEstimate] = {}

    def configure_source(self, limits: BudgetLimits) -> None:
        """登记一个源的额度配置（不限制也要登记，用于状态展示）。"""

        self._limits[limits.source] = limits
        self._estimates.setdefault(
            limits.source, SourceEstimate(source=limits.source, limit=limits)
        )

    def bind_source(self, source: str) -> None:
        if source not in self._limits:
            raise KeyError(f"预算记账器未登记源: {source}")
        self._current_source = source
        # 以账本里当天的累计值初始化本轮估算：这样即使本轮一个请求都没
        # 发出就熔断，汇总与状态展示仍然反映全天真实用量。
        counters = self._load_counters(source)
        estimate = self._estimates[source]
        estimate.requests = counters.request_count
        estimate.response_bytes = counters.response_bytes

    def _load_counters(self, source: str) -> BudgetCounters:
        """获取某源当日计数；子类按真实账本/内存估算分别实现。"""

        raise NotImplementedError

    def unbind_source(self) -> None:
        self._current_source = None

    @property
    def current_source(self) -> str | None:
        return self._current_source

    def _require_source(self) -> str:
        if self._current_source is None:
            raise RuntimeError("记账器尚未绑定源，无法记账")
        return self._current_source

    def estimate(self, source: str) -> SourceEstimate:
        return self._estimates[source]

    def estimates(self) -> list[SourceEstimate]:
        return [self._estimates[name] for name in sorted(self._estimates)]


class SqliteBudgetMeter(_BaseMeter):
    """正式运行记账器：真实读写 SQLite 账本，触顶即熔断。"""

    def __init__(self, ledger: BudgetLedger) -> None:
        super().__init__()
        self.ledger = ledger

    def _load_counters(self, source: str) -> BudgetCounters:
        return self.ledger.get_counters(source)

    def on_request(self) -> None:
        """HTTP 请求发出前调用：预留额度，触顶直接熔断、请求不会发出。"""

        source = self._require_source()
        limits = self._limits[source]
        estimate = self._estimates[source]
        # ledger 内部完成 check-and-increment 原子事务；超限抛异常且计数不变。
        counters = self.ledger.reserve_request(limits)
        estimate.requests = counters.request_count
        estimate.response_bytes = counters.response_bytes

    def on_response(self, byte_count: int) -> None:
        """HTTP 响应完整接收后调用：累加实际响应字节。"""

        source = self._require_source()
        limits = self._limits[source]
        estimate = self._estimates[source]
        counters = self.ledger.add_response_bytes(source, byte_count)
        estimate.requests = counters.request_count
        estimate.response_bytes = counters.response_bytes
        if (
            limits.daily_bytes_limit is not None
            and counters.response_bytes >= limits.daily_bytes_limit
        ):
            estimate.would_break = True
            estimate.break_reason = (
                f"每日响应字节上限 {limits.daily_bytes_limit} 已用尽"
            )

    def mark_request_break(self, exc: BudgetExceededError) -> None:
        """记录熔断原因，供本轮汇总与 CLI 输出使用。"""

        estimate = self._estimates[exc.source]
        estimate.would_break = True
        estimate.break_reason = exc.reason

    def snapshot(self, source: str) -> BudgetCounters:
        """读取某源当日账本快照（权威数据，来自 SQLite）。"""

        return self.ledger.get_counters(source)

    def close(self) -> None:
        self.ledger.close()


class DryRunBudgetMeter(_BaseMeter):
    """演练记账器：纯内存估算，不触碰账本、不熔断。"""

    def __init__(self) -> None:
        super().__init__()
        self._counters: dict[str, BudgetCounters] = {}

    def _load_counters(self, source: str) -> BudgetCounters:
        # 演练不读账本：同一进程内重复绑定同一源时保留本轮估算累计。
        return self._counters.get(
            source,
            BudgetCounters(
                source=source,
                usage_date=utc_today(),
                request_count=0,
                response_bytes=0,
            ),
        )

    def on_request(self) -> None:
        source = self._require_source()
        counters = self._counters.get(
            source,
            BudgetCounters(
                source=source,
                usage_date=utc_today(),
                request_count=0,
                response_bytes=0,
            ),
        )
        counters = BudgetCounters(
            source=source,
            usage_date=counters.usage_date,
            request_count=counters.request_count + 1,
            response_bytes=counters.response_bytes,
        )
        self._counters[source] = counters
        estimate = self._estimates[source]
        estimate.requests = counters.request_count
        estimate.response_bytes = counters.response_bytes
        limits = self._limits[source]
        self._refresh_break_estimate(estimate, limits, counters)

    def on_response(self, byte_count: int) -> None:
        source = self._require_source()
        counters = self._counters[source]
        counters = BudgetCounters(
            source=source,
            usage_date=counters.usage_date,
            request_count=counters.request_count,
            response_bytes=counters.response_bytes + byte_count,
        )
        self._counters[source] = counters
        estimate = self._estimates[source]
        estimate.requests = counters.request_count
        estimate.response_bytes = counters.response_bytes
        self._refresh_break_estimate(estimate, self._limits[source], counters)

    @staticmethod
    def _refresh_break_estimate(
        estimate: SourceEstimate,
        limits: BudgetLimits,
        counters: BudgetCounters,
    ) -> None:
        if (
            limits.daily_request_limit is not None
            and counters.request_count >= limits.daily_request_limit
        ):
            estimate.would_break = True
            estimate.break_reason = (
                f"若正式运行，请求次数将达到每日上限 {limits.daily_request_limit}"
            )
        if (
            limits.daily_bytes_limit is not None
            and counters.response_bytes >= limits.daily_bytes_limit
        ):
            estimate.would_break = True
            estimate.break_reason = (
                f"若正式运行，响应字节将达到每日上限 {limits.daily_bytes_limit}"
            )

    def snapshot(self, source: str) -> BudgetCounters:
        return self._counters.get(
            source,
            BudgetCounters(
                source=source,
                usage_date=utc_today(),
                request_count=0,
                response_bytes=0,
            ),
        )

    def close(self) -> None:
        return None
