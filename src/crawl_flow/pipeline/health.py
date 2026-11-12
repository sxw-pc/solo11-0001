"""源健康探测：只请求列表第一页，按结果 + 历史错误率 + 平均响应时间评级。

设计约束：

- 复用 :class:`~crawl_flow.fetch.client.Fetcher` 与 ``SourceAdapter`` 的列表 URL/
  参数，不自定义特殊请求逻辑；UA、请求头、重试规则与正常采集一致。
- 默认超时为源 ``timeout_seconds`` 的一半，只请求 1 页、不抓详情、不落 raw、
  不写业务表；只有显式持久化时才追加 ``probe_records``。
- 探测在调用方线程内同步执行，不额外创建线程/连接池，避免与采集争抢资源。
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from crawl_flow.config import SourceConfig
from crawl_flow.errors import FetchError, ParseError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.models import HealthRating, ProbeOutcome, ProbeRecord, SourceHealthStats
from crawl_flow.sources import SourceAdapter, create_adapter
from crawl_flow.sources.base import build_fetcher
from crawl_flow.storage.db import Database

LOGGER_NAME = "crawl_flow.health"
MAX_ERROR_LENGTH = 500

# 评级阈值（集中定义，便于测试与后续调整）：
# - 即时结果：超时 / 5xx / 解析失败 / 传输错误 => 至多 degraded；4xx => unhealthy。
# - 历史错误率 >= 0.5 => 直接 unhealthy；>= 0.2 => 至多 degraded。
# - 本次响应耗时超过慢响应阈值 => 至多 degraded。
SLOW_PROBE_MS = 2000
SLOW_AVG_MS = 1500
FAILURE_RATE_UNHEALTHY = 0.5
FAILURE_RATE_DEGRADED = 0.2


@dataclass(frozen=True)
class ProbeResult:
    """一次探测的完整结果（评级 + 用于展示的原始信号）。"""

    source: str
    probed_at: datetime
    outcome: ProbeOutcome
    rating: HealthRating
    http_status: int | None
    elapsed_ms: int
    history_failure_rate: float | None = None
    avg_response_ms: float | None = None
    error: str | None = None

    def to_record(self) -> ProbeRecord:
        return ProbeRecord(
            source=self.source,
            probed_at=self.probed_at,
            outcome=self.outcome,
            rating=self.rating,
            http_status=self.http_status,
            elapsed_ms=self.elapsed_ms,
            history_failure_rate=self.history_failure_rate,
            avg_response_ms=self.avg_response_ms,
            error=self.error,
        )


def probe_source(
    source: SourceConfig,
    *,
    fetcher: Fetcher | None = None,
    history: SourceHealthStats | None = None,
    logger: logging.Logger | None = None,
) -> ProbeResult:
    """探测单个源。

    只请求列表第 1 页；不解析详情、不落 raw、不写业务数据库。``fetcher``
    不传时按源配置新建（客户端超时取 ``effective_probe_timeout()``，即正常
    采集的一半），探测结束立即关闭；传共享 fetcher 时仍以单请求超时覆盖，
    保证「探测超时 = 正常一半」且 UA / 请求头 / 重试规则不变。
    """

    log = logger or logging.getLogger(LOGGER_NAME)
    owns_fetcher = fetcher is None
    active_fetcher = fetcher or build_fetcher(
        source, timeout=source.effective_probe_timeout()
    )
    stats = history or SourceHealthStats()
    probed_at = datetime.now(timezone.utc)
    started = time.perf_counter()

    try:
        adapter = create_adapter(source, active_fetcher)
        result, outcome, error = _fetch_first_page(
            adapter, timeout=source.effective_probe_timeout()
        )
    finally:
        if owns_fetcher:
            active_fetcher.close()

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    rating = rate_health(outcome, elapsed_ms=elapsed_ms, history=stats)
    probe = ProbeResult(
        source=source.name,
        probed_at=probed_at,
        outcome=outcome,
        rating=rating,
        http_status=result.status_code if result is not None else None,
        elapsed_ms=elapsed_ms,
        history_failure_rate=stats.failure_rate,
        avg_response_ms=stats.avg_response_ms,
        error=error,
    )
    log.info(
        "probe source=%s rating=%s outcome=%s http_status=%s elapsed_ms=%d "
        "failure_rate=%s avg_response_ms=%s",
        probe.source,
        probe.rating,
        probe.outcome,
        probe.http_status if probe.http_status is not None else "-",
        probe.elapsed_ms,
        _fmt_float(probe.history_failure_rate),
        _fmt_float(probe.avg_response_ms),
    )
    return probe


def probe_sources(
    sources: list[SourceConfig],
    *,
    db: Database | None = None,
    persist: bool = False,
    logger: logging.Logger | None = None,
) -> list[ProbeResult]:
    """依次探测多个源；``persist=True`` 时把结果写入 ``probe_records``。

    每个源独立新建/关闭 Fetcher，串行执行；单个源探测失败不影响后续源。
    """

    log = logger or logging.getLogger(LOGGER_NAME)
    probes: list[ProbeResult] = []
    for source in sources:
        history = _safe_health_stats(db, source.name, log)
        probes.append(probe_source(source, history=history, logger=log))
    if persist:
        if db is None:
            raise ValueError("持久化探测结果需要提供 Database")
        db.init()
        for probe in probes:
            db.insert_probe(probe.to_record())
    return probes


def _safe_health_stats(
    db: Database | None,
    source: str,
    log: logging.Logger,
) -> SourceHealthStats:
    """读取历史统计；库里没有表或读取失败时退化为空统计，探测继续。"""

    if db is None:
        return SourceHealthStats()
    try:
        return db.health_stats(source)
    except StoreError as exc:
        log.warning("probe history unavailable source=%s error=%s", source, exc)
        return SourceHealthStats()


def rate_health(
    outcome: ProbeOutcome,
    *,
    elapsed_ms: int,
    history: SourceHealthStats,
) -> HealthRating:
    """按「即时结果 → 历史错误率 → 平均响应时间」依次收紧评级。

    规则：

    - 4xx 为永久失效 => ``unhealthy``。
    - 2xx/3xx => 先按瞬时健康处理，再由历史错误率与耗时收紧。
    - 超时 / 5xx / 传输错误 / 解析失败 => 临时或部分故障，基线为 ``degraded``；
      若历史错误率也高则升为 ``unhealthy``。
    """

    if outcome == "http_4xx":
        return "unhealthy"
    rating: HealthRating = "healthy" if outcome == "ok" else "degraded"

    if history.failure_rate is not None:
        if history.failure_rate >= FAILURE_RATE_UNHEALTHY:
            return "unhealthy"
        if history.failure_rate >= FAILURE_RATE_DEGRADED:
            rating = _worse(rating, "degraded")

    if elapsed_ms >= SLOW_PROBE_MS:
        rating = _worse(rating, "degraded")
    if history.avg_response_ms is not None and history.avg_response_ms >= SLOW_AVG_MS:
        rating = _worse(rating, "degraded")
    return rating


def _fetch_first_page(
    adapter: SourceAdapter,
    *,
    timeout: float | None,
) -> tuple[FetchResult | None, ProbeOutcome, str | None]:
    """请求列表第 1 页并解析；各类异常映射为探测结果，不向上抛。"""

    try:
        result = adapter.fetch_list_page(1, timeout=timeout)
    except FetchError as exc:
        outcome: ProbeOutcome = "timeout" if exc.timed_out else "transport_error"
        return None, outcome, _summary(str(exc))
    except ParseError as exc:
        return None, "parse_error", _summary(exc.reason)

    if result.status_code >= 500:
        return result, "http_5xx", f"HTTP {result.status_code}"
    if result.status_code >= 400:
        return result, "http_4xx", f"HTTP {result.status_code}"

    try:
        adapter.parse_list(result)
    except ParseError as exc:
        return result, "parse_error", _summary(exc.reason)
    return result, "ok", None


def _worse(current: HealthRating, floor: HealthRating) -> HealthRating:
    return current if _severity(current) >= _severity(floor) else floor


def _severity(rating: HealthRating) -> int:
    return {"healthy": 0, "degraded": 1, "unhealthy": 2}[rating]


def _summary(value: str) -> str:
    return value.strip().replace("\n", " ")[:MAX_ERROR_LENGTH]


def _fmt_float(value: float | None) -> str:
    return "-" if value is None else f"{value:.3f}"
