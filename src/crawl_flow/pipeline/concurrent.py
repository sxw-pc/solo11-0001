"""并发采集编排。

与 :mod:`crawl_flow.pipeline.run` 的串行实现共用同一套单条处理逻辑
（:func:`~crawl_flow.pipeline.run.process_item`），因此错误隔离、raw 落盘、
失败标记规则完全不变；本模块只做两件事：

1. 用与源并发上限等大的有界线程池并发处理详情条目；
2. 给 :class:`Database` / :class:`RawStore` 加锁包装，保证多线程访问安全。

列表翻页仍在调用线程顺序执行（翻页结果决定后续任务，且列表请求本身也
经过调度器，受 QPS 与域名规则约束），适配器、归一化、存储等核心组件
零修改。
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from crawl_flow.config import SourceConfig
from crawl_flow.pipeline.run import ItemOutcome, RunSummary, process_item
from crawl_flow.scheduler import RequestScheduler, wrap_fetcher
from crawl_flow.scheduler.pool import Transport
from crawl_flow.sources import create_adapter
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

LOGGER_NAME = "crawl_flow.pipeline.concurrent"


class ThreadSafeDatabase:
    """``Database`` 的线程安全代理：所有方法在同一把可重入锁内执行。

    锁直接复用 ``Database`` 实例自带的锁，因此多个源各自包装同一个
    ``Database`` 时仍然串行化；SQLAlchemy 的 Session 不是线程共享对象，
    加锁后多线程的 upsert/统计调用与原单线程管道的写入顺序语义一致。
    """

    def __init__(self, db: Database) -> None:
        if isinstance(db, ThreadSafeDatabase):
            # 幂等：重复包装直接返回同一个代理。
            self._db = db._db
            self._lock = db._lock
        else:
            self._db = db
            self._lock = db.lock

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._db, name)
        if callable(attr):
            def _locked(*args: Any, **kwargs: Any) -> Any:
                with self._lock:
                    return attr(*args, **kwargs)
            return _locked
        return attr


class ThreadSafeRawStore:
    """``RawStore`` 的线程安全代理；复用底层实例锁，包装幂等。"""

    def __init__(self, raw_store: RawStore) -> None:
        if isinstance(raw_store, ThreadSafeRawStore):
            self._raw_store = raw_store._raw_store
            self._lock = raw_store._lock
        else:
            self._raw_store = raw_store
            self._lock = raw_store.lock

    def save(self, **kwargs: Any) -> str:
        with self._lock:
            return self._raw_store.save(**kwargs)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._raw_store, name)


def run_source_concurrent(
    source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    scheduler: RequestScheduler,
    *,
    fetcher: Transport,
    limit: int | None = None,
    dry_run: bool = False,
    logger: logging.Logger | None = None,
) -> RunSummary:
    """并发采集一个源。

    与 :func:`crawl_flow.pipeline.run.run_source` 入参/返回值保持同构，
    区别是详情条目经有界线程池并发执行，HTTP 请求统一走 ``scheduler``。
    ``fetcher`` 为共享的底层直连客户端，由调用方负责生命周期管理。
    """

    log = logger or logging.getLogger(LOGGER_NAME)
    scheduled_fetcher = wrap_fetcher(
        source.name, scheduler, fetcher, logger=log
    )
    adapter = create_adapter(source, scheduled_fetcher)
    # 包装幂等且复用底层锁：多源共享同一 Database/RawStore 时串行化。
    safe_db = ThreadSafeDatabase(db)
    safe_raw = ThreadSafeRawStore(raw_store)

    summary = RunSummary(source=source.name)
    started = time.perf_counter()
    seen: set[str] = set()
    processed = 0
    max_workers = scheduler.config.effective_source_concurrency(source.name)

    futures: list[Future[ItemOutcome]] = []
    with ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix=f"crawl-{source.name}",
    ) as pool:
        try:
            for page in adapter.iter_list_pages():
                for item in page.items:
                    if item.external_id in seen:
                        summary.skipped += 1
                        log_item_safe(log, source.name, item.external_id)
                        continue
                    seen.add(item.external_id)
                    if limit is not None and processed >= limit:
                        summary.skipped += 1
                        log_item_safe(log, source.name, item.external_id)
                        continue
                    processed += 1
                    futures.append(
                        pool.submit(
                            process_item,
                            adapter,
                            item,
                            db=safe_db,
                            raw_store=safe_raw,
                            dry_run=dry_run,
                            logger=log,
                        )
                    )
        finally:
            # 按提交顺序收集，保证汇总确定性；工作线程异常（如 StoreError）
            # 与串行管道一样向上抛出，由 CLI 统一按退出码 2 处理。
            outcomes = [future.result() for future in futures]

    for outcome in outcomes:
        summary.results.append(outcome)
        if outcome.status == "ok":
            summary.ok += 1
        else:
            summary.failed += 1

    summary.elapsed_ms = int((time.perf_counter() - started) * 1000)
    log.info(
        "run summary source=%s ok=%d failed=%d skipped=%d elapsed_ms=%d dry_run=%s "
        "mode=concurrent degraded=%s",
        summary.source,
        summary.ok,
        summary.failed,
        summary.skipped,
        summary.elapsed_ms,
        str(dry_run).lower(),
        str(scheduler.degraded).lower(),
    )
    return summary


def log_item_safe(
    logger: logging.Logger, source: str, external_id: str
) -> None:
    """跳过条目日志（标准库 logging 自身线程安全）。"""

    from crawl_flow.observability.logging import log_item

    log_item(logger, source, external_id, "skipped", 0)
