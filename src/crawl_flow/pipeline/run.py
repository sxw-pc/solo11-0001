"""采集主流程：列表翻页 → 详情抓取 → 解析 → 归一化 → 布控 → 落 raw → 入库。"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from crawl_flow.config import SourceConfig
from crawl_flow.errors import FetchError, ParseError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.models import RawRecord, Record, WatchHit
from crawl_flow.observability.logging import log_item
from crawl_flow.pipeline.normalize import normalize_record
from crawl_flow.sources import ListItem, SourceAdapter, create_adapter
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore
from crawl_flow.watch import WatchEngine
from crawl_flow.watch.engine import RuleHit

LOGGER_NAME = "crawl_flow.pipeline"
MAX_ERROR_LENGTH = 500


@dataclass
class ItemOutcome:
    """单条记录的处理结果。"""

    external_id: str
    status: str
    elapsed_ms: int
    error: str | None = None


@dataclass
class RunSummary:
    """一轮采集的汇总。"""

    source: str
    ok: int = 0
    failed: int = 0
    skipped: int = 0
    elapsed_ms: int = 0
    results: list[ItemOutcome] = field(default_factory=list)


def run_source(
    source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    *,
    fetcher: Fetcher | None = None,
    limit: int | None = None,
    dry_run: bool = False,
    watch_engine: WatchEngine | None = None,
    logger: logging.Logger | None = None,
) -> RunSummary:
    """采集一个源。

    列表页请求/解析失败会抛出异常并终止本轮；单条详情失败只记录并继续。
    ``limit`` 限制处理的条数，超出部分记为 skipped（仍会翻完列表页以便计数）。
    ``watch_engine`` 为该源启用关键词布控；为 ``None`` 时跳过布控。
    """

    log = logger or logging.getLogger(LOGGER_NAME)
    owns_fetcher = fetcher is None
    active_fetcher = fetcher or Fetcher(timeout=source.timeout_seconds)
    adapter = create_adapter(source, active_fetcher)
    summary = RunSummary(source=source.name)
    started = time.perf_counter()
    seen: set[str] = set()
    processed = 0

    try:
        for page in adapter.iter_list_pages():
            for item in page.items:
                if item.external_id in seen:
                    summary.skipped += 1
                    log_item(log, source.name, item.external_id, "skipped", 0)
                    continue
                seen.add(item.external_id)
                if limit is not None and processed >= limit:
                    summary.skipped += 1
                    log_item(log, source.name, item.external_id, "skipped", 0)
                    continue
                processed += 1
                outcome = process_item(
                    adapter,
                    item,
                    db=db,
                    raw_store=raw_store,
                    dry_run=dry_run,
                    watch_engine=watch_engine,
                    logger=log,
                )
                summary.results.append(outcome)
                if outcome.status == "ok":
                    summary.ok += 1
                else:
                    summary.failed += 1
    finally:
        if owns_fetcher:
            active_fetcher.close()

    summary.elapsed_ms = int((time.perf_counter() - started) * 1000)
    log.info(
        "run summary source=%s ok=%d failed=%d skipped=%d elapsed_ms=%d dry_run=%s",
        summary.source,
        summary.ok,
        summary.failed,
        summary.skipped,
        summary.elapsed_ms,
        str(dry_run).lower(),
    )
    return summary


def process_item(
    adapter: SourceAdapter,
    item: ListItem,
    *,
    db: Database,
    raw_store: RawStore,
    dry_run: bool,
    logger: logging.Logger,
    watch_engine: WatchEngine | None = None,
) -> ItemOutcome:
    """抓取并处理单条详情；FetchError/ParseError 被隔离为 failed。"""

    source = adapter.source
    started = time.perf_counter()
    fetched_at = datetime.now(timezone.utc)

    try:
        result = adapter.fetch_detail(item.external_id)
    except FetchError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return _fail(
            source.name,
            item,
            db=db,
            raw_store=raw_store,
            dry_run=dry_run,
            logger=logger,
            error=str(exc),
            elapsed_ms=elapsed_ms,
            fetched_at=fetched_at,
        )

    if result.status_code >= 400:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return _fail(
            source.name,
            item,
            db=db,
            raw_store=raw_store,
            dry_run=dry_run,
            logger=logger,
            error=f"HTTP {result.status_code}",
            elapsed_ms=elapsed_ms,
            fetched_at=fetched_at,
            result=result,
        )

    try:
        fields = adapter.parse_detail(result, item.external_id)
        record = normalize_record(
            source,
            item.external_id,
            fields,
            detail_url=result.url,
            fetched_at=fetched_at,
        )
    except ParseError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return _fail(
            source.name,
            item,
            db=db,
            raw_store=raw_store,
            dry_run=dry_run,
            logger=logger,
            error=exc.reason,
            elapsed_ms=elapsed_ms,
            fetched_at=fetched_at,
            result=result,
        )

    # 归一化完成、写入存储前先过布控引擎；命中结果待条目入库后再落 watch_hits。
    watch_hits = _scan_watch(watch_engine, record, logger=logger)

    if not dry_run:
        raw_path = raw_store.save(
            source=source.name,
            external_id=item.external_id,
            payload=_success_payload(source.name, item, result, fetched_at),
            fetched_at=fetched_at,
        )
        db.upsert_raw(
            RawRecord(
                source=source.name,
                external_id=item.external_id,
                url=result.url,
                http_status=result.status_code,
                fetched_at=fetched_at,
                status="ok",
                error=None,
                raw_path=raw_path,
            )
        )
        db.upsert_record(record)
        _persist_watch_hits(
            db,
            record=record,
            hits=watch_hits,
            hit_at=fetched_at,
            logger=logger,
        )

    elapsed_ms = int((time.perf_counter() - started) * 1000)
    log_item(logger, source.name, item.external_id, "ok", elapsed_ms)
    return ItemOutcome(external_id=item.external_id, status="ok", elapsed_ms=elapsed_ms)


def _scan_watch(
    watch_engine: WatchEngine | None,
    record: Record,
    *,
    logger: logging.Logger,
) -> list[RuleHit]:
    """对归一化记录执行布控扫描。

    引擎内部已隔离单规则异常；这里再包一层兜底，保证布控自身的任何意外
    都只记日志，绝不让条目入库失败。
    """

    if watch_engine is None:
        return []
    try:
        return watch_engine.scan_record(record)
    except Exception as exc:  # noqa: BLE001 - 布控是旁路能力，不能拖垮主链路
        logger.warning(
            "布控引擎异常，跳过本条布控 source=%s external_id=%s error=%s",
            record.source,
            record.external_id,
            exc,
        )
        return []


def _persist_watch_hits(
    db: Database,
    *,
    record: Record,
    hits: list[RuleHit],
    hit_at: datetime,
    logger: logging.Logger,
) -> None:
    """把命中写入独立的 watch_hits 表；写入失败不影响条目入库结果。"""

    for hit in hits:
        watch_hit = WatchHit(
            source=record.source,
            external_id=record.external_id,
            rule_name=hit.rule_name,
            field=hit.field,
            rule_type=hit.rule_type,
            matched=hit.matched,
            context=hit.context,
            hit_at=hit_at,
        )
        try:
            inserted = db.insert_watch_hit(watch_hit)
        except StoreError as exc:
            logger.warning(
                "布控命中写入失败，跳过 source=%s external_id=%s rule=%s error=%s",
                record.source,
                record.external_id,
                hit.rule_name,
                exc,
            )
            continue
        if inserted:
            logger.info(
                "watch hit source=%s external_id=%s rule=%s field=%s matched=%r",
                record.source,
                record.external_id,
                hit.rule_name,
                hit.field,
                hit.matched,
            )


def _fail(
    source_name: str,
    item: ListItem,
    *,
    db: Database,
    raw_store: RawStore,
    dry_run: bool,
    logger: logging.Logger,
    error: str,
    elapsed_ms: int,
    fetched_at: datetime,
    result: FetchResult | None = None,
) -> ItemOutcome:
    summary = error.strip().replace("\n", " ")[:MAX_ERROR_LENGTH]
    if not dry_run:
        payload = _failure_payload(source_name, item, fetched_at, summary, result)
        raw_path = raw_store.save(
            source=source_name,
            external_id=item.external_id,
            payload=payload,
            fetched_at=fetched_at,
        )
        db.upsert_raw(
            RawRecord(
                source=source_name,
                external_id=item.external_id,
                url=result.url if result is not None else item.url,
                http_status=result.status_code if result is not None else None,
                fetched_at=fetched_at,
                status="failed",
                error=summary,
                raw_path=raw_path,
            )
        )
    log_item(logger, source_name, item.external_id, "failed", elapsed_ms)
    logger.warning(
        "item failed source=%s external_id=%s error=%s",
        source_name,
        item.external_id,
        summary,
    )
    return ItemOutcome(
        external_id=item.external_id,
        status="failed",
        elapsed_ms=elapsed_ms,
        error=summary,
    )


def _success_payload(
    source_name: str,
    item: ListItem,
    result: FetchResult,
    fetched_at: datetime,
) -> dict[str, Any]:
    return {
        "source": source_name,
        "external_id": item.external_id,
        "url": result.url,
        "status": result.status_code,
        "fetched_at": fetched_at.isoformat(),
        "headers": result.headers,
        "body": result.body,
    }


def _failure_payload(
    source_name: str,
    item: ListItem,
    fetched_at: datetime,
    error: str,
    result: FetchResult | None,
) -> dict[str, Any]:
    return {
        "source": source_name,
        "external_id": item.external_id,
        "url": result.url if result is not None else item.url,
        "status": result.status_code if result is not None else None,
        "fetched_at": fetched_at.isoformat(),
        "headers": result.headers if result is not None else {},
        "body": result.body if result is not None else "",
        "error": error,
    }
