"""采集主流程：列表翻页 → 详情抓取 → 解析 → 规则引擎清洗/校验 → 落 raw → 入库。"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from crawl_flow.config import SourceConfig
from crawl_flow.errors import FetchError, ParseError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.models import RawRecord, Record
from crawl_flow.observability.logging import log_item
from crawl_flow.pipeline.normalize import (
    normalize_record,
    normalize_record_with_rules,
)
from crawl_flow.rules.engine import ProcessResult, RuleEngine
from crawl_flow.sources import ListItem, SourceAdapter, create_adapter
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

LOGGER_NAME = "crawl_flow.pipeline"
MAX_ERROR_LENGTH = 500


@dataclass
class ItemOutcome:
    """单条记录的处理结果。"""

    external_id: str
    status: str
    elapsed_ms: int
    error: str | None = None
    quality_score: float | None = None
    low_quality: bool = False


@dataclass
class RunSummary:
    """一轮采集的汇总。"""

    source: str
    ok: int = 0
    failed: int = 0
    skipped: int = 0
    low_quality: int = 0
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
    logger: logging.Logger | None = None,
    engine: RuleEngine | None = None,
) -> RunSummary:
    """采集一个源。

    列表页请求/解析失败会抛出异常并终止本轮；单条详情失败只记录并继续。
    ``limit`` 限制处理的条数，超出部分记为 skipped（仍会翻完列表页以便计数）。
    ``engine`` 为可选规则引擎；为 None 时走历史默认清洗路径。
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
                    logger=log,
                    engine=engine,
                )
                summary.results.append(outcome)
                if outcome.status == "ok":
                    summary.ok += 1
                elif outcome.status == "skipped_quality":
                    summary.skipped += 1
                else:
                    summary.failed += 1
                if outcome.low_quality:
                    summary.low_quality += 1
    finally:
        if owns_fetcher:
            active_fetcher.close()

    summary.elapsed_ms = int((time.perf_counter() - started) * 1000)
    log.info(
        "run summary source=%s ok=%d failed=%d skipped=%d low_quality=%d elapsed_ms=%d dry_run=%s",
        summary.source,
        summary.ok,
        summary.failed,
        summary.skipped,
        summary.low_quality,
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
    engine: RuleEngine | None = None,
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

    engine_result: ProcessResult | None = None
    try:
        fields = adapter.parse_detail(result, item.external_id)
        if engine is None or not engine.has_rules(source.name):
            record: Record = normalize_record(
                source,
                item.external_id,
                fields,
                detail_url=result.url,
                fetched_at=fetched_at,
            )
        else:
            record, engine_result = normalize_record_with_rules(
                source,
                item.external_id,
                fields,
                detail_url=result.url,
                fetched_at=fetched_at,
                engine=engine,
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

    elapsed_ms = int((time.perf_counter() - started) * 1000)

    # 质量校验处置：skip 不入库（但留 raw 与轨迹），mark/warn 正常入库。
    if engine_result is not None and engine_result.action == "skip":
        if not dry_run:
            raw_path = raw_store.save(
                source=source.name,
                external_id=item.external_id,
                payload=_success_payload(
                    source.name, item, result, fetched_at, engine_result
                ),
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
        logger.info(
            "item skipped by quality source=%s external_id=%s score=%.2f failures=%d",
            source.name,
            item.external_id,
            engine_result.quality_score,
            len(engine_result.validation_failures),
        )
        log_item(logger, source.name, item.external_id, "skipped_quality", elapsed_ms)
        return ItemOutcome(
            external_id=item.external_id,
            status="skipped_quality",
            elapsed_ms=elapsed_ms,
            quality_score=engine_result.quality_score,
            low_quality=engine_result.low_quality,
        )

    if not dry_run:
        raw_path = raw_store.save(
            source=source.name,
            external_id=item.external_id,
            payload=_success_payload(source.name, item, result, fetched_at, engine_result),
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

    if engine_result is not None and engine_result.low_quality:
        logger.info(
            "low quality record source=%s external_id=%s score=%.2f threshold=%.2f",
            source.name,
            item.external_id,
            engine_result.quality_score,
            engine_result.threshold,
        )
    log_item(logger, source.name, item.external_id, "ok", elapsed_ms)
    return ItemOutcome(
        external_id=item.external_id,
        status="ok",
        elapsed_ms=elapsed_ms,
        quality_score=engine_result.quality_score if engine_result else None,
        low_quality=bool(engine_result and engine_result.low_quality),
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
    engine_result: ProcessResult | None = None,
) -> dict[str, Any]:
    payload = {
        "source": source_name,
        "external_id": item.external_id,
        "url": result.url,
        "status": result.status_code,
        "fetched_at": fetched_at.isoformat(),
        "headers": result.headers,
        "body": result.body,
    }
    if engine_result is not None:
        payload["quality"] = {
            "score": engine_result.quality_score,
            "threshold": engine_result.threshold,
            "low_quality": engine_result.low_quality,
            "action": engine_result.action,
            "field_scores": engine_result.field_scores,
            "validation_failures": engine_result.validation_failures,
            "degraded_fields": engine_result.degraded_fields,
        }
        payload["rule_trace"] = engine_result.trace_as_dicts()
    return payload


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
