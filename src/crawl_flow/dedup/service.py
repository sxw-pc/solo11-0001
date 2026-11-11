"""去重编排：扫描 records → 计算/复用指纹 → 重建重复标记。"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from crawl_flow.dedup.config import DedupConfig
from crawl_flow.dedup.fingerprint import compute_fingerprint
from crawl_flow.dedup.similarity import FingerprintEntry, build_duplicate_groups
from crawl_flow.dedup.store import DedupStore

LOGGER_NAME = "crawl_flow.dedup"


@dataclass
class DedupSummary:
    """一轮去重的汇总。"""

    scanned: int = 0
    computed: int = 0
    reused: int = 0
    skipped_short: int = 0
    pruned: int = 0
    groups: int = 0
    marked: int = 0
    elapsed_ms: int = 0
    rule_version: str = ""
    notes: list[str] = field(default_factory=list)

    @property
    def fingerprints_per_second(self) -> float:
        if self.elapsed_ms <= 0:
            return float("inf")
        return self.computed / (self.elapsed_ms / 1000)


class DedupService:
    """去重主流程；只读 records，指纹与标记写入模块自有表。"""

    def __init__(self, store: DedupStore, config: DedupConfig) -> None:
        self.store = store
        self.config = config

    def run(
        self,
        *,
        source: str | None = None,
        force: bool = False,
        logger: logging.Logger | None = None,
    ) -> DedupSummary:
        """跑一轮去重。

        1. 扫描 records（可按源过滤），为缺失或规则版本过期的记录计算指纹；
        2. 内容为空/过短的记录跳过且不生成指纹，并清理其历史指纹；
        3. 基于全部当前版本指纹（跨源）重建重复标记，只标记不删除。
        """

        log = logger or logging.getLogger(LOGGER_NAME)
        started = time.perf_counter()
        version = self.config.effective_version()
        summary = DedupSummary(rule_version=version)

        records = self.store.iter_records(source)
        summary.scanned = len(records)
        existing = self.store.fingerprint_map()

        pending: list[dict[str, object]] = []
        invalid_keys: list[tuple[str, str]] = []
        seen_keys: set[tuple[str, str]] = set()
        now = datetime.now(timezone.utc)

        for record in records:
            key = (record.source, record.external_id)
            seen_keys.add(key)
            row = existing.get(key)
            if not force and row is not None and row.rule_version == version:
                summary.reused += 1
                continue
            fingerprint = compute_fingerprint(record.title, record.content, self.config)
            if fingerprint is None:
                summary.skipped_short += 1
                if row is not None:
                    invalid_keys.append(key)
                log.info(
                    "source=%s external_id=%s status=fingerprint_skipped reason=too_short",
                    record.source,
                    record.external_id,
                )
                continue
            pending.append(
                {
                    "source": record.source,
                    "external_id": record.external_id,
                    "rule_version": version,
                    "exact_hash": fingerprint.exact_hash,
                    "content_hash": fingerprint.content_hash,
                    "title_hash": fingerprint.title_hash,
                    "simhash": (
                        format(fingerprint.simhash, "x")
                        if fingerprint.simhash is not None
                        else None
                    ),
                    "content_length": fingerprint.content_length,
                    "created_at": row.created_at if row is not None else now,
                    "updated_at": now,
                }
            )

        # 清理 records 中已不存在的孤儿指纹（仅限本次扫描范围，只清理
        # 指纹表，不动原始数据）。
        scope = (
            existing.keys()
            if source is None
            else [key for key in existing if key[0] == source]
        )
        orphan_keys = [key for key in scope if key not in seen_keys]
        summary.pruned = self.store.delete_fingerprints(invalid_keys + orphan_keys)
        self.store.upsert_fingerprints(pending)
        summary.computed = len(pending)

        # 跨源重建重复标记：仅使用当前规则版本的指纹。
        current_rows = self.store.list_fingerprints(rule_version=version)
        entries = [
            FingerprintEntry(
                source=row.source,
                external_id=row.external_id,
                exact_hash=row.exact_hash,
                simhash=int(row.simhash, 16) if row.simhash else None,
                ordinal=row.id,
            )
            for row in current_rows
        ]
        groups = build_duplicate_groups(
            entries,
            bits=self.config.similarity.bits,
            threshold=self.config.similarity.threshold,
            similarity_enabled=self.config.similarity.enabled,
        )
        self.store.replace_groups(groups, version)
        summary.groups = len(groups)
        summary.marked = sum(len(group.members) for group in groups)

        summary.elapsed_ms = int((time.perf_counter() - started) * 1000)
        log.info(
            "dedup summary scanned=%d computed=%d reused=%d skipped_short=%d "
            "pruned=%d groups=%d marked=%d elapsed_ms=%d rule_version=%s",
            summary.scanned,
            summary.computed,
            summary.reused,
            summary.skipped_short,
            summary.pruned,
            summary.groups,
            summary.marked,
            summary.elapsed_ms,
            version,
        )
        return summary
