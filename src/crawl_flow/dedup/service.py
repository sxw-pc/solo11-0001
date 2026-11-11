"""去重服务编排：计算指纹 → 持久化 → 识别完全/近似重复 → 只写标记。

服务层是模块的对外主入口，保证四条硬约束：

1. 只通过 :class:`~crawl_flow.dedup.reader.RecordReader` 只读主库，
   所有写操作落在独立指纹库；
2. 去重结果以标记簇形式存储，不删除、不改写任何采集数据；
3. 已持久化且未变化的指纹不重复写库，任务重启无需重算；
4. 规则版本变化时按新版本批量重算，并清理失效的历史指纹与标记。
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass
from typing import Iterable

from crawl_flow.dedup.config import DedupConfig
from crawl_flow.dedup.fingerprint import (
    Fingerprinter,
    FingerprintRule,
    max_hamming_for_threshold,
)
from crawl_flow.dedup.models import FingerprintRow
from crawl_flow.dedup.reader import RecordData
from crawl_flow.dedup.similarity import (
    cluster_pairs,
    find_similar_pairs,
    group_by_exact_hash,
)
from crawl_flow.dedup.store import FingerprintStore

LOGGER_NAME = "crawl_flow.dedup"


@dataclass
class ComputeStats:
    """一轮指纹计算的统计结果。"""

    rule_version: int
    total: int = 0
    written: int = 0
    unchanged: int = 0
    skipped: int = 0
    stale_deleted: int = 0

    @property
    def computed(self) -> int:
        """实际生成有效指纹的条目数（不含跳过、未变化）。"""

        return self.written


@dataclass
class MarkStats:
    """一轮重复标记的统计结果。"""

    rule_version: int
    fingerprint_count: int = 0
    exact_groups: int = 0
    similar_groups: int = 0
    marked_records: int = 0
    max_hamming: int = 0


@dataclass
class FingerprintResult:
    """单条记录的指纹计算视图（供程序化调用与测试使用）。"""

    source: str
    external_id: str
    title_hash: str | None
    content_hash: str | None
    simhash_hex: str | None
    skipped: bool
    skip_reason: str | None
    content_length: int


class FingerprintService:
    """指纹计算与内容级去重的编排服务。"""

    def __init__(
        self,
        store: FingerprintStore,
        config: DedupConfig | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.store = store
        self.config = config or DedupConfig()
        self.logger = logger or logging.getLogger(LOGGER_NAME)
        self.rule = self._build_rule(self.config)
        self.fingerprinter = Fingerprinter(self.rule)

    @staticmethod
    def _build_rule(config: DedupConfig) -> FingerprintRule:
        rule_config = config.rule
        return FingerprintRule(
            version=rule_config.version,
            hash_algorithm=rule_config.hash_algorithm,
            lowercase=rule_config.lowercase,
            strip_punctuation=rule_config.strip_punctuation,
            cjk_bigram=rule_config.cjk_bigram,
            min_content_length=rule_config.min_content_length,
            min_title_length=rule_config.min_title_length,
        )

    # ------------------------------------------------------------------
    # 单条计算（无副作用，便于嵌入其它流程或单独测试）
    # ------------------------------------------------------------------

    def fingerprint_record(self, record: RecordData) -> FingerprintResult:
        result = self.fingerprinter.fingerprint(
            title=record.title, content=record.content
        )
        return FingerprintResult(
            source=record.source,
            external_id=record.external_id,
            title_hash=result.title_hash,
            content_hash=result.content_hash,
            simhash_hex=result.simhash_hex,
            skipped=result.skipped,
            skip_reason=result.skip_reason,
            content_length=len(result.content_normalized),
        )

    # ------------------------------------------------------------------
    # 批量计算与持久化
    # ------------------------------------------------------------------

    def compute_fingerprints(
        self,
        records: Iterable[RecordData],
        *,
        force: bool = False,
        prune_missing: bool = False,
    ) -> ComputeStats:
        """为采集记录批量计算并持久化指纹。

        ``force=True`` 时无视已持久化指纹强制重算（规则更新后的批量重算
        入口）；``prune_missing`` 清理主库已不存在的失效指纹。
        内容为空/过短自动跳过，不生成无效指纹。
        """

        stats = ComputeStats(rule_version=self.rule.version)
        active_keys: list[tuple[str, str]] = []

        for record in records:
            stats.total += 1
            key = (record.source, record.external_id)
            active_keys.append(key)
            result = self.fingerprinter.fingerprint(
                title=record.title, content=record.content
            )

            if result.skipped:
                # 跳过也留痕（skipped=1），但 content_hash/simhash 均为 NULL，
                # 后续聚类天然不会纳入这些条目。
                self._upsert(record, result, force_write=force)
                stats.skipped += 1
                self.logger.info(
                    "fingerprint skipped source=%s external_id=%s reason=%s",
                    record.source,
                    record.external_id,
                    result.skip_reason,
                )
                continue

            if not force and self._is_unchanged(record, result):
                stats.unchanged += 1
                continue

            self._upsert(record, result, force_write=True)
            stats.written += 1

        if prune_missing:
            stats.stale_deleted = self.store.delete_fingerprints_not_in(active_keys)
            if stats.stale_deleted:
                self.logger.info(
                    "stale fingerprints removed count=%d rule_version=%d",
                    stats.stale_deleted,
                    self.rule.version,
                )

        self.logger.info(
            "fingerprint compute finished rule_version=%d total=%d written=%d "
            "unchanged=%d skipped=%d stale_deleted=%d",
            stats.rule_version,
            stats.total,
            stats.written,
            stats.unchanged,
            stats.skipped,
            stats.stale_deleted,
        )
        return stats

    def recompute_all(self, records: Iterable[RecordData]) -> ComputeStats:
        """规则更新后的全量重算：强制按当前规则版本重写并清理失效指纹。"""

        return self.compute_fingerprints(
            records, force=True, prune_missing=True
        )

    def _is_unchanged(self, record: RecordData, result: object) -> bool:
        existing = self.store.get_fingerprint(record.source, record.external_id)
        if existing is None or existing.rule_version != self.rule.version:
            return False
        return (
            existing.content_hash == result.content_hash  # type: ignore[attr-defined]
            and existing.simhash_hex == result.simhash_hex  # type: ignore[attr-defined]
            and existing.title_hash == result.title_hash  # type: ignore[attr-defined]
            and existing.skipped is False
        )

    def _upsert(self, record: RecordData, result: object, *, force_write: bool) -> None:
        # force_write 当前仅用于语义清晰；upsert 本身幂等。
        del force_write
        self.store.upsert_fingerprint(
            {
                "source": record.source,
                "external_id": record.external_id,
                "title": record.title[:512],
                "title_hash": result.title_hash,  # type: ignore[attr-defined]
                "content_hash": result.content_hash,  # type: ignore[attr-defined]
                "simhash_hex": result.simhash_hex,  # type: ignore[attr-defined]
                "rule_version": self.rule.version,
                "rule_signature": self.rule.signature(),
                "content_length": len(result.content_normalized),  # type: ignore[attr-defined]
                "skipped": result.skipped,  # type: ignore[attr-defined]
                "skip_reason": result.skip_reason,  # type: ignore[attr-defined]
            }
        )

    # ------------------------------------------------------------------
    # 重复识别与标记
    # ------------------------------------------------------------------

    def build_dedup_marks(self) -> MarkStats:
        """基于当前规则版本的全部指纹识别重复并重建标记簇。

        完全重复：归一化正文哈希一致，准确率 100%（同算法下同内容必同哈希）。
        近似重复：SimHash 汉明距离 <= 阈值换算值，先 LSH 召回再逐对校验，
        低于阈值的内容不会被误判。所有关联来源与条目标识一并落标记表。
        """

        rows = self.store.iter_fingerprints(rule_version=self.rule.version)
        stats = MarkStats(
            rule_version=self.rule.version, fingerprint_count=len(rows)
        )
        if not rows:
            self.store.replace_groups([])
            return stats

        row_by_key: dict[tuple[str, str], FingerprintRow] = {
            (row.source, row.external_id): row for row in rows
        }
        keys = list(row_by_key.keys())
        groups: list[dict[str, object]] = []
        marked: set[tuple[str, str]] = set()

        # 1) 完全重复：内容精确哈希分组（同源、跨源一视同仁）。
        exact_groups = group_by_exact_hash(
            ((key, row.content_hash) for key, row in row_by_key.items())
        )
        exact_hash_by_key = {
            key: row_by_key[key].content_hash for key in keys
        }
        for members in exact_groups:
            group = self._build_group_payload("exact", members, row_by_key)
            group["min_similarity"] = 1.0
            groups.append(group)
            marked.update(members)
        stats.exact_groups = len(exact_groups)

        # 2) 近似重复：SimHash + 配置阈值；完全重复对不再重复计入相似簇。
        if self.config.similarity.enabled:
            max_hamming = max_hamming_for_threshold(self.config.similarity.threshold)
            stats.max_hamming = max_hamming
            simhash_items = [
                (key, int(row.simhash_hex, 16))
                for key, row in row_by_key.items()
                if row.simhash_hex
            ]
            pairs = find_similar_pairs(
                simhash_items,
                max_hamming=max_hamming,
                bands=self.config.similarity.bands,
            )
            non_exact_pairs = [
                pair
                for pair in pairs
                if exact_hash_by_key[pair.left] != exact_hash_by_key[pair.right]
            ]
            similar_groups = cluster_pairs(keys, non_exact_pairs)
            pair_index = {
                tuple(sorted((pair.left, pair.right))): pair for pair in non_exact_pairs
            }
            for members in similar_groups:
                group = self._build_group_payload("similar", members, row_by_key)
                hamming_values = [
                    pair_index[tuple(sorted((left, right)))].hamming
                    for index, left in enumerate(members)
                    for right in members[index + 1 :]
                    if tuple(sorted((left, right))) in pair_index
                ]
                similarity_values = [
                    pair_index[tuple(sorted((left, right)))].similarity
                    for index, left in enumerate(members)
                    for right in members[index + 1 :]
                    if tuple(sorted((left, right))) in pair_index
                ]
                group["max_hamming"] = max(hamming_values) if hamming_values else None
                group["min_similarity"] = (
                    min(similarity_values) if similarity_values else None
                )
                groups.append(group)
                marked.update(members)
            stats.similar_groups = len(similar_groups)

        self.store.replace_groups(groups)
        stats.marked_records = len(marked)
        self.logger.info(
            "dedup marks rebuilt rule_version=%d fingerprints=%d exact_groups=%d "
            "similar_groups=%d marked_records=%d threshold=%.2f",
            stats.rule_version,
            stats.fingerprint_count,
            stats.exact_groups,
            stats.similar_groups,
            stats.marked_records,
            self.config.similarity.threshold,
        )
        return stats

    def _build_group_payload(
        self,
        group_type: str,
        members: list[tuple[str, str]],
        row_by_key: dict[tuple[str, str], FingerprintRow],
    ) -> dict[str, object]:
        sources = sorted({key[0] for key in members})
        if group_type == "exact":
            group_key = row_by_key[members[0]].content_hash
        else:
            joined = "|".join(f"{source}:{external_id}" for source, external_id in members)
            group_key = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:16]
        return {
            "group_type": group_type,
            "rule_version": self.rule.version,
            "group_key": group_key,
            "size": len(members),
            "source_count": len(sources),
            "sources": sources,
            "max_hamming": None,
            "min_similarity": None,
            "members": [
                {
                    "source": source,
                    "external_id": external_id,
                    "title": row_by_key[(source, external_id)].title,
                }
                for source, external_id in members
            ],
        }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def lookup_record_marks(self, source: str, external_id: str) -> list[dict[str, object]]:
        """查询单条记录命中的重复簇，返回含全部关联来源与条目标识的视图。"""

        result: list[dict[str, object]] = []
        for group in self.store.groups_for_record(source, external_id):
            members = self.store.list_members(group.id)
            result.append(
                {
                    "group_id": group.id,
                    "group_type": group.group_type,
                    "group_key": group.group_key,
                    "rule_version": group.rule_version,
                    "size": group.size,
                    "sources": list(group.sources),
                    "min_similarity": group.min_similarity,
                    "max_hamming": group.max_hamming,
                    "members": [
                        {
                            "source": member.source,
                            "external_id": member.external_id,
                            "title": member.title,
                        }
                        for member in members
                    ],
                }
            )
        return result

    def run_full(
        self, records: Iterable[RecordData], *, force: bool = False
    ) -> tuple[ComputeStats, MarkStats]:
        """计算指纹并立即重建标记的便捷组合入口。"""

        compute_stats = self.compute_fingerprints(records, force=force, prune_missing=force)
        mark_stats = self.build_dedup_marks()
        return compute_stats, mark_stats
