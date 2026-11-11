"""独立内容指纹与去重模块。

与采集主流程完全解耦：只读主库 ``records``，指纹与去重标记写入独立的
SQLite 文件，只做标记不删除数据。核心能力：

* :class:`FingerprintRule` / :class:`Fingerprinter`：可配置的归一化规则、
  精确内容哈希与中文友好的 64 位 SimHash；
* :class:`FingerprintService`：指纹批量计算、持久化、规则版本化重算、
  同源/跨源完全重复与阈值内近似重复识别；
* :class:`FingerprintStore`：指纹与标记簇的独立持久化存储。
"""

from crawl_flow.dedup.config import (
    DedupConfig,
    FingerprintRuleConfig,
    SimilarityConfig,
    load_dedup_config,
)
from crawl_flow.dedup.fingerprint import (
    ContentFingerprint,
    Fingerprinter,
    FingerprintRule,
    hamming_distance,
    max_hamming_for_threshold,
    simhash,
    similarity,
)
from crawl_flow.dedup.reader import RecordData, RecordReader
from crawl_flow.dedup.service import (
    ComputeStats,
    FingerprintResult,
    FingerprintService,
    MarkStats,
)
from crawl_flow.dedup.similarity import (
    SimilarPair,
    UnionFind,
    cluster_pairs,
    find_similar_pairs,
    group_by_exact_hash,
)
from crawl_flow.dedup.store import FingerprintStore
from crawl_flow.dedup.text import normalize_text, tokenize

__all__ = [
    "ComputeStats",
    "ContentFingerprint",
    "DedupConfig",
    "FingerprintResult",
    "FingerprintRule",
    "FingerprintRuleConfig",
    "FingerprintService",
    "FingerprintStore",
    "Fingerprinter",
    "MarkStats",
    "RecordData",
    "RecordReader",
    "SimilarPair",
    "SimilarityConfig",
    "UnionFind",
    "cluster_pairs",
    "find_similar_pairs",
    "group_by_exact_hash",
    "hamming_distance",
    "load_dedup_config",
    "max_hamming_for_threshold",
    "normalize_text",
    "simhash",
    "similarity",
    "tokenize",
]
