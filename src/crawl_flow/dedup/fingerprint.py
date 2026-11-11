"""内容指纹：精确哈希（SHA-256 等）+ 近似指纹（64 位 SimHash）。

精确指纹保证「完全相同（归一化后）的正文 → 完全一致的指纹」，用于
100% 准确的同源/跨源完全重复识别；SimHash 配合汉明距离用于高度相似
但非完全重复内容的识别。两种算法均为纯标准库实现，无模型依赖。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from crawl_flow.dedup.text import normalize_text, tokenize

SIMHASH_BITS = 64
SUPPORTED_HASH_ALGORITHMS = ("sha256", "sha1", "md5", "blake2b")


@dataclass(frozen=True)
class FingerprintRule:
    """指纹生成规则。

    ``version`` 写入每条指纹记录：规则调整后递增版本号，重算时与历史
    指纹区分，避免新旧规则产生的指纹混用。
    """

    version: int = 1
    hash_algorithm: str = "sha256"
    lowercase: bool = True
    strip_punctuation: bool = True
    cjk_bigram: bool = True
    min_content_length: int = 20
    min_title_length: int = 2
    simhash_bits: int = SIMHASH_BITS

    def __post_init__(self) -> None:
        if self.hash_algorithm not in SUPPORTED_HASH_ALGORITHMS:
            raise ValueError(
                f"不支持的哈希算法 {self.hash_algorithm!r}，"
                f"可选: {', '.join(SUPPORTED_HASH_ALGORITHMS)}"
            )
        if self.version < 1:
            raise ValueError("规则版本号 version 必须 >= 1")
        if self.min_content_length < 0 or self.min_title_length < 0:
            raise ValueError("最小长度阈值不能为负数")
        if self.simhash_bits != SIMHASH_BITS:
            raise ValueError(f"当前仅支持 {SIMHASH_BITS} 位 SimHash")

    def signature(self) -> str:
        """规则的稳定文本签名，便于日志与排查。"""

        return (
            f"v{self.version}|{self.hash_algorithm}|lowercase={self.lowercase}|"
            f"strip_punct={self.strip_punctuation}|cjk_bigram={self.cjk_bigram}|"
            f"min_content={self.min_content_length}|min_title={self.min_title_length}"
        )


@dataclass(frozen=True)
class ContentFingerprint:
    """单条内容的计算结果；为空/过短时对应字段为 ``None`` 并给出原因。"""

    content_normalized: str
    content_hash: str | None
    simhash: int | None
    simhash_hex: str | None
    title_hash: str | None
    skipped: bool
    skip_reason: str | None


def _digest(algorithm: str, text: str) -> str:
    return hashlib.new(algorithm, text.encode("utf-8")).hexdigest()


def _token_hash64(token: str) -> int:
    """对单个 token 取确定性 64 位哈希（blake2b，截断到 64 位）。"""

    digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=False)


def simhash(tokens: list[str], *, bits: int = SIMHASH_BITS) -> int:
    """根据带权 token 列表计算 SimHash。

    权重按 token 在本文内的出现频次给定（频次即词频 tf），无需外部语料。
    重复 token 只哈希一次，累加时乘以频次，保证结果与 token 顺序无关、
    仅与词频分布有关——同一内容无论出现在哪个源，SimHash 都相同。
    """

    if not tokens:
        return 0
    weights: dict[str, int] = {}
    for token in tokens:
        weights[token] = weights.get(token, 0) + 1

    accumulator = [0] * bits
    for token, weight in weights.items():
        token_hash = _token_hash64(token)
        for index in range(bits):
            if token_hash >> index & 1:
                accumulator[index] += weight
            else:
                accumulator[index] -= weight

    result = 0
    for index, value in enumerate(accumulator):
        if value > 0:
            result |= 1 << index
    return result


def hamming_distance(left: int, right: int, *, bits: int = SIMHASH_BITS) -> int:
    """两个等长指纹的汉明距离（不同 bit 的数量）。"""

    return (left ^ right).bit_count() if hasattr(int, "bit_count") else _bit_count(
        left ^ right, bits
    )


def _bit_count(value: int, bits: int) -> int:  # pragma: no cover - 旧版本兼容
    return sum((value >> index) & 1 for index in range(bits))


def similarity(left: int, right: int, *, bits: int = SIMHASH_BITS) -> float:
    """SimHash 相似度，取值 [0, 1]：``1 - 汉明距离 / 位数``。"""

    return 1.0 - hamming_distance(left, right, bits=bits) / bits


def max_hamming_for_threshold(
    threshold: float, *, bits: int = SIMHASH_BITS
) -> int:
    """把相似度阈值换算成允许的最大汉明距离。

    相似度 >= 阈值 ⇔ 距离 <= floor((1 - threshold) * bits)，向下取整保证
    低于阈值的内容绝不会被误判为相似。
    """

    if not 0.0 <= threshold <= 1.0:
        raise ValueError("相似度阈值必须在 [0, 1] 区间内")
    return int((1.0 - threshold) * bits + 1e-12)


class Fingerprinter:
    """按 :class:`FingerprintRule` 计算标题与正文指纹。"""

    def __init__(self, rule: FingerprintRule | None = None) -> None:
        self.rule = rule or FingerprintRule()

    def normalize(self, text: str | None) -> str:
        return normalize_text(text, strip_punctuation=self.rule.strip_punctuation)

    def fingerprint_content(self, content: str | None) -> ContentFingerprint:
        """计算正文指纹；空或长度低于阈值时跳过，不生成无效指纹。"""

        normalized = self.normalize(content)
        # 归一化后按字符数判断长度（空白已折叠），跨源空白差异不影响判定。
        if not normalized:
            return self._skipped(normalized, "内容为空")
        if len(normalized) < self.rule.min_content_length:
            return self._skipped(normalized, f"内容长度 {len(normalized)} 低于最小阈值")

        content_hash = _digest(self.rule.hash_algorithm, normalized)
        tokens = tokenize(normalized, cjk_bigram=self.rule.cjk_bigram)
        simhash_value = simhash(tokens, bits=self.rule.simhash_bits)
        return ContentFingerprint(
            content_normalized=normalized,
            content_hash=content_hash,
            simhash=simhash_value,
            simhash_hex=f"{simhash_value:0{self.rule.simhash_bits // 4}x}",
            title_hash=None,
            skipped=False,
            skip_reason=None,
        )

    def fingerprint_title(self, title: str | None) -> str | None:
        """标题只生成精确哈希；为空或过短时返回 ``None``。"""

        normalized = self.normalize(title)
        if not normalized or len(normalized) < self.rule.min_title_length:
            return None
        return _digest(self.rule.hash_algorithm, normalized)

    def fingerprint(self, *, title: str | None, content: str | None) -> ContentFingerprint:
        """同时计算标题与正文指纹（数据库落库用的组合入口）。"""

        result = self.fingerprint_content(content)
        title_hash = self.fingerprint_title(title)
        return ContentFingerprint(
            content_normalized=result.content_normalized,
            content_hash=result.content_hash,
            simhash=result.simhash,
            simhash_hex=result.simhash_hex,
            title_hash=title_hash,
            skipped=result.skipped,
            skip_reason=result.skip_reason,
        )

    @staticmethod
    def _skipped(normalized: str, reason: str) -> ContentFingerprint:
        return ContentFingerprint(
            content_normalized=normalized,
            content_hash=None,
            simhash=None,
            simhash_hex=None,
            title_hash=None,
            skipped=True,
            skip_reason=reason,
        )
