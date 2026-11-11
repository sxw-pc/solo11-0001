"""指纹计算：文本归一化、中文友好分词、精确哈希与 SimHash。

纯标准库实现，不引入分词词典或预训练模型：

- 精确指纹：归一化文本的哈希，相同内容必然得到相同指纹；
- 相似指纹：对「中文按字符 n-gram、拉丁文按单词」的特征做
  tf 加权 SimHash，用汉明距离衡量相似度。
"""

from __future__ import annotations

import hashlib
import unicodedata
from collections import Counter
from dataclasses import dataclass

from crawl_flow.dedup.config import DedupConfig

# 指纹拼接时分隔标题与正文，避免 "ab"+"c" 与 "a"+"bc" 撞哈希。
_FIELD_SEPARATOR = "\x00"


@dataclass(frozen=True)
class Fingerprint:
    """单条记录的指纹结果。"""

    exact_hash: str
    content_hash: str
    title_hash: str
    simhash: int | None
    content_length: int


def normalize_text(text: str, config: DedupConfig) -> str:
    """按配置做指纹前归一化；用于精确指纹与长度判断。"""

    rules = config.normalization
    if rules.nfkc:
        text = unicodedata.normalize("NFKC", text)
    if rules.lowercase:
        text = text.lower()
    if not (rules.remove_whitespace or rules.remove_punctuation):
        return text
    kept: list[str] = []
    for char in text:
        if rules.remove_whitespace and char.isspace():
            continue
        if rules.remove_punctuation and unicodedata.category(char).startswith("P"):
            continue
        kept.append(char)
    return "".join(kept)


def _is_cjk(char: str) -> bool:
    code = ord(char)
    return (
        0x4E00 <= code <= 0x9FFF  # CJK 统一表意文字
        or 0x3400 <= code <= 0x4DBF  # 扩展 A
        or 0xF900 <= code <= 0xFAFF  # 兼容表意文字
        or 0x20000 <= code <= 0x2A6DF  # 扩展 B
    )


def tokenize(text: str, config: DedupConfig) -> list[str]:
    """把文本切成相似度特征：CJK 连续段取字符 n-gram，其余按单词。

    分词前只做 NFKC 与小写化；标点与空白一律视为分隔符（相似度
    关注用字/用词分布，标点差异不应影响内容相似判断）。
    """

    if config.normalization.nfkc:
        text = unicodedata.normalize("NFKC", text)
    if config.normalization.lowercase:
        text = text.lower()

    tokens: list[str] = []
    run: list[str] = []
    run_is_cjk = False
    ngram = config.similarity.cjk_ngram

    def flush() -> None:
        nonlocal run, run_is_cjk
        if not run:
            return
        piece = "".join(run)
        if run_is_cjk:
            if len(piece) <= ngram:
                tokens.append(piece)
            else:
                tokens.extend(piece[i : i + ngram] for i in range(len(piece) - ngram + 1))
        else:
            tokens.append(piece)
        run = []

    for char in text:
        if _is_cjk(char):
            if run and not run_is_cjk:
                flush()
            run_is_cjk = True
            run.append(char)
        elif char.isalnum() and not char.isspace():
            if run and run_is_cjk:
                flush()
            run_is_cjk = False
            run.append(char)
        else:
            flush()
    flush()
    return tokens


def simhash(tokens: list[tuple[str, float]], bits: int) -> int:
    """对加权特征计算 SimHash；相同特征集必然得到相同指纹。"""

    digest_size = bits // 8
    sums = [0.0] * bits
    for token, weight in tokens:
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=digest_size).digest()
        value = int.from_bytes(digest, "big")
        for bit in range(bits):
            if (value >> bit) & 1:
                sums[bit] += weight
            else:
                sums[bit] -= weight
    fingerprint = 0
    for bit in range(bits):
        if sums[bit] > 0:
            fingerprint |= 1 << bit
    return fingerprint


def _hash_text(algorithm: str, text: str) -> str:
    return hashlib.new(algorithm, text.encode("utf-8")).hexdigest()


def compute_fingerprint(
    title: str,
    content: str,
    config: DedupConfig,
) -> Fingerprint | None:
    """计算一条记录的指纹；内容为空或过短时返回 ``None``（不生成指纹）。"""

    normalized_content = normalize_text(content or "", config)
    if len(normalized_content) < config.min_content_length or not normalized_content:
        return None
    normalized_title = normalize_text(title or "", config)

    algorithm = config.exact.algorithm
    content_hash = _hash_text(algorithm, normalized_content)
    title_hash = _hash_text(algorithm, normalized_title)

    exact_parts: list[str] = []
    if config.fields.title:
        exact_parts.append("T:" + normalized_title)
    if config.fields.content:
        exact_parts.append("C:" + normalized_content)
    exact_hash = _hash_text(algorithm, _FIELD_SEPARATOR.join(exact_parts))

    simhash_value: int | None = None
    if config.similarity.enabled:
        weighted: Counter[str] = Counter()
        if config.fields.content:
            weighted.update(tokenize(content or "", config))
        if config.fields.title:
            title_tokens = Counter(tokenize(title or "", config))
            for token, count in title_tokens.items():
                weighted[token] += count * config.similarity.title_weight
        features = [(token, float(weight)) for token, weight in weighted.items()]
        simhash_value = simhash(features, config.similarity.bits)

    return Fingerprint(
        exact_hash=exact_hash,
        content_hash=content_hash,
        title_hash=title_hash,
        simhash=simhash_value,
        content_length=len(normalized_content),
    )
