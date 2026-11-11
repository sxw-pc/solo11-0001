"""指纹计算单元测试：归一化、分词、精确指纹、SimHash 与分带索引。"""

from __future__ import annotations

import random

import pytest

from crawl_flow.dedup.config import DedupConfig, NormalizationRules, SimilarityRules
from crawl_flow.dedup.fingerprint import compute_fingerprint, normalize_text, tokenize
from crawl_flow.dedup.similarity import (
    BandIndex,
    hamming_distance,
    max_distance_for_threshold,
    similarity_score,
)

BASE = (
    "近日，某市发展改革委发布最新一轮新能源汽车购置补贴政策，明确对符合条件的购车用户"
    "给予每辆最高两万元的一次性补贴，政策自下个月一日起正式实施，有效期至今年年底。"
)
VARIANT = (
    "日前，某市发展改革委发布最新一轮新能源汽车购置补贴政策，明确对符合条件的购车用户"
    "给予每辆最高三万元的一次性补贴，政策自下个月一日起正式实施，有效期至今年年底。"
)
UNRELATED = (
    "英格兰足球超级联赛本轮焦点战在雨中进行，主队凭借下半场的一次角球配合头球破门，"
    "最终一比零小胜对手，积分榜排名升至第三位。"
)
TITLE = "某市发布新能源汽车购置补贴政策"


def test_identical_content_produces_identical_fingerprint() -> None:
    config = DedupConfig()
    first = compute_fingerprint(TITLE, BASE, config)
    second = compute_fingerprint(TITLE, BASE, config)
    assert first is not None and second is not None
    assert first.exact_hash == second.exact_hash
    assert first.content_hash == second.content_hash
    assert first.simhash == second.simhash


def test_identical_content_across_field_styles_is_equal() -> None:
    """空白、换行、标点、全角、大小写差异不影响精确指纹。"""

    config = DedupConfig()
    reference = compute_fingerprint(TITLE, BASE, config)
    assert reference is not None

    restyled = (
        "近日 ， 某市发展改革委发布最新一轮新能源汽车购置补贴政策 ，\n"
        "明确对符合条件的购车用户给予每辆最高两万元的一次性补贴　，\n"
        "政策自下个月一日起正式实施，有效期至今年年底 。"
    )
    assert compute_fingerprint(TITLE, restyled, config).exact_hash == reference.exact_hash  # type: ignore[union-attr]

    fullwidth = "ＡＢＣ新能源补贴１２３，这是一段用于验证全角与半角差异的正文内容。"
    halfwidth = "abc新能源补贴123这是一段用于验证全角与半角差异的正文内容"
    assert compute_fingerprint("t", fullwidth, config).exact_hash == compute_fingerprint("t", halfwidth, config).exact_hash  # type: ignore[union-attr]


def test_different_content_produces_different_fingerprint() -> None:
    config = DedupConfig()
    first = compute_fingerprint(TITLE, BASE, config)
    second = compute_fingerprint(TITLE, VARIANT, config)
    third = compute_fingerprint("英超战报", UNRELATED, config)
    assert first is not None and second is not None and third is not None
    assert first.exact_hash != second.exact_hash
    assert first.exact_hash != third.exact_hash
    assert second.exact_hash != third.exact_hash


def test_title_participates_in_exact_fingerprint() -> None:
    config = DedupConfig()
    first = compute_fingerprint("标题甲", BASE, config)
    second = compute_fingerprint("标题乙", BASE, config)
    assert first is not None and second is not None
    assert first.content_hash == second.content_hash
    assert first.exact_hash != second.exact_hash


def test_empty_or_short_content_is_skipped() -> None:
    config = DedupConfig()
    assert compute_fingerprint(TITLE, "", config) is None
    assert compute_fingerprint(TITLE, "   ", config) is None
    assert compute_fingerprint(TITLE, "太短", config) is None
    # 归一化后不足 min_content_length 也跳过（标点空白不计入长度）。
    assert compute_fingerprint(TITLE, "，。、　……", config) is None


def test_min_content_length_is_configurable() -> None:
    config = DedupConfig(min_content_length=2)
    assert compute_fingerprint(TITLE, "你好", config) is not None
    strict = DedupConfig(min_content_length=100)
    assert compute_fingerprint(TITLE, BASE, strict) is None


def test_normalization_rules_are_configurable() -> None:
    content = "Hello，World 这是一段足够长的用于测试标点开关的正文内容。"
    default = compute_fingerprint("t", content, DedupConfig())
    keep_punct = compute_fingerprint(
        "t",
        content,
        DedupConfig(normalization=NormalizationRules(remove_punctuation=False)),
    )
    assert default is not None and keep_punct is not None
    assert default.content_hash != keep_punct.content_hash

    no_punct_variant = compute_fingerprint(
        "t",
        "HelloWorld 这是一段足够长的用于测试标点开关的正文内容",
        DedupConfig(normalization=NormalizationRules(remove_punctuation=False)),
    )
    assert no_punct_variant is not None
    assert no_punct_variant.content_hash != keep_punct.content_hash


def test_tokenize_uses_cjk_ngrams_and_latin_words() -> None:
    config = DedupConfig()
    tokens = tokenize("新能源汽车补贴 policy 2024", config)
    assert "新能" in tokens and "能源" in tokens  # 中文 bigram
    assert "policy" in tokens and "2024" in tokens  # 拉丁/数字整词
    # 单个汉字不足 bigram 时保留单字
    assert tokenize("好", config) == ["好"]


def test_simhash_similarity_for_chinese_text() -> None:
    config = DedupConfig()
    base = compute_fingerprint(TITLE, BASE, config)
    variant = compute_fingerprint(TITLE, VARIANT, config)
    unrelated = compute_fingerprint("英超战报", UNRELATED, config)
    assert base is not None and variant is not None and unrelated is not None
    assert base.simhash is not None and variant.simhash is not None
    assert unrelated.simhash is not None

    assert similarity_score(base.simhash, base.simhash, 64) == 1.0
    near = similarity_score(base.simhash, variant.simhash, 64)
    far = similarity_score(base.simhash, unrelated.simhash, 64)
    assert near >= 0.9  # 小幅改写 → 高于默认阈值
    assert far < 0.9  # 无关内容 → 低于默认阈值，不会误判


def test_hamming_and_threshold_math() -> None:
    assert hamming_distance(0b1010, 0b1001) == 2
    assert similarity_score(0, 0, 64) == 1.0
    assert max_distance_for_threshold(0.9, 64) == 6
    assert max_distance_for_threshold(1.0, 64) == 0
    # 距离 6 → 相似度 0.90625 ≥ 0.9；距离 7 → 0.890625 < 0.9
    assert similarity_score(0, 0b111111, 64) >= 0.9
    assert similarity_score(0, 0b1111111, 64) < 0.9


def test_band_index_recalls_all_pairs_within_distance() -> None:
    """鸽巢分带：距离 ≤ k 的指纹必然成为候选（100% 召回）。"""

    rng = random.Random(2026)
    bits, k = 64, 6
    index = BandIndex(bits, k)
    base = rng.getrandbits(bits)
    index.add(0, base)
    for item in range(1, 50):
        mutated = base
        for bit in rng.sample(range(bits), k):  # 恰好翻转 k 位
            mutated ^= 1 << bit
        index.add(item, mutated)

    candidates = index.candidates(base)
    assert set(range(50)) <= candidates  # 全部召回，无一遗漏


def test_similarity_disabled_produces_no_simhash() -> None:
    config = DedupConfig(similarity=SimilarityRules(enabled=False))
    fingerprint = compute_fingerprint(TITLE, BASE, config)
    assert fingerprint is not None
    assert fingerprint.simhash is None
    assert fingerprint.exact_hash  # 精确指纹仍正常生成


def test_normalize_text_without_optional_steps() -> None:
    config = DedupConfig(
        normalization=NormalizationRules(
            nfkc=False, lowercase=False, remove_whitespace=False, remove_punctuation=False
        )
    )
    assert normalize_text("Ａb ，c", config) == "Ａb ，c"


@pytest.mark.parametrize("bits", [16, 32, 64, 128])
def test_simhash_supports_configurable_bits(bits: int) -> None:
    config = DedupConfig(similarity=SimilarityRules(bits=bits))
    fingerprint = compute_fingerprint(TITLE, BASE, config)
    assert fingerprint is not None
    assert fingerprint.simhash is not None
    assert fingerprint.simhash < 1 << bits
