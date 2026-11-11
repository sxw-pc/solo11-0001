"""文本归一化、分词与指纹基础特性测试。"""

from __future__ import annotations

import pytest

from crawl_flow.dedup.fingerprint import (
    Fingerprinter,
    FingerprintRule,
    hamming_distance,
    max_hamming_for_threshold,
    simhash,
    similarity,
)
from crawl_flow.dedup.text import normalize_text, tokenize

# ---------------------------------------------------------------------
# 归一化
# ---------------------------------------------------------------------

def test_normalize_nfkc_fullwidth_and_whitespace() -> None:
    # 全角字母/数字经 NFKC 转半角；全角空格与连续空白折叠。
    assert normalize_text("Ｈｅｌｌｏ　　World") == "hello world"
    # CJK 字符之间的空白无语义，归一化时删除；拉丁词之间的空格保留。
    assert normalize_text("第一段\r\n  第二段\t\t第三段") == "第一段第二段第三段"
    assert normalize_text("hello   world") == "hello world"


def test_normalize_strips_punctuation_and_symbols() -> None:
    text = "你好，世界！这是一条测试内容。"
    normalized = normalize_text(text)
    assert "，" not in normalized and "！" not in normalized and "。" not in normalized
    assert normalized.startswith("你好")


def test_normalize_keeps_punctuation_when_disabled() -> None:
    # NFKC 会把全角标点规范成半角，但标点不会被剥离
    normalized = normalize_text("你好，世界！", strip_punctuation=False)
    assert "," in normalized and "!" in normalized
    assert normalized == "你好,世界!"


def test_normalize_none_and_empty() -> None:
    assert normalize_text(None) == ""
    assert normalize_text("   \n　") == ""


# ---------------------------------------------------------------------
# 中文友好分词
# ---------------------------------------------------------------------

def test_tokenize_chinese_unigram_and_bigram() -> None:
    tokens = tokenize("天气很好")
    # unigram 全部保留
    assert ["天", "气", "很", "好"] == [token for token in tokens if len(token) == 1]
    # 相邻 bigram
    assert {"天气", "气很", "很好"}.issubset(set(tokens))


def test_tokenize_latin_words_and_numbers() -> None:
    tokens = tokenize("python3 与 sql 2024")
    assert "python3" in tokens
    assert "sql" in tokens
    assert "2024" in tokens
    assert "与" in tokens


def test_tokenize_bigram_can_be_disabled() -> None:
    tokens = tokenize("天气很好", cjk_bigram=False)
    assert tokens == ["天", "气", "很", "好"]


def test_tokenize_japanese_kana_treated_as_cjk() -> None:
    tokens = tokenize("データベース")
    # 假名按 CJK 字符逐字切分并生成 bigram
    assert len(tokens) >= 5
    assert "デー" in tokens


# ---------------------------------------------------------------------
# 精确指纹：完全相同内容指纹完全一致（100% 确定性）
# ---------------------------------------------------------------------

@pytest.fixture
def fingerprinter() -> Fingerprinter:
    return Fingerprinter(FingerprintRule())


def test_identical_content_produces_identical_fingerprint(fingerprinter: Fingerprinter) -> None:
    content = "这是一段足够长的正文内容，用于验证完全相同的输入产生完全一致的指纹。"
    first = fingerprinter.fingerprint_content(content)
    second = fingerprinter.fingerprint_content(content)
    assert first.content_hash is not None
    assert first.content_hash == second.content_hash
    assert first.simhash_hex == second.simhash_hex
    assert first.simhash == second.simhash


def test_cross_source_surface_differences_normalize_to_same_fingerprint(
    fingerprinter: Fingerprinter,
) -> None:
    # 来源不同导致的空白、全角半角、标点形态、大小写差异不应改变指纹：
    # NFKC 统一全角/半角，strip_punctuation 再彻底剥离标点，只剩文字与数字。
    left = "开源数据库在同一台测试机上完成压测，包含插入与点查。"
    right = "开源数据库 在同一台测试机上完成压测,  包含插入与点查"
    assert (
        fingerprinter.fingerprint_content(left).content_hash
        == fingerprinter.fingerprint_content(right).content_hash
    )
    # 大小写差异同样被归一化吸收
    upper = "OpenSource DATABASE 性能压测报告正文内容，足够长以跳过最短长度限制。"
    lower = "opensource database 性能压测报告正文内容，足够长以跳过最短长度限制。"
    assert (
        fingerprinter.fingerprint_content(upper).content_hash
        == fingerprinter.fingerprint_content(lower).content_hash
    )


def test_different_content_produces_different_fingerprint(
    fingerprinter: Fingerprinter,
) -> None:
    first = fingerprinter.fingerprint_content("完全不同的第一条正文内容，谈论的是数据库压测与吞吐。")
    second = fingerprinter.fingerprint_content("另一条毫不相关的正文内容，主题是城市公园的春季花期观测。")
    assert first.content_hash != second.content_hash
    assert first.simhash != second.simhash


def test_title_fingerprint(fingerprinter: Fingerprinter) -> None:
    assert (
        fingerprinter.fingerprint_title("开源数据库性能基准测试汇总")
        == fingerprinter.fingerprint_title("开源数据库性能基准测试汇总")
    )
    assert fingerprinter.fingerprint_title("a") is None  # 低于 min_title_length
    assert fingerprinter.fingerprint_title("") is None
    assert fingerprinter.fingerprint_title(None) is None


# ---------------------------------------------------------------------
# 空/短内容跳过，不生成无效指纹
# ---------------------------------------------------------------------

def test_empty_and_short_content_skipped() -> None:
    fingerprinter = Fingerprinter(FingerprintRule(min_content_length=20))
    empty = fingerprinter.fingerprint_content("")
    assert empty.skipped and empty.content_hash is None and empty.simhash is None
    assert empty.skip_reason == "内容为空"

    short = fingerprinter.fingerprint_content("短内容")
    assert short.skipped and short.content_hash is None and short.simhash_hex is None
    assert "低于最小阈值" in (short.skip_reason or "")

    whitespace_only = fingerprinter.fingerprint_content("  \n 　 ")
    assert whitespace_only.skipped


def test_configurable_min_length_changes_skip_boundary() -> None:
    strict = Fingerprinter(FingerprintRule(min_content_length=5))
    result = strict.fingerprint_content("一二三四五六七八")
    assert not result.skipped and result.content_hash is not None


# ---------------------------------------------------------------------
# SimHash 与相似度
# ---------------------------------------------------------------------

def test_simhash_deterministic_and_order_independent() -> None:
    tokens_a = ["数据", "数据库", "性能", "测试"]
    tokens_b = list(reversed(tokens_a))
    assert simhash(tokens_a) == simhash(tokens_b)
    assert simhash([]) == 0


def test_simhash_small_edit_keeps_high_similarity() -> None:
    base_text = (
        "三款开源数据库在同一台测试机上完成读写混合压测，测试包含插入点查与范围查询，"
        "记录吞吐和尾延迟数据，用于后续的性能基线对比分析工作。"
    )
    edited_text = (
        "三款开源数据库在同一台测试机上完成读写混合压测，测试包含插入点查与范围查询，"
        "记录吞吐和尾延迟数据，用于后续的性能基线对比与评估工作。"
    )
    fingerprinter = Fingerprinter(FingerprintRule())
    base = fingerprinter.fingerprint_content(base_text)
    edited = fingerprinter.fingerprint_content(edited_text)
    score = similarity(base.simhash, edited.simhash)
    assert score >= 0.9
    assert hamming_distance(base.simhash, edited.simhash) <= max_hamming_for_threshold(0.9)


def test_unrelated_text_low_similarity(fingerprinter: Fingerprinter) -> None:
    tech = fingerprinter.fingerprint_content(
        "数据库压测显示写入吞吐随并发线程数增长，在达到临界点之后尾延迟开始明显上升。"
    )
    park = fingerprinter.fingerprint_content(
        "志愿者在城东公园记录到十二种进入花期的植物，观测数据按周汇总包含开花株数。"
    )
    assert similarity(tech.simhash, park.simhash) < 0.9


def test_threshold_to_hamming_conversion() -> None:
    # 阈值 0.9 → 距离 <= floor(0.1 * 64) = 6
    assert max_hamming_for_threshold(0.9) == 6
    assert max_hamming_for_threshold(1.0) == 0
    assert max_hamming_for_threshold(0.0) == 64
    with pytest.raises(ValueError):
        max_hamming_for_threshold(1.5)


# ---------------------------------------------------------------------
# 规则可配置
# ---------------------------------------------------------------------

def test_rule_rejects_invalid_algorithm() -> None:
    with pytest.raises(ValueError):
        FingerprintRule(hash_algorithm="nope")


def test_rule_version_changes_signature() -> None:
    v1 = FingerprintRule(version=1)
    v2 = FingerprintRule(version=2)
    assert v1.signature() != v2.signature()


def test_hash_algorithm_configurable() -> None:
    sha1 = Fingerprinter(FingerprintRule(hash_algorithm="sha1"))
    result = sha1.fingerprint_content("一段用于验证不同哈希算法可配置的足够长的正文内容。")
    assert result.content_hash is not None and len(result.content_hash) == 40  # sha1 hex 长度
