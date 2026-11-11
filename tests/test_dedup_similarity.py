"""近似重复检测：LSH 候选召回、阈值零误判、并查集聚类、精确分组。"""

from __future__ import annotations

import pytest

from crawl_flow.dedup.fingerprint import (
    Fingerprinter,
    FingerprintRule,
    hamming_distance,
)
from crawl_flow.dedup.similarity import (
    UnionFind,
    cluster_pairs,
    find_similar_pairs,
    group_by_exact_hash,
)


def _simhashes(texts: list[str]) -> list[tuple[str, int]]:
    fingerprinter = Fingerprinter(FingerprintRule())
    return [
        (f"key-{index}", fingerprinter.fingerprint_content(text).simhash)
        for index, text in enumerate(texts)
    ]


def test_union_find_groups_connected_components() -> None:
    union_find = UnionFind(["a", "b", "c", "d"])
    union_find.union("a", "b")
    union_find.union("b", "c")
    groups = union_find.groups()
    assert ["a", "b", "c"] in groups
    assert ["d"] in groups


def test_exact_hash_grouping() -> None:
    items = [
        (("src_a", "1"), "hash-x"),
        (("src_b", "2"), "hash-x"),  # 跨源同哈希
        (("src_a", "3"), "hash-y"),
        (("src_c", "4"), "hash-y"),
        (("src_a", "5"), "hash-z"),  # 孤立条目不出现
    ]
    groups = group_by_exact_hash(items)
    assert sorted(groups) == sorted([
        [("src_a", "1"), ("src_b", "2")],
        [("src_a", "3"), ("src_c", "4")],
    ])


def test_find_similar_pairs_respects_threshold_no_false_positive() -> None:
    base = (
        "三款开源数据库在同一台测试机上完成读写混合压测，测试包含插入点查与范围查询，"
        "记录吞吐和尾延迟数据，用于后续的性能基线对比分析工作。"
    )
    edited = (
        "三款开源数据库在同一台测试机上完成读写混合压测，测试包含插入点查与范围查询，"
        "记录吞吐和尾延迟数据，用于后续的性能基线对比与评估工作。"
    )
    unrelated = "城东公园的春季花期观测记录了十二种植物，志愿者按周汇总开花株数与游客数量。"
    items = _simhashes([base, edited, unrelated])
    pairs = find_similar_pairs(items, max_hamming=6, bands=4)

    returned = {(pair.left, pair.right) for pair in pairs}
    assert ("key-0", "key-1") in returned  # 高度相似被召回
    assert ("key-0", "key-2") not in returned  # 低于阈值不会被误判
    assert ("key-1", "key-2") not in returned
    # 每一对都必须真实满足距离约束（LSH 只是召回，校验是精确的）
    simhash_by_key = dict(items)
    for pair in pairs:
        assert (
            hamming_distance(simhash_by_key[pair.left], simhash_by_key[pair.right]) <= 6
        )


def test_pairs_empty_input_and_zero_threshold() -> None:
    assert find_similar_pairs([], max_hamming=6) == []
    items = _simhashes([
        "第一条足够长的正文内容，用来验证阈值为零时只有完全相等才会被标记为相似。",
        "第二条足够长的正文内容，用来验证阈值为零时只有完全相等才会被标记为相似。",
    ])
    # max_hamming=0：只有 SimHash 完全相等才成簇
    pairs = find_similar_pairs(items, max_hamming=0, bands=4)
    simhash_by_key = dict(items)
    for pair in pairs:
        assert simhash_by_key[pair.left] == simhash_by_key[pair.right]


def test_cluster_pairs_merges_transitive_relations() -> None:
    keys = ["a", "b", "c", "d"]
    from crawl_flow.dedup.similarity import SimilarPair

    pairs = [
        SimilarPair("a", "b", hamming=3, similarity=0.95),
        SimilarPair("b", "c", hamming=5, similarity=0.92),
    ]
    groups = cluster_pairs(keys, pairs)
    assert groups == [["a", "b", "c"]]  # d 为单点，被过滤


def test_lsh_recall_matches_bruteforce_on_dataset() -> None:
    """LSH 召回结果与 O(n²) 暴力扫描在同一数据集上完全一致。"""

    texts = [
        "城市公园春季花期观测报告显示三月上旬有十二种植物进入盛花期，游客数量稳步增长。",
        "城市公园春季花期观测报告显示三月上旬有十二种植物进入盛花期，游客数量持续增长。",
        "开源数据库性能基准测试汇总了三款产品的读写吞吐、点查延迟与范围查询表现数据。",
        "开源数据库性能基准测试汇总了三款产品的读写吞吐、点查延迟与范围查询表现指标。",
        "社区图书馆本月新增三百册中文图书，涵盖文学、科技与少儿读物三个主要类别。",
        "轨道交通晚高峰客流同比下降八个百分点，换乘通道的拥堵情况得到了明显缓解。",
        "农民专业合作社试水直播带货，本地特色农产品的线上销售额环比增长了四成。",
    ]
    items = _simhashes(texts)
    max_hamming = 6

    lsh_pairs = {
        tuple(sorted((pair.left, pair.right)))
        for pair in find_similar_pairs(items, max_hamming=max_hamming, bands=4)
    }
    brute_pairs: set[tuple[str, str]] = set()
    for index, (left, left_hash) in enumerate(items):
        for right, right_hash in items[index + 1 :]:
            if hamming_distance(left_hash, right_hash) <= max_hamming:
                brute_pairs.add((left, right))

    assert lsh_pairs == brute_pairs
    # 该数据集至少能识别出两组近似重复
    assert len(brute_pairs) >= 2


def test_invalid_bands_rejected() -> None:
    with pytest.raises(ValueError):
        find_similar_pairs(_simhashes(["内容足够长的一条测试文本，用来触发 bands 校验逻辑。"]), max_hamming=6, bands=3)
