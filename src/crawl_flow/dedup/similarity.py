"""近似重复检测：SimHash 多视角分带 LSH 召回候选对 + 汉明距离校验 + 并查集聚类。

为什么需要 LSH：全量两两比较是 O(n²)，历史条目上万后不可接受。把 64 位
SimHash 切成若干 band，两个指纹只要在任意一个 band 上完全相同就进入候选
集合，再用真实汉明距离做一次校验，保证输出的每一对都确实满足阈值
（零误判）。单一 band 切分在阈值距离上可能被完全打散，因此采用多视角
分带（主视角 + 细粒度视角取并集）提升召回；视角只影响召回，不影响
「低于阈值绝不标记」的精确性。整个过程无模型、纯计算。
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Hashable, Iterable

from crawl_flow.dedup.fingerprint import SIMHASH_BITS, hamming_distance, similarity

DEFAULT_BANDS = 4
# 多视角分带表：主视角之外再叠加的细粒度 band 切分（都必须能整除 64）。
# 例：主视角 4 段（每段 16 bit）在汉明距离 6 时可能 6 个差异 bit 散到 4 段、
# 每段至少 1 个不同而没有共享 band；叠加 8 段视角（每段 8 bit）后，距离 6
# 最多污染 6 个小段，8 段中至少 2 段完全相同，召回大幅提升。
_EXTRA_VIEWS: dict[int, tuple[int, ...]] = {
    1: (),
    2: (4,),
    4: (8,),
    8: (),
}


class UnionFind:
    """最小并查集：用于把满足相似度的条目合并成连通簇。"""

    def __init__(self, keys: Iterable[Hashable]) -> None:
        self._parent: dict[Hashable, Hashable] = {key: key for key in keys}
        self._rank: dict[Hashable, int] = {key: 0 for key in self._parent}

    def find(self, key: Hashable) -> Hashable:
        parent = self._parent[key]
        if parent != key:
            self._parent[key] = self.find(parent)
        return self._parent[key]

    def union(self, left: Hashable, right: Hashable) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        rank_left = self._rank[root_left]
        rank_right = self._rank[root_right]
        if rank_left < rank_right:
            self._parent[root_left] = root_right
        elif rank_left > rank_right:
            self._parent[root_right] = root_left
        else:
            self._parent[root_right] = root_left
            self._rank[root_left] += 1

    def groups(self) -> list[list[Hashable]]:
        buckets: dict[Hashable, list[Hashable]] = defaultdict(list)
        for key in self._parent:
            buckets[self.find(key)].append(key)
        return [sorted(members) for members in buckets.values()]


@dataclass(frozen=True)
class SimilarPair:
    """通过阈值校验的一对相似条目。"""

    left: Hashable
    right: Hashable
    hamming: int
    similarity: float


def _single_view_band_keys(
    simhash_value: int, *, view: int, bands: int, bits: int
) -> list[tuple[int, int, int]]:
    """把一个 SimHash 按 ``bands`` 段切分，返回 (视角标识, band 序号, 段值)。"""

    if bits % bands != 0:
        raise ValueError(f"位数 {bits} 必须能被 band 数 {bands} 整除")
    width = bits // bands
    mask = (1 << width) - 1
    return [
        (view, index, (simhash_value >> (index * width)) & mask)
        for index in range(bands)
    ]


def _all_band_keys(
    simhash_value: int, *, bands: int, bits: int
) -> list[tuple[int, int, int]]:
    """主视角 + 配置启用的额外细粒度视角的全部 band 键。"""

    keys = _single_view_band_keys(simhash_value, view=bands, bands=bands, bits=bits)
    for extra_bands in _EXTRA_VIEWS.get(bands, ()):
        keys.extend(
            _single_view_band_keys(
                simhash_value, view=extra_bands, bands=extra_bands, bits=bits
            )
        )
    return keys


def find_similar_pairs(
    items: list[tuple[Hashable, int]],
    *,
    max_hamming: int,
    bands: int = DEFAULT_BANDS,
    bits: int = SIMHASH_BITS,
) -> list[SimilarPair]:
    """找出所有汉明距离不超过 ``max_hamming`` 的条目对。

    ``items`` 为 ``(条目键, SimHash)`` 列表。先用多视角 band 倒排召回候选
    对，再逐一校验真实汉明距离，因此返回结果不会出现「低于阈值却被标记」
    的误判；多视角只用于提升召回率。
    """

    if not items or max_hamming < 0:
        return []
    if bands < 1:
        raise ValueError("band 数必须 >= 1")
    if bits % bands != 0:
        raise ValueError(f"位数 {bits} 必须能被 band 数 {bands} 整除")

    inverted: dict[tuple[int, int, int], list[Hashable]] = defaultdict(list)
    for key, simhash_value in items:
        for band_key in _all_band_keys(simhash_value, bands=bands, bits=bits):
            inverted[band_key].append(key)

    simhash_by_key = dict(items)
    seen_pairs: set[tuple[Hashable, Hashable]] = set()
    pairs: list[SimilarPair] = []
    for members in inverted.values():
        if len(members) < 2:
            continue
        for index, left in enumerate(members):
            for right in members[index + 1 :]:
                pair = (left, right) if left < right else (right, left)  # type: ignore[operator]
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)
                distance = hamming_distance(
                    simhash_by_key[left], simhash_by_key[right], bits=bits
                )
                if distance <= max_hamming:
                    pairs.append(
                        SimilarPair(
                            left=pair[0],
                            right=pair[1],
                            hamming=distance,
                            similarity=similarity(
                                simhash_by_key[left], simhash_by_key[right], bits=bits
                            ),
                        )
                    )
    pairs.sort(key=lambda pair: (pair.hamming, pair.left, pair.right))
    return pairs


def cluster_pairs(
    all_keys: Iterable[Hashable], pairs: Iterable[SimilarPair]
) -> list[list[Hashable]]:
    """用并查集把相似对合并成连通簇，只返回成员数 >= 2 的簇。"""

    union_find = UnionFind(all_keys)
    for pair in pairs:
        union_find.union(pair.left, pair.right)
    return [group for group in union_find.groups() if len(group) >= 2]


def group_by_exact_hash(
    items: Iterable[tuple[Hashable, str]],
) -> list[list[Hashable]]:
    """按精确内容哈希分组，返回成员数 >= 2 的组（完全重复）。"""

    buckets: dict[str, list[Hashable]] = defaultdict(list)
    for key, content_hash in items:
        buckets[content_hash].append(key)
    return [sorted(members) for members in buckets.values() if len(members) >= 2]
