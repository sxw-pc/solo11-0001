"""相似度计算：汉明距离、鸽巢分带索引与重复组构建。

分带索引利用鸽巢原理：把 ``bits`` 位指纹切成 ``max_distance + 1`` 段，
汉明距离 ≤ ``max_distance`` 的两个指纹至少有一段完全相同，因此候选
枚举对阈值内的相似对是 100% 召回的，再逐一精确验算，不漏判、不误判。
"""

from __future__ import annotations

from dataclasses import dataclass, field


def hamming_distance(left: int, right: int) -> int:
    """两个 SimHash 指纹的汉明距离。"""

    return (left ^ right).bit_count()


def similarity_score(left: int, right: int, bits: int) -> float:
    """由汉明距离换算相似度，取值 [0, 1]。"""

    return 1.0 - hamming_distance(left, right) / bits


def max_distance_for_threshold(threshold: float, bits: int) -> int:
    """相似度 ≥ threshold 等价于汉明距离 ≤ 本函数返回值。"""

    # 加微小 epsilon 抵消浮点误差（如 0.1 的表示误差）。
    return int((1.0 - threshold) * bits + 1e-9)


class BandIndex:
    """SimHash 分带索引：保证距离 ≤ max_distance 的指纹必然成为候选。"""

    def __init__(self, bits: int, max_distance: int) -> None:
        self.bits = bits
        self.band_count = min(max_distance + 1, bits)
        self.band_width = (bits + self.band_count - 1) // self.band_count
        self._buckets: dict[tuple[int, int], list[int]] = {}

    def _band_keys(self, fingerprint: int) -> list[tuple[int, int]]:
        mask = (1 << self.band_width) - 1
        return [
            (band, (fingerprint >> (band * self.band_width)) & mask)
            for band in range(self.band_count)
        ]

    def add(self, item_id: int, fingerprint: int) -> None:
        for key in self._band_keys(fingerprint):
            self._buckets.setdefault(key, []).append(item_id)

    def candidates(self, fingerprint: int) -> set[int]:
        found: set[int] = set()
        for key in self._band_keys(fingerprint):
            found.update(self._buckets.get(key, ()))
        return found


class UnionFind:
    """按内部编号合并重复/相似条目。"""

    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, item: int) -> int:
        parent = self.parent
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left != root_right:
            self.parent[root_right] = root_left


@dataclass
class GroupMember:
    """重复组内的一条记录标记。"""

    source: str
    external_id: str
    similarity: float
    is_representative: bool


@dataclass
class DuplicateGroup:
    """一个重复/相似内容组（≥2 条记录）。"""

    match_type: str  # exact / similar / mixed
    members: list[GroupMember] = field(default_factory=list)


@dataclass
class FingerprintEntry:
    """参与分组的最小指纹信息（与存储层解耦）。"""

    source: str
    external_id: str
    exact_hash: str
    simhash: int | None
    ordinal: int  # 用于挑选代表：值越小越「原始」


def build_duplicate_groups(
    entries: list[FingerprintEntry],
    *,
    bits: int,
    threshold: float,
    similarity_enabled: bool = True,
) -> list[DuplicateGroup]:
    """把指纹列表聚成重复组；只有 ≥2 条记录的组会被返回。

    完全重复（exact_hash 相同）必然同组、相似度记 1.0；SimHash
    相似度 ≥ threshold 的记录并入同组并记录与代表的实际相似度。
    """

    if len(entries) < 2:
        return []

    union_find = UnionFind(len(entries))

    exact_buckets: dict[str, list[int]] = {}
    for index, entry in enumerate(entries):
        exact_buckets.setdefault(entry.exact_hash, []).append(index)
    for bucket in exact_buckets.values():
        for index in bucket[1:]:
            union_find.union(bucket[0], index)

    simhash_entries = [
        (index, entry.simhash)
        for index, entry in enumerate(entries)
        if entry.simhash is not None
    ]
    if similarity_enabled and len(simhash_entries) >= 2:
        max_distance = max_distance_for_threshold(threshold, bits)
        index = BandIndex(bits, max_distance)
        for position, (_, fingerprint) in enumerate(simhash_entries):
            assert fingerprint is not None
            for other_position in index.candidates(fingerprint):
                other_fingerprint = simhash_entries[other_position][1]
                assert other_fingerprint is not None
                if hamming_distance(fingerprint, other_fingerprint) <= max_distance:
                    union_find.union(
                        simhash_entries[position][0], simhash_entries[other_position][0]
                    )
            index.add(position, fingerprint)

    clusters: dict[int, list[int]] = {}
    for position in range(len(entries)):
        clusters.setdefault(union_find.find(position), []).append(position)

    groups: list[DuplicateGroup] = []
    for cluster in clusters.values():
        if len(cluster) < 2:
            continue
        representative = min(cluster, key=lambda i: (entries[i].ordinal, i))
        rep_entry = entries[representative]
        exact_hashes = {entries[i].exact_hash for i in cluster}
        if len(exact_hashes) == 1:
            match_type = "exact"
        elif len(exact_hashes) == len(cluster):
            match_type = "similar"
        else:
            match_type = "mixed"

        members: list[GroupMember] = []
        for position in sorted(cluster, key=lambda i: (entries[i].source, entries[i].external_id)):
            entry = entries[position]
            if entry.exact_hash == rep_entry.exact_hash:
                score = 1.0
            elif (
                similarity_enabled
                and entry.simhash is not None
                and rep_entry.simhash is not None
            ):
                score = similarity_score(entry.simhash, rep_entry.simhash, bits)
            else:
                score = 1.0
            members.append(
                GroupMember(
                    source=entry.source,
                    external_id=entry.external_id,
                    similarity=round(score, 6),
                    is_representative=position == representative,
                )
            )
        groups.append(DuplicateGroup(match_type=match_type, members=members))

    groups.sort(key=lambda group: (group.members[0].source, group.members[0].external_id))
    return groups
