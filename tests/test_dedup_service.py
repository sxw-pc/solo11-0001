"""去重编排测试：跨源识别、阈值控制、跳过规则、持久化与零侵入。"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from crawl_flow.dedup.config import DedupConfig, SimilarityRules
from crawl_flow.dedup.service import DedupService
from crawl_flow.dedup.store import DedupStore
from crawl_flow.models import Record
from crawl_flow.storage.db import Database

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


def _record(source: str, external_id: str, title: str, content: str) -> Record:
    return Record(
        source=source,
        external_id=external_id,
        title=title,
        content=content,
        url=f"http://example.com/{source}/{external_id}",
    )


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    return tmp_path / "dedup.db"


@pytest.fixture
def database(db_path: Path) -> Iterator[Database]:
    db = Database(db_path)
    db.init()
    try:
        yield db
    finally:
        db.close()


@pytest.fixture
def store(db_path: Path, database: Database) -> Iterator[DedupStore]:
    dedup_store = DedupStore(db_path)
    dedup_store.init()
    try:
        yield dedup_store
    finally:
        dedup_store.close()


def _run(store: DedupStore, config: DedupConfig | None = None, **kwargs: object):
    return DedupService(store, config or DedupConfig()).run(**kwargs)  # type: ignore[arg-type]


def _groups(store: DedupStore):
    return [(group, store.list_members(group.id)) for group in store.list_groups()]


def test_cross_source_exact_duplicates_are_grouped(
    store: DedupStore, database: Database
) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_b", "x9", TITLE, BASE))
    database.upsert_record(_record("src_b", "x10", "英超战报", UNRELATED))

    summary = _run(store)
    assert summary.scanned == 3
    assert summary.computed == 3
    assert summary.groups == 1
    assert summary.marked == 2

    [(group, members)] = _groups(store)
    assert group.match_type == "exact"
    assert group.member_count == 2
    # 跨源：所有关联的来源与条目标识都被标记
    keys = {(m.source, m.external_id) for m in members}
    assert keys == {("src_a", "1"), ("src_b", "x9")}
    assert all(m.similarity == 1.0 for m in members)
    assert sum(1 for m in members if m.is_representative) == 1


def test_same_source_duplicates_are_grouped(
    store: DedupStore, database: Database
) -> None:
    """同源不同 external_id 的相同内容同样被识别。"""

    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_a", "2", TITLE, BASE))
    summary = _run(store)
    assert summary.groups == 1
    [(group, members)] = _groups(store)
    assert group.match_type == "exact"
    assert {(m.source, m.external_id) for m in members} == {("src_a", "1"), ("src_a", "2")}


def test_near_duplicate_marked_above_threshold(
    store: DedupStore, database: Database
) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_b", "2", TITLE, VARIANT))

    summary = _run(store)  # 默认阈值 0.9
    assert summary.groups == 1
    [(group, members)] = _groups(store)
    assert group.match_type == "similar"
    scores = {m.external_id: m.similarity for m in members}
    assert scores["1"] == 1.0  # 代表条目
    assert 0.9 <= scores["2"] < 1.0


def test_below_threshold_is_not_marked(store: DedupStore, database: Database) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_b", "2", "英超战报", UNRELATED))

    summary = _run(store)
    assert summary.groups == 0
    assert summary.marked == 0
    assert _groups(store) == []


def test_threshold_is_configurable(store: DedupStore, database: Database) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_b", "2", TITLE, VARIANT))

    # 阈值调高到 0.99：同一对内容不再被标记（相似度约 0.97）。
    strict = DedupConfig(similarity=SimilarityRules(threshold=0.99))
    summary = _run(store, strict)
    assert summary.groups == 0

    # 阈值调回 0.9：重新标记。
    summary = _run(store, DedupConfig())
    assert summary.groups == 1


def test_short_or_empty_content_is_skipped(
    store: DedupStore, database: Database
) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_a", "2", TITLE, ""))
    database.upsert_record(_record("src_a", "3", TITLE, "太短"))
    database.upsert_record(_record("src_a", "4", TITLE, "太短"))

    summary = _run(store)
    assert summary.scanned == 4
    assert summary.computed == 1
    assert summary.skipped_short == 3
    # 过短内容不生成指纹，也不会互相组成重复组
    assert len(store.list_fingerprints()) == 1
    assert summary.groups == 0


def test_original_records_are_never_modified(
    store: DedupStore, database: Database
) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_b", "2", TITLE, BASE))
    before = [(r.source, r.external_id, r.title, r.content) for r in store.iter_records()]

    _run(store)

    after = [(r.source, r.external_id, r.title, r.content) for r in store.iter_records()]
    assert before == after
    assert database.count_records() == 2


def test_fingerprints_persist_across_runs(store: DedupStore, database: Database) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_b", "2", TITLE, VARIANT))

    first = _run(store)
    assert first.computed == 2
    fingerprints = store.list_fingerprints()
    assert len(fingerprints) == 2

    # 第二轮（模拟任务重启后重跑）：指纹全部复用，不重新计算。
    second = _run(store)
    assert second.computed == 0
    assert second.reused == 2
    assert second.groups == first.groups == 1


def test_rule_change_triggers_batch_recompute(
    store: DedupStore, database: Database
) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    config_v1 = DedupConfig(rule_version="v1")
    first = _run(store, config_v1)
    assert first.computed == 1
    assert store.list_fingerprints()[0].rule_version.startswith("v1+")

    # 规则更新（阈值调整）→ 版本变化 → 历史指纹批量重算。
    config_v2 = DedupConfig(
        rule_version="v2", similarity=SimilarityRules(threshold=0.95)
    )
    second = _run(store, config_v2)
    assert second.computed == 1
    assert second.reused == 0
    rows = store.list_fingerprints()
    assert len(rows) == 1
    assert rows[0].rule_version.startswith("v2+")


def test_force_recomputes_everything(store: DedupStore, database: Database) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    _run(store)
    summary = _run(store, force=True)
    assert summary.computed == 1
    assert summary.reused == 0


def test_orphan_fingerprints_are_pruned(store: DedupStore, database: Database) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_a", "2", TITLE, VARIANT))
    _run(store)
    assert len(store.list_fingerprints()) == 2

    # 模拟 records 中条目被外部清理：指纹表中孤儿被清除（原始数据不受影响）。
    with database.engine.connect() as conn:
        conn.exec_driver_sql("DELETE FROM records WHERE external_id = '2'")
        conn.commit()
    summary = _run(store)
    assert summary.pruned == 1
    assert len(store.list_fingerprints()) == 1


def test_source_filter_only_scans_one_source(
    store: DedupStore, database: Database
) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_b", "2", TITLE, BASE))

    summary = _run(store, source="src_a")
    assert summary.scanned == 1
    assert summary.computed == 1
    # src_b 的指纹未被计算，但其 records 行不受影响
    assert len(store.list_fingerprints()) == 1
    assert database.count_records("src_b") == 1

    # 全量跑后跨源成组
    summary = _run(store)
    assert summary.groups == 1


def test_mixed_group_of_exact_and_similar(store: DedupStore, database: Database) -> None:
    database.upsert_record(_record("src_a", "1", TITLE, BASE))
    database.upsert_record(_record("src_b", "2", TITLE, BASE))  # 与 1 完全重复
    database.upsert_record(_record("src_c", "3", TITLE, VARIANT))  # 与 1 相似

    summary = _run(store)
    assert summary.groups == 1
    [(group, members)] = _groups(store)
    assert group.match_type == "mixed"
    assert len(members) == 3
    by_id = {m.external_id: m for m in members}
    assert by_id["2"].similarity == 1.0
    assert 0.9 <= by_id["3"].similarity < 1.0


def test_fingerprint_throughput_meets_requirement(
    store: DedupStore, database: Database
) -> None:
    """指纹生成（含入库）速度不低于 100 条/秒。"""

    total = 300
    for index in range(total):
        database.upsert_record(
            _record("bulk", str(index), f"标题{index}", f"{BASE}（第{index}期）")
        )
    summary = _run(store)
    assert summary.computed == total
    assert summary.fingerprints_per_second >= 100
