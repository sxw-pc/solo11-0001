"""指纹服务与独立持久化测试：跨源识别、阈值标记、规则重算、零侵入。"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import create_engine, func, inspect, select
from sqlalchemy.orm import Session

from crawl_flow.dedup.config import DedupConfig
from crawl_flow.dedup.models import (
    DedupGroupMemberRow,
    DedupGroupRow,
)
from crawl_flow.dedup.reader import RecordData, RecordReader
from crawl_flow.dedup.service import FingerprintService
from crawl_flow.dedup.store import FingerprintStore
from crawl_flow.models import Base, RecordRow

TECH_CONTENT = (
    "三款开源数据库在同一台测试机上完成读写混合压测，测试包含插入、点查与范围查询，"
    "记录吞吐和尾延迟，用于后续的性能基线对比分析工作。"
)
TECH_CONTENT_EDITED = (
    "三款开源数据库在同一台测试机上完成读写混合压测，测试包含插入、点查与范围查询，"
    "记录吞吐和尾延迟，用于后续的性能基线对比与评估分析工作。"
)
PARK_CONTENT = (
    "三月上旬志愿者在城东公园记录到十二种进入花期的植物，观测数据按周汇总，"
    "包含开花株数、游客数量与天气备注信息。"
)
LIBRARY_CONTENT = (
    "社区图书馆本月新增三百册中文图书，涵盖文学、科技与少儿读物三个主要类别，"
    "周末还将举办两场面向亲子家庭的阅读推广活动。"
)


def _add_record(session: Session, source: str, external_id: str, title: str, content: str) -> None:
    session.add(
        RecordRow(
            source=source,
            external_id=external_id,
            title=title,
            category=None,
            tags=[],
            published_at=None,
            content=content,
            url=f"https://example.test/{source}/{external_id}",
            fetched_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
    )


@pytest.fixture
def main_db_path(tmp_path: Path) -> Path:
    path = tmp_path / "crawl.db"
    engine = create_engine(f"sqlite:///{path}", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        # 同源完全重复（不同 external_id、同内容）
        _add_record(session, "src_a", "a-1", "数据库压测报告", TECH_CONTENT)
        # 跨源完全重复（不同源、不同 ID、同内容，仅空白差异）
        _add_record(session, "src_b", "b-9", "数据库压测报告转载", " " + TECH_CONTENT)
        # 跨源近似重复（局部改写）
        _add_record(session, "src_c", "c-3", "数据库压测报告改写版", TECH_CONTENT_EDITED)
        # 无关内容
        _add_record(session, "src_a", "a-2", "公园花期观测", PARK_CONTENT)
        _add_record(session, "src_c", "c-4", "图书馆新书通报", LIBRARY_CONTENT)
        # 空内容：跳过指纹
        _add_record(session, "src_a", "a-3", "空正文条目", "")
        # 过短内容：跳过指纹
        _add_record(session, "src_b", "b-1", "短正文条目", "短")
        session.commit()
    engine.dispose()
    return path


@pytest.fixture
def setup(tmp_path: Path, main_db_path: Path):
    fingerprint_path = tmp_path / "fingerprints.db"
    config = DedupConfig.model_validate(
        {"fingerprint_db": str(fingerprint_path), "similarity": {"threshold": 0.9, "bands": 4}}
    )
    store = FingerprintStore(fingerprint_path)
    store.init()
    service = FingerprintService(store, config)
    reader = RecordReader(main_db_path)
    yield config, store, service, reader, main_db_path, fingerprint_path
    store.close()
    reader.close()


def test_persistence_and_skip_short_content(setup) -> None:
    _, store, service, reader, _, _ = setup
    stats = service.compute_fingerprints(reader.iter_records())

    assert stats.total == 7
    assert stats.skipped == 2
    assert stats.written == 5
    assert store.count_fingerprints() == 7
    assert store.count_skipped() == 2

    skipped_row = store.get_fingerprint("src_b", "b-1")
    assert skipped_row.skipped is True
    assert skipped_row.content_hash is None and skipped_row.simhash_hex is None
    assert skipped_row.skip_reason is not None

    ok_row = store.get_fingerprint("src_a", "a-1")
    assert ok_row.skipped is False
    assert len(ok_row.content_hash) == 64  # sha256
    assert len(ok_row.simhash_hex) == 16
    assert ok_row.rule_version == 1


def test_exact_duplicates_identified_with_100_percent_accuracy(setup) -> None:
    _, store, service, reader, _, _ = setup
    service.run_full(reader.iter_records())

    exact_groups = store.list_groups(group_type="exact", rule_version=1)
    assert len(exact_groups) == 1
    group = exact_groups[0]
    members = {(m.source, m.external_id) for m in store.list_members(group.id)}
    # 同源与跨源的完全重复都必须被识别
    assert members == {("src_a", "a-1"), ("src_b", "b-9")}
    assert group.source_count == 2
    assert set(group.sources) == {"src_a", "src_b"}
    assert group.min_similarity == 1.0


def test_similar_but_not_identical_marked_above_threshold(setup) -> None:
    _, store, service, reader, _, _ = setup
    _, mark_stats = service.run_full(reader.iter_records())

    similar_groups = store.list_groups(group_type="similar", rule_version=1)
    assert mark_stats.similar_groups == len(similar_groups) == 1
    group = similar_groups[0]
    members = {(m.source, m.external_id) for m in store.list_members(group.id)}
    # 近似重复簇包含原文与改写版（精确重复对不重复计入 similar）
    assert ("src_c", "c-3") in members
    assert ("src_a", "a-1") in members
    assert group.min_similarity >= 0.9
    assert group.max_hamming <= 6


def test_below_threshold_not_marked(setup) -> None:
    _, store, service, reader, _, _ = setup
    service.run_full(reader.iter_records())

    # 公园、图书馆条目不应出现在任何簇中
    for source, external_id in (("src_a", "a-2"), ("src_c", "c-4")):
        assert store.groups_for_record(source, external_id) == []

    # 被标记条目可以反查到完整关联来源与标识
    marks = service.lookup_record_marks("src_b", "b-9")
    assert any(mark["group_type"] == "exact" for mark in marks)
    exact_mark = next(mark for mark in marks if mark["group_type"] == "exact")
    assert {m["source"] for m in exact_mark["members"]} == {"src_a", "src_b"}


def test_fingerprints_persist_across_restart_and_skip_recompute(setup) -> None:
    config, store, service, reader, _, fingerprint_path = setup
    first = service.compute_fingerprints(reader.iter_records())
    assert first.written == 5 and first.unchanged == 0
    store.close()

    # 模拟任务重启：重新打开同一个指纹库
    restarted_store = FingerprintStore(fingerprint_path)
    restarted_store.init()
    restarted_service = FingerprintService(restarted_store, config)
    second = restarted_service.compute_fingerprints(reader.iter_records())
    try:
        # 指纹持久化存在且未变化，无需重新写库
        assert second.written == 0
        assert second.unchanged == 5
        assert second.skipped == 2
    finally:
        restarted_store.close()


def test_rule_update_triggers_batch_recompute(setup) -> None:
    config, store, service, reader, _, _ = setup
    service.recompute_all(reader.iter_records())
    original_hash = store.get_fingerprint("src_a", "a-1").content_hash

    # 规则升级：更换哈希算法并递增版本号
    new_config = config.model_copy(
        deep=True,
        update={
            "rule": config.rule.model_copy(
                deep=True, update={"version": 2, "hash_algorithm": "sha1"}
            )
        },
    )
    upgraded = FingerprintService(store, new_config)
    stats = upgraded.recompute_all(reader.iter_records())
    mark_stats = upgraded.build_dedup_marks()

    assert stats.rule_version == 2
    # 5 条有效指纹按新版本重写；2 条空/短内容仍计入 skipped（同样按新版本留痕）
    assert stats.written == 5
    assert stats.skipped == 2
    new_row = store.get_fingerprint("src_a", "a-1")
    assert new_row.rule_version == 2
    assert new_row.content_hash != original_hash
    assert len(new_row.content_hash) == 40  # sha1
    skipped_row = store.get_fingerprint("src_a", "a-3")
    assert skipped_row.rule_version == 2 and skipped_row.skipped is True
    # 标记簇同样按新版本重建
    assert all(group.rule_version == 2 for group in store.list_groups())
    assert mark_stats.rule_version == 2
    assert mark_stats.exact_groups == 1


def test_threshold_configurable_no_false_positive(setup, tmp_path: Path) -> None:
    config, store, _, reader, _, _ = setup
    # 极严格阈值：1.0 意味着只有完全相同的 SimHash 才算相似
    strict_config = config.model_copy(
        deep=True, update={"similarity": config.similarity.model_copy(update={"threshold": 1.0})}
    )
    strict_service = FingerprintService(store, strict_config)
    _, mark_stats = strict_service.run_full(reader.iter_records())
    assert mark_stats.similar_groups == 0
    # 完全重复仍然 100% 识别（精确哈希不受相似度阈值影响）
    assert mark_stats.exact_groups == 1


def test_main_database_never_modified(setup, main_db_path: Path) -> None:
    _, _, service, reader, _, _ = setup
    before_engine = create_engine(f"sqlite:///{main_db_path}", future=True)
    with Session(before_engine) as session:
        before = [(r.source, r.external_id, r.content, r.updated_at) for r in session.execute(select(RecordRow)).scalars()]
    before_engine.dispose()

    service.run_full(reader.iter_records())

    after_engine = create_engine(f"sqlite:///{main_db_path}", future=True)
    with Session(after_engine) as session:
        after = [(r.source, r.external_id, r.content, r.updated_at) for r in session.execute(select(RecordRow)).scalars()]
    with after_engine.connect() as connection:
        table_names = set(inspect(connection).get_table_names())
    after_engine.dispose()

    assert before == after  # 主库 records 行原样保留
    # 主库没有出现任何去重模块的表
    assert "content_fingerprints" not in table_names
    assert "dedup_groups" not in table_names


def test_reader_rejects_write_attempt(setup, main_db_path: Path) -> None:
    from sqlalchemy.exc import OperationalError

    reader = RecordReader(main_db_path)
    try:
        with pytest.raises(OperationalError):
            with reader.engine.begin() as connection:
                connection.execute(RecordRow.__table__.delete())
    finally:
        reader.close()


def test_fingerprint_db_is_separate_file(setup, tmp_path: Path, main_db_path: Path) -> None:
    _, store, _, _, _, fingerprint_path = setup
    assert fingerprint_path != main_db_path
    # 指纹库只含去重模块自己的三张表
    names = set(inspect(store.engine).get_table_names())
    assert names == {"content_fingerprints", "dedup_groups", "dedup_group_members"}


def test_stale_fingerprints_removed_on_full_recompute(setup, main_db_path: Path) -> None:
    _, store, service, reader, _, _ = setup
    # 先为一条「后来被删除」的记录手工写入指纹
    store.upsert_fingerprint(
        {
            "source": "ghost",
            "external_id": "gone-1",
            "title": "幽灵条目",
            "title_hash": None,
            "content_hash": "x",
            "simhash_hex": "0" * 16,
            "rule_version": 1,
            "rule_signature": "v1",
            "content_length": 100,
            "skipped": False,
            "skip_reason": None,
        }
    )
    stats = service.recompute_all(reader.iter_records())
    assert stats.stale_deleted == 1
    assert store.get_fingerprint("ghost", "gone-1") is None


def test_programmatic_record_input_without_main_db(tmp_path: Path) -> None:
    """服务也支持直接传入记录数据，无需采集主库（库外复用场景）。"""

    store = FingerprintStore(":memory:")
    store.init()
    try:
        service = FingerprintService(store, DedupConfig())
        records = [
            RecordData("s1", "1", "标题甲", TECH_CONTENT),
            RecordData("s2", "2", "标题乙", TECH_CONTENT),
        ]
        compute_stats, mark_stats = service.run_full(records)
        assert compute_stats.written == 2
        assert mark_stats.exact_groups == 1
        assert store.count_groups(group_type="exact") == 1
    finally:
        store.close()


def test_mark_tables_store_all_related_sources(setup) -> None:
    _, store, service, reader, _, _ = setup
    service.run_full(reader.iter_records())
    with store._session_factory() as session:
        member_count = session.execute(
            select(func.count()).select_from(DedupGroupMemberRow)
        ).scalar_one()
        group_count = session.execute(
            select(func.count()).select_from(DedupGroupRow)
        ).scalar_one()
    # 精确簇 2 成员 + 相似簇 3 成员
    assert member_count == 5
    assert group_count == 2
