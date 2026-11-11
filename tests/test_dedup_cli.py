"""``crawl-flow dedup`` 命令组冒烟测试与指纹性能基准。"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.config import SourceConfig
from crawl_flow.dedup.config import DedupConfig
from crawl_flow.dedup.fingerprint import Fingerprinter, FingerprintRule
from crawl_flow.dedup.reader import RecordData
from crawl_flow.dedup.service import FingerprintService
from crawl_flow.dedup.store import FingerprintStore
from crawl_flow.models import RecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

runner = CliRunner()


def _seed_cross_source_duplicates(db: Database) -> None:
    """在采集结果之外手工插入跨源重复与空内容条目。"""

    with Session(db.engine) as session:
        anchor = session.query(RecordRow).filter(RecordRow.external_id == "api-1").one()
        now = datetime.now(timezone.utc)
        session.add(
            RecordRow(
                source="partner_site",
                external_id="p-100",
                title="转载：" + anchor.title,
                category=anchor.category,
                tags=[],
                published_at=None,
                content=anchor.content,
                url="https://partner.example.test/p-100",
                fetched_at=now,
                updated_at=now,
            )
        )
        session.add(
            RecordRow(
                source="partner_site",
                external_id="p-101",
                title="空内容转载",
                category=None,
                tags=[],
                published_at=None,
                content="",
                url="https://partner.example.test/p-101",
                fetched_at=now,
                updated_at=now,
            )
        )
        session.commit()


def test_dedup_full_workflow_via_cli(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    tmp_path: Path,
) -> None:
    run_source(api_source, db, raw_store, limit=5)
    _seed_cross_source_duplicates(db)
    db.close()

    # db fixture 用的是 tmp_path/crawl.db
    fingerprint_db = tmp_path / "fingerprints.db"
    dedup_config = tmp_path / "dedup.yaml"
    dedup_config.write_text(
        f"fingerprint_db: {fingerprint_db}\n"
        "rule:\n  version: 1\n"
        "similarity:\n  enabled: true\n  threshold: 0.9\n  bands: 4\n",
        encoding="utf-8",
    )

    # 1) compute：增量计算并打标记
    result = runner.invoke(
        app,
        [
            "dedup", "compute",
            "--db", str(tmp_path / "crawl.db"),
            "--config", str(dedup_config),
        ],
    )
    assert result.exit_code == 0, result.output
    # 5 条采集 + 1 条跨源转载共 6 条有效指纹；空内容 1 条跳过
    assert "written=6" in result.output
    assert "skipped=1" in result.output
    assert "exact_groups=1" in result.output

    # 2) 再次 compute：持久化命中，unchanged 增加、written 为 0
    second = runner.invoke(
        app,
        ["dedup", "compute", "--db", str(tmp_path / "crawl.db"), "--config", str(dedup_config)],
    )
    assert second.exit_code == 0, second.output
    assert "written=0" in second.output and "unchanged=6" in second.output

    # 3) stats
    stats = runner.invoke(
        app, ["dedup", "stats", "--config", str(dedup_config)]
    )
    assert stats.exit_code == 0, stats.output
    assert "exact=1" in stats.output

    # 4) groups：跨源簇包含两个来源的条目
    groups = runner.invoke(
        app, ["dedup", "groups", "--config", str(dedup_config)]
    )
    assert groups.exit_code == 0, groups.output
    assert "跨源" in groups.output
    assert "demo_api/api-1" in groups.output
    assert "partner_site/p-100" in groups.output

    # 5) show 单条记录反查
    show = runner.invoke(
        app,
        [
            "dedup", "show",
            "--source", "partner_site",
            "--external-id", "p-100",
            "--config", str(dedup_config),
        ],
    )
    assert show.exit_code == 0, show.output
    assert "content_hash=" in show.output
    assert "simhash=" in show.output
    assert "exact" in show.output


def test_dedup_recompute_after_rule_bump(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    tmp_path: Path,
) -> None:
    run_source(api_source, db, raw_store, limit=3)
    db.close()

    fingerprint_db = tmp_path / "fingerprints.db"
    config_path = tmp_path / "dedup.yaml"
    config_path.write_text(
        f"fingerprint_db: {fingerprint_db}\nrule:\n  version: 1\n  hash_algorithm: sha256\n",
        encoding="utf-8",
    )
    first = runner.invoke(
        app, ["dedup", "compute", "--db", str(tmp_path / "crawl.db"), "--config", str(config_path)]
    )
    assert first.exit_code == 0, first.output

    config_path.write_text(
        f"fingerprint_db: {fingerprint_db}\nrule:\n  version: 2\n  hash_algorithm: sha1\n",
        encoding="utf-8",
    )
    recompute = runner.invoke(
        app, ["dedup", "recompute", "--db", str(tmp_path / "crawl.db"), "--config", str(config_path)]
    )
    assert recompute.exit_code == 0, recompute.output
    assert "rule_version=2" in recompute.output
    assert "written=3" in recompute.output

    stats = runner.invoke(app, ["dedup", "stats", "--config", str(config_path)])
    assert "rule_version=2" in stats.output
    assert "algorithm=sha1" in stats.output


def test_dedup_missing_main_db_exits_2(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "dedup", "compute",
            "--db", str(tmp_path / "missing.db"),
            "--fingerprint-db", str(tmp_path / "fp.db"),
        ],
    )
    assert result.exit_code == 2
    assert "采集主库不存在" in result.output


def test_dedup_show_unknown_record_exits_1(tmp_path: Path) -> None:
    result = runner.invoke(
        app,
        [
            "dedup", "show",
            "--source", "nope",
            "--external-id", "x",
            "--fingerprint-db", str(tmp_path / "fp.db"),
        ],
    )
    assert result.exit_code == 1
    assert "未找到指纹" in result.output


def test_dedup_help_available() -> None:
    result = runner.invoke(app, ["dedup", "--help"])
    assert result.exit_code == 0
    for command in ("compute", "recompute", "stats", "groups", "show"):
        assert command in result.output


# ---------------------------------------------------------------------
# 性能基准：指纹生成速度 >= 100 条/秒
# ---------------------------------------------------------------------

def test_fingerprint_throughput_above_100_per_second() -> None:
    import time

    fingerprinter = Fingerprinter(FingerprintRule())
    base = (
        "三款开源数据库在同一台测试机上完成读写混合压测，测试包含插入、点查与范围查询，"
        "记录吞吐和尾延迟数据，用于后续的性能基线对比分析工作。"
    )
    records = [
        RecordData(f"s{i % 5}", f"id-{i}", f"标题{i}", base + f"这是第{i}条记录的末尾补充句。")
        for i in range(500)
    ]

    started = time.perf_counter()
    for record in records:
        result = fingerprinter.fingerprint(title=record.title, content=record.content)
        assert not result.skipped
    elapsed = time.perf_counter() - started

    throughput = len(records) / elapsed
    assert throughput >= 100, f"吞吐量 {throughput:.1f} 条/秒低于 100 条/秒要求"


def test_full_pipeline_throughput_with_persistence(tmp_path: Path) -> None:
    import time

    store = FingerprintStore(":memory:")
    store.init()
    try:
        service = FingerprintService(store, DedupConfig())
        base = (
            "城市公园春季花期观测报告显示三月上旬有十二种植物进入盛花期，志愿者按周汇总"
            "开花株数、游客数量与天气备注，数据将用于园林部门的花期预报与客流疏导工作。"
        )
        records = [
            RecordData(f"s{i % 4}", f"id-{i}", f"标题{i}", base + f"末尾补充第{i}句。")
            for i in range(300)
        ]
        started = time.perf_counter()
        service.compute_fingerprints(records)
        elapsed = time.perf_counter() - started
        assert store.count_fingerprints() == 300
        assert 300 / elapsed >= 100, f"含持久化吞吐 {300 / elapsed:.1f} 条/秒不达标"
    finally:
        store.close()
