"""``crawl-flow dedup`` 命令组：指纹计算、重算、统计与标记查询。

命令组独立于采集命令 ``run``：不改变采集入库行为，只读取主库并写独立
指纹库。去重结果仅以标记形式存储，任何命令都不会删除采集数据。
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from crawl_flow.dedup.config import DedupConfig, load_dedup_config
from crawl_flow.dedup.reader import RecordReader
from crawl_flow.dedup.service import FingerprintService
from crawl_flow.dedup.store import FingerprintStore
from crawl_flow.errors import ConfigError, StoreError
from crawl_flow.observability.logging import configure_logging

DEFAULT_DB = Path("data/crawl.db")
DEFAULT_DEDUP_CONFIG = Path("config/dedup.yaml")

dedup_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="内容指纹与去重（独立指纹库，只标记不删除采集数据）",
)


def _build_service(
    *, dedup_config: Path, fingerprint_db: Optional[Path], verbose: bool
) -> tuple[FingerprintService, FingerprintStore, DedupConfig]:
    """组装配置、只读读取器与服务；指纹库不存在时自动建表。"""

    logger = configure_logging(verbose)
    config_path = dedup_config if dedup_config.exists() else None
    try:
        config = load_dedup_config(config_path)
    except ConfigError as exc:
        typer.secho(f"配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    db_path = Path(config.fingerprint_db) if fingerprint_db is None else fingerprint_db
    store = FingerprintStore(db_path)
    store.init()
    service = FingerprintService(store, config, logger=logger)
    return service, store, config


def _load_records(main_db: Path, source: Optional[str]):
    try:
        reader = RecordReader(main_db)
        records = reader.iter_records(source=source)
    except StoreError as exc:
        typer.secho(f"读取采集主库失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    return records


@dedup_app.command("compute")
def compute_command(
    main_db: Path = typer.Option(DEFAULT_DB, "--db", help="采集主库 SQLite 路径（只读）"),
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只处理指定源"),
    dedup_config: Path = typer.Option(DEFAULT_DEDUP_CONFIG, "--config", help="去重配置文件路径"),
    fingerprint_db: Optional[Path] = typer.Option(
        None, "--fingerprint-db", help="指纹库路径，默认取配置中的 fingerprint_db"
    ),
    no_marks: bool = typer.Option(False, "--no-marks", help="只算指纹，不重建去重标记"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """为采集库中的记录计算内容指纹并识别重复（增量，已存在且未变化的跳过）。"""

    service, store, config = _build_service(
        dedup_config=dedup_config, fingerprint_db=fingerprint_db, verbose=verbose
    )
    try:
        records = _load_records(main_db, source)
        stats = service.compute_fingerprints(records)
        mark_stats = None
        if not no_marks:
            mark_stats = service.build_dedup_marks()
    except StoreError as exc:
        typer.secho(f"指纹库写入失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        store.close()

    typer.echo(
        f"指纹计算完成 rule_version={stats.rule_version} total={stats.total} "
        f"written={stats.written} unchanged={stats.unchanged} skipped={stats.skipped} "
        f"stale_deleted={stats.stale_deleted}"
    )
    if mark_stats is not None:
        typer.echo(
            f"去重标记 exact_groups={mark_stats.exact_groups} "
            f"similar_groups={mark_stats.similar_groups} "
            f"marked_records={mark_stats.marked_records} "
            f"threshold={config.similarity.threshold} "
            f"max_hamming={mark_stats.max_hamming}"
        )


@dedup_app.command("recompute")
def recompute_command(
    main_db: Path = typer.Option(DEFAULT_DB, "--db", help="采集主库 SQLite 路径（只读）"),
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只重算指定源"),
    dedup_config: Path = typer.Option(DEFAULT_DEDUP_CONFIG, "--config", help="去重配置文件路径"),
    fingerprint_db: Optional[Path] = typer.Option(
        None, "--fingerprint-db", help="指纹库路径，默认取配置中的 fingerprint_db"
    ),
    no_marks: bool = typer.Option(False, "--no-marks", help="只重算指纹，不重建去重标记"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """规则更新后按当前规则版本批量重算历史条目指纹并重建标记。"""

    service, store, config = _build_service(
        dedup_config=dedup_config, fingerprint_db=fingerprint_db, verbose=verbose
    )
    try:
        records = _load_records(main_db, source)
        if source is None:
            stats = service.recompute_all(records)
        else:
            # 指定源时只强制重写该源，不清理其它源的历史指纹。
            stats = service.compute_fingerprints(records, force=True, prune_missing=False)
        mark_stats = None
        if not no_marks:
            mark_stats = service.build_dedup_marks()
    except StoreError as exc:
        typer.secho(f"指纹库写入失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        store.close()

    typer.echo(
        f"批量重算完成 rule_version={stats.rule_version} total={stats.total} "
        f"written={stats.written} skipped={stats.skipped} stale_deleted={stats.stale_deleted}"
    )
    if mark_stats is not None:
        typer.echo(
            f"去重标记 exact_groups={mark_stats.exact_groups} "
            f"similar_groups={mark_stats.similar_groups} "
            f"marked_records={mark_stats.marked_records} "
            f"threshold={config.similarity.threshold}"
        )


@dedup_app.command("stats")
def stats_command(
    dedup_config: Path = typer.Option(DEFAULT_DEDUP_CONFIG, "--config", help="去重配置文件路径"),
    fingerprint_db: Optional[Path] = typer.Option(
        None, "--fingerprint-db", help="指纹库路径，默认取配置中的 fingerprint_db"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看指纹与去重标记的汇总统计。"""

    _, store, config = _build_service(
        dedup_config=dedup_config, fingerprint_db=fingerprint_db, verbose=verbose
    )
    try:
        total = store.count_fingerprints()
        skipped = store.count_skipped()
        version = config.rule.version
        version_total = store.count_fingerprints(rule_version=version)
        exact = store.count_groups(group_type="exact", rule_version=version)
        similar = store.count_groups(group_type="similar", rule_version=version)
    except StoreError as exc:
        typer.secho(f"统计失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        store.close()

    typer.echo(f"rule_version={version} algorithm={config.rule.hash_algorithm}")
    typer.echo(
        f"fingerprints total={total} current_version={version_total} skipped={skipped}"
    )
    typer.echo(
        f"groups exact={exact} similar={similar} "
        f"threshold={config.similarity.threshold} bands={config.similarity.bands}"
    )


@dedup_app.command("groups")
def groups_command(
    group_type: Optional[str] = typer.Option(
        None, "--type", help="只看指定类型：exact（完全重复）/ similar（近似重复）"
    ),
    dedup_config: Path = typer.Option(DEFAULT_DEDUP_CONFIG, "--config", help="去重配置文件路径"),
    fingerprint_db: Optional[Path] = typer.Option(
        None, "--fingerprint-db", help="指纹库路径，默认取配置中的 fingerprint_db"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """列出全部重复/相似簇及每个簇关联的来源与条目标识。"""

    _, store, config = _build_service(
        dedup_config=dedup_config, fingerprint_db=fingerprint_db, verbose=verbose
    )
    if group_type is not None and group_type not in ("exact", "similar"):
        typer.secho("--type 只支持 exact 或 similar", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    try:
        groups = store.list_groups(
            group_type=group_type, rule_version=config.rule.version
        )
        if not groups:
            typer.echo("没有命中的重复/相似簇")
            return
        for group in groups:
            members = store.list_members(group.id)
            cross_source = "跨源" if group.source_count > 1 else "同源"
            similarity_hint = (
                f" min_similarity={group.min_similarity:.3f}"
                if group.min_similarity is not None
                else " min_similarity=1.000"
            )
            typer.echo(
                f"[#{group.id}] {group.group_type} ({cross_source}) size={group.size} "
                f"sources={','.join(group.sources)}{similarity_hint}"
            )
            for member in members:
                typer.echo(f"    - {member.source}/{member.external_id}  {member.title}")
    except StoreError as exc:
        typer.secho(f"查询失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        store.close()


@dedup_app.command("show")
def show_command(
    source: str = typer.Option(..., "--source", "-s", help="条目所属源"),
    external_id: str = typer.Option(..., "--external-id", "-i", help="源内条目标识"),
    dedup_config: Path = typer.Option(DEFAULT_DEDUP_CONFIG, "--config", help="去重配置文件路径"),
    fingerprint_db: Optional[Path] = typer.Option(
        None, "--fingerprint-db", help="指纹库路径，默认取配置中的 fingerprint_db"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查询单条记录的指纹及其命中的全部重复/相似簇。"""

    service, store, _ = _build_service(
        dedup_config=dedup_config, fingerprint_db=fingerprint_db, verbose=verbose
    )
    try:
        row = store.get_fingerprint(source, external_id)
        if row is None:
            typer.secho(
                f"未找到指纹 {source}/{external_id}，请先运行 dedup compute",
                fg=typer.colors.YELLOW,
                err=True,
            )
            raise typer.Exit(code=1)
        typer.echo(f"record={source}/{external_id} title={row.title}")
        typer.echo(
            f"rule_version={row.rule_version} content_length={row.content_length} "
            f"skipped={row.skipped}"
        )
        if row.skipped:
            typer.echo(f"skip_reason={row.skip_reason}（未生成有效指纹）")
            return
        typer.echo(f"content_hash={row.content_hash}")
        typer.echo(f"title_hash={row.title_hash}")
        typer.echo(f"simhash={row.simhash_hex}")
        marks = service.lookup_record_marks(source, external_id)
        if not marks:
            typer.echo("未命中任何重复/相似簇")
            return
        typer.echo(f"命中 {len(marks)} 个簇：")
        for mark in marks:
            typer.echo(
                f"  - {mark['group_type']} group_id={mark['group_id']} "
                f"sources={','.join(mark['sources'])} size={mark['size']}"
            )
            for member in mark["members"]:
                typer.echo(
                    f"      * {member['source']}/{member['external_id']}  {member['title']}"
                )
    except StoreError as exc:
        typer.secho(f"查询失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        store.close()
