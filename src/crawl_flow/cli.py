"""Typer 命令行入口：run / db stats / dedup / serve-mock。"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_sources, select_sources
from crawl_flow.dedup import DedupConfig, DedupService, DedupStore, load_dedup_config
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

DEFAULT_CONFIG = Path("config/sources.yaml")
DEFAULT_DEDUP_CONFIG = Path("config/dedup.yaml")
DEFAULT_DB = Path("data/crawl.db")
DEFAULT_RAW_DIR = Path("data/raw")

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="crawl-flow：公开数据采集 / 解析 / 归一化 / 入库管道",
)
db_app = typer.Typer(help="数据库运维命令", no_args_is_help=True)
app.add_typer(db_app, name="db")
dedup_app = typer.Typer(help="内容指纹与去重（只标记，不改动原始数据）", no_args_is_help=True)
app.add_typer(dedup_app, name="dedup")


@app.command("run")
def run_command(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只采集指定源"),
    all_sources: bool = typer.Option(False, "--all", help="采集所有 enabled 源"),
    limit: Optional[int] = typer.Option(None, "--limit", min=1, help="每个源最多处理 N 条"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只解析，不写 raw 与数据库"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录"),
) -> None:
    """跑一轮采集。"""

    logger = configure_logging(verbose)
    try:
        sources = load_sources(config)
        selected = select_sources(sources, name=source, all_sources=all_sources)
    except ConfigError as exc:
        typer.secho(f"配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    db = Database(db_path)
    raw_store = RawStore(raw_dir)
    summaries: list[RunSummary] = []
    try:
        db.init()
        with Fetcher() as fetcher:
            for selected_source in selected:
                summaries.append(
                    run_source(
                        selected_source,
                        db,
                        raw_store,
                        fetcher=fetcher,
                        limit=limit,
                        dry_run=dry_run,
                        logger=logger,
                    )
                )
    except FetchError as exc:
        typer.secho(f"采集失败（整轮中止）: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    except StoreError as exc:
        typer.secho(f"存储失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        db.close()

    _print_summaries(summaries, dry_run=dry_run)


@db_app.command("stats")
def db_stats(
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看每个源的 records / raw 统计。"""

    configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    database = Database(db_path)
    try:
        database.init()
        stats = database.stats()
    except StoreError as exc:
        typer.secho(f"统计失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()

    typer.echo(f"{'source':<24}{'records':>8}{'raw_ok':>8}{'raw_failed':>12}  last_fetched_at")
    for row in stats["sources"]:
        last_seen = row["last_fetched_at"] or "-"
        typer.echo(
            f"{row['source']:<24}{row['records']:>8}{row['raw_ok']:>8}"
            f"{row['raw_failed']:>12}  {last_seen}"
        )
    totals = stats["totals"]
    typer.echo(
        f"{'TOTAL':<24}{totals['records']:>8}{totals['raw_ok']:>8}"
        f"{totals['raw_failed']:>12}"
    )


def _load_dedup_rules(config: Path) -> "DedupConfig":
    """加载去重规则；默认路径不存在时退回内置默认规则。"""

    if not config.exists() and config == DEFAULT_DEDUP_CONFIG:
        return DedupConfig()
    return load_dedup_config(config)


def _open_dedup_store(db_path: Path) -> DedupStore:
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    store = DedupStore(db_path)
    store.init()
    return store


@dedup_app.command("run")
def dedup_run(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只处理指定源的新增/变更记录"),
    force: bool = typer.Option(False, "--force", help="强制重算全部历史指纹"),
    config: Path = typer.Option(DEFAULT_DEDUP_CONFIG, "--config", help="去重规则配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """计算内容指纹并重建重复/相似标记（只标记，不删除原始数据）。"""

    logger = configure_logging(verbose)
    try:
        rules = _load_dedup_rules(config)
    except ConfigError as exc:
        typer.secho(f"配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    store = _open_dedup_store(db_path)
    try:
        summary = DedupService(store, rules).run(source=source, force=force, logger=logger)
    except StoreError as exc:
        typer.secho(f"去重失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        store.close()

    typer.echo(
        f"去重完成 rule_version={summary.rule_version} "
        f"scanned={summary.scanned} computed={summary.computed} "
        f"reused={summary.reused} skipped_short={summary.skipped_short} "
        f"pruned={summary.pruned} elapsed_ms={summary.elapsed_ms}"
    )
    typer.echo(f"重复组 groups={summary.groups} 被标记记录 marked={summary.marked}（仅标记，未改动原始数据）")


@dedup_app.command("stats")
def dedup_stats(
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看指纹与去重标记统计。"""

    configure_logging(verbose)
    store = _open_dedup_store(db_path)
    try:
        stats = store.stats()
    finally:
        store.close()

    typer.echo(f"fingerprints={stats['fingerprints']}")
    for version, count in sorted(stats["fingerprint_versions"].items()):
        typer.echo(f"  rule_version={version} count={count}")
    typer.echo(f"groups={stats['groups']} marked_records={stats['marked_records']}")
    for match_type, count in sorted(stats["groups_by_type"].items()):
        typer.echo(f"  match_type={match_type} groups={count}")


@dedup_app.command("groups")
def dedup_groups(
    limit: int = typer.Option(20, "--limit", min=1, help="最多展示多少个重复组"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """列出重复/相似内容组及其关联的来源与条目标识。"""

    configure_logging(verbose)
    store = _open_dedup_store(db_path)
    try:
        groups = store.list_groups(limit=limit)
        members = {group.id: store.list_members(group.id) for group in groups}
    finally:
        store.close()

    if not groups:
        typer.echo("暂无重复内容标记")
        return
    for group in groups:
        typer.echo(
            f"group={group.group_key} match_type={group.match_type} "
            f"members={group.member_count}"
        )
        for member in members[group.id]:
            flag = "*" if member.is_representative else " "
            typer.echo(
                f"  {flag} source={member.source} external_id={member.external_id} "
                f"similarity={member.similarity:.4f}"
            )


@app.command("serve-mock")
def serve_mock(
    port: int = typer.Option(8010, "--port", min=1, max=65535, help="监听端口"),
    host: str = typer.Option("127.0.0.1", "--host", help="监听地址"),
) -> None:
    """启动本地 mock 站点（离线可用）。"""

    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    try:
        from mock_site.app import create_app
    except ImportError as exc:
        typer.secho(
            f"无法导入 mock_site，请在仓库根目录安装后运行: {exc}",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1) from exc

    typer.echo(f"mock site: http://{host}:{port}")
    create_app().run(host=host, port=port, debug=False, use_reloader=False)


def _print_summaries(summaries: list[RunSummary], *, dry_run: bool) -> None:
    mode = "（dry-run，未写 raw 与数据库）" if dry_run else ""
    typer.echo(f"本轮汇总{mode}")
    for summary in summaries:
        typer.echo(
            f"source={summary.source} ok={summary.ok} failed={summary.failed} "
            f"skipped={summary.skipped} elapsed_ms={summary.elapsed_ms}"
        )
    if summaries:
        typer.echo(
            f"total ok={sum(s.ok for s in summaries)} "
            f"failed={sum(s.failed for s in summaries)} "
            f"skipped={sum(s.skipped for s in summaries)}"
        )


if __name__ == "__main__":
    app()
