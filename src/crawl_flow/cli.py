"""Typer 命令行入口：run / db stats / serve-mock。"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.archive import archive_cold_data
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

DEFAULT_CONFIG = Path("config/sources.yaml")
DEFAULT_DB = Path("data/crawl.db")
DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_ARCHIVE_DIR = Path("data/archive")
DEFAULT_RETENTION_DAYS = 90

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="crawl-flow：公开数据采集 / 解析 / 归一化 / 入库管道",
)
db_app = typer.Typer(help="数据库运维命令", no_args_is_help=True)
app.add_typer(db_app, name="db")


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
    archive_dir: Optional[Path] = typer.Option(
        None, "--archive-dir", help="归档目录（默认自动读取主库中的归档设置）"
    ),
    hot_only: bool = typer.Option(
        False, "--hot-only", help="只统计主库热数据，不合并归档库"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看每个源的 records / raw 统计（归档启用时自动合并归档库）。"""

    configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    database = Database(db_path, archive_dir=archive_dir)
    try:
        database.init()
        stats = database.stats(include_archive=not hot_only)
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
    archive = stats["archive"]
    if archive.get("enabled"):
        typer.echo(
            f"archive enabled dir={archive['archive_dir']} shards={archive['shards']} "
            f"records={archive['records']} raw={archive['raw']}"
        )


@app.command("archive")
def archive_command(
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="主 SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="热原始响应目录"),
    archive_dir: Path = typer.Option(
        DEFAULT_ARCHIVE_DIR, "--archive-dir", help="归档库与冷 raw 目录"
    ),
    days: Optional[int] = typer.Option(
        None, "--days", min=1, help=f"归档超过 N 天的数据（默认 {DEFAULT_RETENTION_DAYS}）"
    ),
    before: Optional[str] = typer.Option(
        None, "--before", help="显式截止时间（YYYY-MM-DD 或 ISO datetime）"
    ),
    start_at: Optional[str] = typer.Option(
        None, "--start", help="只归档该时间之后的数据（YYYY-MM-DD 或 ISO datetime）"
    ),
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只归档指定源"),
    batch_size: int = typer.Option(100, "--batch-size", min=10, max=5000, help="每批行数"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只统计候选行，不迁移"),
    restart: bool = typer.Option(False, "--restart", help="忽略断点，从头开始"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """把冷结构化数据与原始响应迁移到按时间分片的归档库。

    默认不启用归档；只有显式执行本命令才会产生归档设置。归档设置写入主库后，
    ``db stats`` / ``Database.query_records`` 会自动跨库关联，无需手动切换。
    """

    logger = configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    if before is not None and days is not None:
        typer.secho("--before 与 --days 不能同时使用", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    retention_days = days if days is not None else DEFAULT_RETENTION_DAYS
    if before is not None:
        try:
            parsed = datetime.fromisoformat(before)
        except ValueError as exc:
            typer.secho(f"无法解析 --before: {before}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2) from exc
        if parsed.tzinfo is None:
            cutoff = parsed
        else:
            cutoff = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    else:
        cutoff = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            days=retention_days
        )

    start_value: datetime | None = None
    if start_at is not None:
        try:
            parsed_start = datetime.fromisoformat(start_at)
        except ValueError as exc:
            typer.secho(f"无法解析 --start: {start_at}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2) from exc
        start_value = (
            parsed_start
            if parsed_start.tzinfo is None
            else parsed_start.astimezone(timezone.utc).replace(tzinfo=None)
        )
        if start_value >= cutoff:
            typer.secho("--start 必须早于归档截止时间", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2)

    database = Database(db_path)
    try:
        database.init()
        summary = archive_cold_data(
            database,
            archive_dir=archive_dir,
            cold_raw_dir=archive_dir / "raw",
            cutoff=cutoff,
            source=source,
            start=start_value,
            retention_days=retention_days,
            batch_size=batch_size,
            dry_run=dry_run,
            restart=restart,
            logger=logger,
        )
    except StoreError as exc:
        typer.secho(f"归档失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()

    mode = "（dry-run，未迁移）" if summary.dry_run else ""
    typer.echo(
        f"归档完成{mode} scope={summary.scope} cutoff={summary.cutoff.isoformat()} "
        f"records={summary.records_archived} raw={summary.raw_archived} "
        f"batches={summary.batches} max_lock_ms={summary.max_lock_ms} "
        f"resumed={str(summary.resumed).lower()} integrity_ok={str(summary.integrity_ok).lower()}"
    )
    if not summary.dry_run:
        typer.echo(f"archive_dir={summary.archive_dir} cold_raw_dir={summary.cold_raw_dir}")



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
