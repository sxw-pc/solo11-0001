"""Typer 命令行入口：run / db stats / serve-mock。"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.db import STATS_DIMENSIONS, Database
from crawl_flow.storage.export import write_dimension_csv
from crawl_flow.storage.raw_store import RawStore, safe_output_path

DEFAULT_CONFIG = Path("config/sources.yaml")
DEFAULT_DB = Path("data/crawl.db")
DEFAULT_RAW_DIR = Path("data/raw")

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
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
    dimension: Optional[str] = typer.Option(
        None,
        "--dim",
        help="按维度统计：category（分类）或 tag（标签）；缺省时保持按源汇总的默认行为",
    ),
    source: list[str] = typer.Option(
        None,
        "--source",
        "-s",
        help="只统计指定数据源，可重复传入多个；仅与 --dim 搭配使用",
    ),
    start: Optional[datetime] = typer.Option(
        None,
        "--start",
        help="抓取时间区间起点（ISO 8601，如 2024-03-01T00:00:00）；仅与 --dim 搭配使用",
    ),
    end: Optional[datetime] = typer.Option(
        None,
        "--end",
        help="抓取时间区间终点（ISO 8601）；仅与 --dim 搭配使用",
    ),
    output: Optional[Path] = typer.Option(
        None,
        "--output",
        "-o",
        help="把维度统计结果导出为 CSV；仅与 --dim 搭配使用",
    ),
) -> None:
    """查看统计：默认按源汇总，--dim 可按分类/标签做维度统计。"""

    configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    if dimension is None and (source or start is not None or end is not None or output is not None):
        typer.secho(
            "--source/--start/--end/--output 仅在 --dim 维度统计模式下可用",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)

    database = Database(db_path)
    try:
        database.init()
        if dimension is None:
            stats = database.stats()
        else:
            if dimension not in STATS_DIMENSIONS:
                typer.secho(
                    f"不支持的统计维度: {dimension}，可选值: {', '.join(STATS_DIMENSIONS)}",
                    fg=typer.colors.RED,
                    err=True,
                )
                raise typer.Exit(code=2)
            if start is not None and end is not None and start > end:
                typer.secho("时间区间起点不能晚于终点", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=2)
            stats = database.dimension_stats(
                dimension, sources=source or None, start=start, end=end
            )
    except StoreError as exc:
        typer.secho(f"统计失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()

    if dimension is None:
        _print_source_stats(stats)
    else:
        _print_dimension_stats(stats, output_path=output)


def _print_source_stats(stats: dict) -> None:
    """旧版按源汇总输出，保持历史调用的输出格式不变。"""

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


def _print_dimension_stats(stats: dict, *, output_path: Path | None) -> None:
    """按分类/标签维度输出，并可选导出 CSV。"""

    rows = stats["rows"]
    if not rows:
        typer.secho("无匹配数据，统计结果为空", fg=typer.colors.YELLOW)
        if output_path is not None:
            target = _export_csv(output_path, stats)
            typer.echo(f"已导出空结果（仅表头）: {target}")
        return

    typer.echo(
        f"{'dimension':<10}{'value':<20}{'records':>8}{'raw_failed':>12}"
        f"{'failed_pct':>12}  last_updated_at"
    )
    for row in rows:
        last_updated = row["last_updated_at"] or "-"
        typer.echo(
            f"{row['dimension']:<10}{str(row['dimension_value']):<20}"
            f"{row['record_count']:>8}{row['raw_failed']:>12}"
            f"{row['raw_failed_ratio'] * 100:>11.2f}%  {last_updated}"
        )
    totals = stats["totals"]
    typer.echo(
        f"{'TOTAL':<30}{totals['records']:>8}{totals['raw_failed']:>12}"
        f"{totals['raw_failed_ratio'] * 100:>11.2f}%"
    )

    if output_path is not None:
        target = _export_csv(output_path, stats)
        typer.echo(f"已导出 CSV: {target}")


def _export_csv(output_path: Path, stats: dict) -> Path:
    try:
        target = safe_output_path(output_path)
        return write_dimension_csv(target, stats)
    except StoreError as exc:
        typer.secho(f"CSV 导出失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc


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
