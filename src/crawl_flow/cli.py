"""Typer 命令行入口：run / db stats / serve-mock。"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_config, normalize_diff_fields, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.diff import (
    STATUS_ADDED,
    STATUS_ANOMALY,
    STATUS_MODIFIED,
    STATUS_UNCHANGED,
    DiffReport,
)
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

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
    diff: bool = typer.Option(
        False,
        "--diff",
        "--compare",
        help="dry-run 时对比存量记录并输出新增/修改/无变化/异常统计（必须配合 --dry-run）",
    ),
    diff_fields: Optional[str] = typer.Option(
        None,
        "--diff-fields",
        help="覆盖对比字段，逗号分隔（默认取配置 diff.compare_fields，缺省 title,content,published_at）",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录"),
) -> None:
    """跑一轮采集。"""

    logger = configure_logging(verbose)
    try:
        bundle = load_config(config)
        sources = bundle.sources
        selected = select_sources(sources, name=source, all_sources=all_sources)
        diff_enabled = _resolve_diff_mode(diff, dry_run)
        diff_field_list = _resolve_diff_fields(
            diff_fields,
            bundle.diff.fields,
            enabled=diff_enabled,
        )
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
                        diff_fields=diff_field_list if diff_enabled else None,
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
    if diff_enabled:
        _print_diff_reports(summaries)


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


def _resolve_diff_mode(diff: bool, dry_run: bool) -> bool:
    """校验对比参数：``--diff`` 只能与 ``--dry-run`` 搭配，否则报配置错误。"""

    if diff and not dry_run:
        raise ConfigError("--diff 必须与 --dry-run 一起使用")
    return diff


def _resolve_diff_fields(
    cli_fields: Optional[str],
    config_fields: list[str],
    *,
    enabled: bool,
) -> list[str]:
    """确定对比字段：CLI 覆盖优先，其次配置，最后使用默认值。

    未开启对比时直接返回空列表，确保原有 dry-run 行为完全不变。
    """

    if not enabled:
        return []
    if cli_fields is None:
        return list(config_fields)
    try:
        return normalize_diff_fields(cli_fields)
    except ValueError as exc:
        raise ConfigError(f"--diff-fields 无效: {exc}") from exc


# 明细里只输出变更记录，避免大量无变化记录淹没输出；异常全部输出。
MAX_DIFF_DETAILS = 50


def _print_diff_reports(summaries: list[RunSummary]) -> None:
    for summary in summaries:
        report = summary.diff
        if report is not None:
            _print_diff_report(report)

    reports = [summary.diff for summary in summaries if summary.diff is not None]
    if len(reports) <= 1:
        return
    typer.echo(
        f"对比总计 source={len(reports)} added={sum(r.added for r in reports)} "
        f"modified={sum(r.modified for r in reports)} "
        f"unchanged={sum(r.unchanged for r in reports)} "
        f"anomaly={sum(r.anomaly for r in reports)}"
    )


def _print_diff_report(report: DiffReport) -> None:
    typer.echo(
        f"差异对比 source={report.source} fields={','.join(report.fields)} "
        f"added={report.added} modified={report.modified} "
        f"unchanged={report.unchanged} anomaly={report.anomaly} "
        f"compare_ms={report.elapsed_ms}"
    )

    if report.field_changes:
        parts = [f"{name}={count}" for name, count in report.field_changes.items()]
        typer.echo(f"字段变更统计 {' '.join(parts)}")

    shown = 0
    for entry in report.entries:
        if entry.status not in (STATUS_ADDED, STATUS_MODIFIED, STATUS_ANOMALY):
            continue
        if shown >= MAX_DIFF_DETAILS:
            typer.echo(
                f"... 其余 {report.added + report.modified + report.anomaly - shown} "
                "条变更明细已省略（--diff-fields 可缩小对比范围）"
            )
            break
        shown += 1
        if entry.status == STATUS_ADDED:
            typer.echo(f"  added external_id={entry.external_id}")
        elif entry.status == STATUS_MODIFIED:
            changes = " ".join(change.render() for change in entry.changes)
            typer.echo(f"  modified external_id={entry.external_id} {changes}")
        else:
            changes = " ".join(change.render() for change in entry.changes)
            detail = f" {changes}" if changes else ""
            typer.echo(
                f"  anomaly external_id={entry.external_id} "
                f"reason={entry.reason_code} detail={entry.reason}{detail}"
            )

    if not report.has_changes:
        typer.echo(f"  （无差异，{STATUS_UNCHANGED}={report.unchanged}）")


if __name__ == "__main__":
    app()
