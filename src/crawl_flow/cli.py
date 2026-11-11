"""Typer 命令行入口：run / db stats / budget status / serve-mock。"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.budget import (
    BudgetCorruptError,
    BudgetError,
    BudgetLedger,
    BudgetLimits,
    DryRunBudgetMeter,
    SqliteBudgetMeter,
)
from crawl_flow.budget.ledger import utc_today
from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

DEFAULT_CONFIG = Path("config/sources.yaml")
DEFAULT_DB = Path("data/crawl.db")
DEFAULT_RAW_DIR = Path("data/raw")

# 退出码约定：0 正常；1 资源不存在；2 配置/存储/账本启动类错误；3 预算熔断。
EXIT_BUDGET_BROKEN = 3

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="crawl-flow：公开数据采集 / 解析 / 归一化 / 入库管道",
)
db_app = typer.Typer(help="数据库运维命令", no_args_is_help=True)
app.add_typer(db_app, name="db")
budget_app = typer.Typer(help="请求预算账本命令", no_args_is_help=True)
app.add_typer(budget_app, name="budget")


@app.command("run")
def run_command(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只采集指定源"),
    all_sources: bool = typer.Option(False, "--all", help="采集所有 enabled 源"),
    limit: Optional[int] = typer.Option(None, "--limit", min=1, help="每个源最多处理 N 条"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只解析，不写 raw 与数据库；预算只估算不记账"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径（预算表建在同一文件）"),
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

    # 预算记账器：dry-run 用纯内存估算器；正式运行打开 SQLite 账本。
    # 账本损坏时 BudgetCorruptError 在打开阶段直接抛出，拒绝启动。
    meter: SqliteBudgetMeter | DryRunBudgetMeter
    ledger: BudgetLedger | None = None
    if dry_run:
        meter = DryRunBudgetMeter()
    else:
        try:
            ledger = BudgetLedger(db_path)
        except BudgetCorruptError as exc:
            db.close()
            typer.secho(f"预算账本损坏，拒绝启动: {exc}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2) from exc
        except BudgetError as exc:
            db.close()
            typer.secho(f"预算账本不可用: {exc}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2) from exc
        meter = SqliteBudgetMeter(ledger)
    for selected_source in selected:
        meter.configure_source(BudgetLimits.from_source(selected_source))

    try:
        db.init()
        with Fetcher(meter=meter) as fetcher:
            for selected_source in selected:
                # 单源熔断只结束该源，循环继续跑下一个源。
                summaries.append(
                    run_source(
                        selected_source,
                        db,
                        raw_store,
                        fetcher=fetcher,
                        limit=limit,
                        dry_run=dry_run,
                        logger=logger,
                        meter=meter,
                    )
                )
    except FetchError as exc:
        typer.secho(f"采集失败（整轮中止）: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    except StoreError as exc:
        typer.secho(f"存储失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    except BudgetError as exc:
        # 运行中账本损坏/锁定等：不静默继续，直接失败退出。
        typer.secho(f"预算账本错误，中止运行: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        db.close()
        meter.close()

    _print_summaries(summaries, dry_run=dry_run)

    broken = [summary for summary in summaries if summary.budget_broken]
    if broken:
        for summary in broken:
            if dry_run:
                typer.secho(
                    f"[预算估算] source={summary.source} {summary.budget_break_reason} "
                    f"（演练未记账、未熔断）",
                    fg=typer.colors.YELLOW,
                )
            else:
                typer.secho(
                    f"[预算熔断] source={summary.source} 已停止后续抓取："
                    f"{summary.budget_break_reason}",
                    fg=typer.colors.RED,
                    err=True,
                )
        if not dry_run:
            raise typer.Exit(code=EXIT_BUDGET_BROKEN)


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


def _format_limit(value: int | None) -> str:
    return "不限" if value is None else str(value)


def _format_remaining(used: int, limit: int | None) -> str:
    if limit is None:
        return "不限"
    return str(max(0, limit - used))


@budget_app.command("status")
def budget_status(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径（预算表所在文件）"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看各源当日（UTC）已用与剩余的请求/字节额度。"""

    logger = configure_logging(verbose)
    try:
        sources = load_sources(config)
    except ConfigError as exc:
        typer.secho(f"配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    try:
        # 构造即完成完整性校验：损坏的账本直接拒绝读取。
        ledger = BudgetLedger(db_path)
    except BudgetCorruptError as exc:
        typer.secho(f"预算账本损坏，拒绝读取: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    except BudgetError as exc:
        typer.secho(f"预算账本不可用: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    try:
        today_counter_rows = ledger.list_counters()
        usage_date = (
            today_counter_rows[0].usage_date
            if today_counter_rows
            else utc_today()
        )
        today_rows = {row.source: row for row in today_counter_rows}
    except BudgetError as exc:
        ledger.close()
        typer.secho(f"预算查询失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    typer.echo(f"UTC 自然日: {usage_date}")
    typer.echo(
        f"{'source':<24}{'req_used':>10}{'req_limit':>12}{'req_left':>10}"
        f"{'bytes_used':>12}{'bytes_limit':>14}{'bytes_left':>12}"
    )
    for source in sources:
        row = today_rows.get(source.name)
        requests_used = row.request_count if row is not None else 0
        bytes_used = row.response_bytes if row is not None else 0
        limits = BudgetLimits.from_source(source)
        typer.echo(
            f"{source.name:<24}{requests_used:>10}{_format_limit(limits.daily_request_limit):>12}"
            f"{_format_remaining(requests_used, limits.daily_request_limit):>10}"
            f"{bytes_used:>12}{_format_limit(limits.daily_bytes_limit):>14}"
            f"{_format_remaining(bytes_used, limits.daily_bytes_limit):>12}"
        )
    # 账本里可能有配置已删除的源，也展示出来，避免账外有账。
    configured = {source.name for source in sources}
    orphan_rows = [row for name, row in today_rows.items() if name not in configured]
    for row in orphan_rows:
        logger.warning("账本中存在配置未声明的源 source=%s", row.source)
        typer.echo(
            f"{row.source + '(配置缺失)':<24}{row.request_count:>10}{'不限':>12}"
            f"{'不限':>10}{row.response_bytes:>12}{'不限':>14}{'不限':>12}"
        )
    ledger.close()


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
    mode = "（dry-run，未写 raw 与数据库，预算仅估算）" if dry_run else ""
    typer.echo(f"本轮汇总{mode}")
    for summary in summaries:
        typer.echo(
            f"source={summary.source} ok={summary.ok} failed={summary.failed} "
            f"skipped={summary.skipped} elapsed_ms={summary.elapsed_ms} "
            f"requests={summary.budget_requests} bytes={summary.budget_bytes}"
        )
    if summaries:
        typer.echo(
            f"total ok={sum(s.ok for s in summaries)} "
            f"failed={sum(s.failed for s in summaries)} "
            f"skipped={sum(s.skipped for s in summaries)} "
            f"requests={sum(s.budget_requests for s in summaries)} "
            f"bytes={sum(s.budget_bytes for s in summaries)}"
        )


if __name__ == "__main__":
    app()
