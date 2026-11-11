"""Typer 命令行入口：run / db stats / serve-mock / scheduler。"""

from __future__ import annotations

import signal
import sys
import time
from datetime import timezone
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, SchedulerError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.observability.metrics import MetricsRegistry
from crawl_flow.pipeline.run import RunSummary
from crawl_flow.scheduler.cron import CronParseError
from crawl_flow.scheduler.engine import Scheduler, build_job_plans
from crawl_flow.scheduler.runner import run_collection
from crawl_flow.scheduler.server import MetricsServer, ensure_internal_host
from crawl_flow.scheduler.store import SchedulerStore
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

DEFAULT_CONFIG = Path("config/sources.yaml")
DEFAULT_DB = Path("data/crawl.db")
DEFAULT_RAW_DIR = Path("data/raw")
DEFAULT_METRICS_HOST = "127.0.0.1"
DEFAULT_METRICS_PORT = 9108

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="crawl-flow：公开数据采集 / 解析 / 归一化 / 入库管道",
)
db_app = typer.Typer(help="数据库运维命令", no_args_is_help=True)
app.add_typer(db_app, name="db")
scheduler_app = typer.Typer(help="定时调度与指标运维命令", no_args_is_help=True)
app.add_typer(scheduler_app, name="scheduler")


@app.command("run")
def run_command(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只采集指定源"),
    all_sources: bool = typer.Option(False, "--all", help="采集所有 enabled 源"),
    limit: Optional[int] = typer.Option(None, "--limit", min=1, help="每个源最多处理 N 条"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只解析，不写 raw 与数据库"),
    no_metrics: bool = typer.Option(
        False, "--no-metrics", help="本次执行不写入运行指标（默认写入，与定时任务共享统计）"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录"),
) -> None:
    """跑一轮采集（手动触发，指标默认与定时任务统一累加）。"""

    logger = configure_logging(verbose)
    try:
        sources = load_sources(config)
        selected = select_sources(sources, name=source, all_sources=all_sources)
    except ConfigError as exc:
        typer.secho(f"配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    db = Database(db_path)
    raw_store = RawStore(raw_dir)
    registry = MetricsRegistry(db.session_factory)
    summaries: list[RunSummary] = []
    try:
        db.init()
        with Fetcher() as fetcher:
            for selected_source in selected:
                # 手动与定时共用 run_collection：同一套计数器原子自增，互不冲突
                summary = run_collection(
                    selected_source,
                    db,
                    raw_store,
                    registry,
                    trigger_type="manual",
                    limit=limit,
                    dry_run=dry_run,
                    record_metrics=not no_metrics,
                    logger=logger,
                )
                summaries.append(summary)
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


@scheduler_app.command("start")
def scheduler_start(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录"),
    metrics_host: str = typer.Option(
        DEFAULT_METRICS_HOST, "--metrics-host", help="指标端点监听地址（仅回环/内网）"
    ),
    metrics_port: int = typer.Option(
        DEFAULT_METRICS_PORT, "--metrics-port", min=1, max=65535, help="指标端点端口"
    ),
    limit: Optional[int] = typer.Option(None, "--limit", min=1, help="每轮每个源最多处理 N 条"),
    run_immediately: bool = typer.Option(
        False, "--run-immediately", help="启动后先立即执行一轮，再按 cron 计划调度"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """前台启动单进程调度器与指标端点（Ctrl+C 优雅退出）。"""

    logger = configure_logging(verbose)
    try:
        sources = load_sources(config)
        plans = build_job_plans(sources)
    except (ConfigError, CronParseError) as exc:
        typer.secho(f"配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    if not plans:
        typer.secho(
            "没有可调度的源：请在配置中为 enabled 源添加 schedule.cron",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)

    try:
        ensure_internal_host(metrics_host)
    except ValueError as exc:
        typer.secho(f"指标端点配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    database = Database(db_path)
    try:
        database.init()
        store = SchedulerStore(database)
        store.init()
        # 重启恢复：清理残留租约、标记中断执行、重建 last_fired
        interrupted = store.recover_after_restart()
        if interrupted:
            logger.warning("recover_after_restart marked %d interrupted run(s)", interrupted)
        plans_map = {plan.source.name: plan.cron.expression for plan in plans}
        store.sync_jobs(plans_map)

        registry = MetricsRegistry(database.session_factory)
        server = MetricsServer(
            registry, host=metrics_host, port=metrics_port, logger=logger
        )
        try:
            server.start()
        except OSError as exc:
            typer.secho(f"指标端点启动失败: {exc}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2) from exc

        scheduler = Scheduler(
            plans,
            store,
            registry,
            str(db_path),
            str(raw_dir),
            limit=limit,
            logger=logger,
        )
        scheduler.start(run_immediately=run_immediately)
        typer.echo(
            f"scheduler running: {len(plans)} job(s), metrics at {server.url}/metrics "
            f"（Ctrl+C 退出）"
        )
        for plan in plans:
            next_fire = scheduler.next_fire_times().get(plan.source.name)
            typer.echo(
                f"  source={plan.source.name} cron={plan.cron.expression!r} "
                f"next_fire={next_fire.isoformat() if next_fire else '-'}"
            )

        def _shutdown(signum: int, _frame: object) -> None:
            logger.info("received signal %s, shutting down", signum)
            scheduler.stop()
            server.stop()

        signal.signal(signal.SIGTERM, _shutdown)
        signal.signal(signal.SIGINT, _shutdown)
        while scheduler.is_running():
            time.sleep(0.5)
    except SchedulerError as exc:
        typer.secho(f"调度失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()


@scheduler_app.command("status")
def scheduler_status(
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看定时计划、各源成功率、平均耗时与最近执行时间。"""

    configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    database = Database(db_path)
    try:
        database.init()
        store = SchedulerStore(database)
        store.init()
        jobs = store.list_jobs()
        stats = {row.source: row for row in store.stats()}
        leased = store.list_leased_sources()
    finally:
        database.close()

    if not jobs:
        typer.echo("暂无定时计划（先运行 scheduler start 同步配置）")
        return

    header = (
        f"{'source':<20}{'cron':<16}{'enabled':>8}{'runs':>6}{'ok':>5}"
        f"{'failed':>7}{'skipped':>8}{'success':>9}{'avg_ms':>9}  last_run"
    )
    typer.echo(header)
    for job in jobs:
        row = stats.get(job.source)
        if row is None:
            typer.echo(
                f"{job.source:<20}{job.cron_expr:<16}{str(job.enabled):>8}"
                f"{0:>6}{0:>5}{0:>7}{0:>8}{'-':>9}{'-':>9}  -"
            )
            continue
        success = f"{row.success_rate * 100:.1f}%" if row.success_rate is not None else "-"
        avg_ms = str(row.avg_elapsed_ms) if row.avg_elapsed_ms is not None else "-"
        last_run = row.last_run_at.isoformat() if row.last_run_at else "-"
        running = " (running)" if job.source in leased else ""
        typer.echo(
            f"{job.source:<20}{job.cron_expr:<16}{str(job.enabled):>8}"
            f"{row.total_runs:>6}{row.ok_runs:>5}{row.failed_runs:>7}"
            f"{row.skipped_runs:>8}{success:>9}{avg_ms:>9}  {last_run}{running}"
        )


@scheduler_app.command("history")
def scheduler_history(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只看指定源"),
    trigger: Optional[str] = typer.Option(
        None, "--trigger", help="按触发方式过滤：scheduled / manual"
    ),
    limit: int = typer.Option(20, "--limit", min=1, max=500, help="最多返回 N 条"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看定时 / 手动采集的执行历史。"""

    configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    if trigger is not None and trigger not in ("scheduled", "manual"):
        typer.secho("--trigger 只支持 scheduled 或 manual", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)

    database = Database(db_path)
    try:
        database.init()
        store = SchedulerStore(database)
        store.init()
        runs = store.list_runs(source=source, trigger_type=trigger, limit=limit)
    finally:
        database.close()

    if not runs:
        typer.echo("暂无执行历史")
        return

    typer.echo(
        f"{'run_id':>7} {'source':<18}{'trigger':<10}{'status':<12}"
        f"{'elapsed_ms':>10}  started_at (UTC)"
    )
    for run in runs:
        typer.echo(
            f"{run.id:>7} {run.source:<18}{run.trigger_type:<10}{run.status:<12}"
            f"{(run.elapsed_ms if run.elapsed_ms is not None else '-'):>10}  "
            f"{run.started_at.replace(tzinfo=timezone.utc).isoformat()}"
        )


@scheduler_app.command("metrics")
def scheduler_metrics(
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """本地打印 Prometheus 文本格式指标（无需运行中的调度器）。"""

    configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    database = Database(db_path)
    try:
        database.init()
        registry = MetricsRegistry(database.session_factory)
        typer.echo(registry.render_prometheus(), nl=False)
    finally:
        database.close()


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
