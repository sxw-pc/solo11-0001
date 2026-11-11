"""Typer 命令行入口：run / db stats / serve-mock / schedule / metrics。"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.observability.metrics import render_metrics, summarize_sources
from crawl_flow.observability.metrics_server import MetricsServer
from crawl_flow.pipeline.run import RunSummary
from crawl_flow.scheduler.cron import CronSchedule
from crawl_flow.scheduler.runner import execute_and_record
from crawl_flow.scheduler.service import SchedulerService
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
schedule_app = typer.Typer(help="定时调度：常驻调度器与执行历史", no_args_is_help=True)
app.add_typer(schedule_app, name="schedule")


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
    """跑一轮采集（手动触发，指标与定时任务共用同一份统计）。"""

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
                    execute_and_record(
                        selected_source,
                        db,
                        raw_store,
                        trigger="manual",
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


@schedule_app.command("start")
def schedule_start(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录"),
    limit: Optional[int] = typer.Option(None, "--limit", min=1, help="每个源每轮最多处理 N 条"),
    metrics_host: str = typer.Option(
        DEFAULT_METRICS_HOST, "--metrics-host", help="指标端点监听地址（默认仅本机）"
    ),
    metrics_port: int = typer.Option(
        DEFAULT_METRICS_PORT, "--metrics-port", min=1, max=65535, help="指标端点端口"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """前台启动常驻调度器，Ctrl+C 停止。

    为所有配置了 ``schedule`` 且 enabled 的源建立定时计划；同时在
    ``--metrics-host:--metrics-port`` 暴露 ``/metrics``（Prometheus 文本格式，
    默认仅监听 127.0.0.1）。进程重启后定时计划从配置文件自动恢复。
    """

    logger = configure_logging(verbose)
    try:
        sources = load_sources(config)
    except ConfigError as exc:
        typer.secho(f"配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    service = SchedulerService(
        sources, db_path=db_path, raw_dir=raw_dir, limit=limit, logger=logger
    )
    if service.job_count == 0:
        service.stop()
        typer.secho(
            "没有任何 enabled 源配置 schedule，请在 sources.yaml 中为源添加 cron 计划",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    metrics_db = Database(db_path)
    metrics_db.init()

    def _render() -> str:
        return render_metrics(
            metrics_db.job_run_aggregates(), scheduler_state=service.jobs_snapshot()
        )

    metrics_server = MetricsServer(metrics_host, metrics_port, _render)
    metrics_server.start()
    typer.echo(f"调度器已启动：{service.job_count} 个定时任务")
    for job in service.jobs_snapshot():
        typer.echo(f"  {job['source']:<24} {job['schedule']:<16} 下次触发 {job['next_fire']}")
    typer.echo(f"指标端点: http://{metrics_host}:{metrics_server.port}/metrics （仅本机访问）")

    service.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        typer.echo("收到中断信号，正在停止调度器…")
    finally:
        service.stop()
        metrics_server.stop()
        metrics_db.close()


@schedule_app.command("list")
def schedule_list(
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """列出各源的定时计划与下次触发时间。"""

    configure_logging(verbose)
    try:
        sources = load_sources(config)
    except ConfigError as exc:
        typer.secho(f"配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    now = datetime.now().astimezone()
    typer.echo(f"{'source':<24}{'enabled':>8}  {'schedule':<16}  next_fire")
    scheduled = 0
    for source in sources:
        if source.schedule is None:
            typer.echo(f"{source.name:<24}{str(source.enabled):>8}  {'-':<16}  -")
            continue
        scheduled += 1
        next_fire = CronSchedule(source.schedule).next_after(now)
        typer.echo(
            f"{source.name:<24}{str(source.enabled):>8}  {source.schedule:<16}  {next_fire}"
        )
    if scheduled == 0:
        typer.echo("（没有任何源配置 schedule）")


@schedule_app.command("history")
def schedule_history(
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只看指定源"),
    limit: int = typer.Option(20, "--limit", min=1, max=500, help="最近记录条数"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看执行历史、成功率与平均耗时（手动与定时触发都统计在内）。"""

    configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    database = Database(db_path)
    try:
        database.init()
        summaries = summarize_sources(database.job_run_aggregates())
        recent = database.recent_job_runs(source=source, limit=limit)
    except StoreError as exc:
        typer.secho(f"查询失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()

    if source is not None:
        summaries = [row for row in summaries if row["source"] == source]

    typer.echo("== 按源汇总 ==")
    typer.echo(
        f"{'source':<24}{'runs':>6}{'ok':>6}{'failed':>8}{'skipped':>9}"
        f"{'success%':>10}{'avg_ms':>9}  last_status  last_started_at"
    )
    for row in summaries:
        rate = "-" if row["success_rate"] is None else f"{row['success_rate'] * 100:.1f}"
        avg_ms = "-" if row["avg_ms"] is None else f"{row['avg_ms']:.0f}"
        typer.echo(
            f"{row['source']:<24}{row['runs']:>6}{row['succeeded']:>6}{row['failed']:>8}"
            f"{row['skipped']:>9}{rate:>10}{avg_ms:>9}  {row['last_status'] or '-':<11}"
            f"  {row['last_started_at'] or '-'}"
        )
    if not summaries:
        typer.echo("（暂无执行记录）")

    typer.echo("")
    typer.echo(f"== 最近 {len(recent)} 条执行记录 ==")
    typer.echo(
        f"{'id':>6}  {'source':<20}{'trigger':<10}{'status':<11}{'ok':>5}{'failed':>7}"
        f"{'skipped':>8}{'ms':>7}  started_at  error"
    )
    for row in recent:
        error = (row["error"] or "").replace("\n", " ")[:60]
        typer.echo(
            f"{row['id']:>6}  {row['source']:<20}{row['trigger']:<10}{row['status']:<11}"
            f"{row['ok_count']:>5}{row['failed_count']:>7}{row['skipped_count']:>8}"
            f"{row['elapsed_ms']:>7}  {row['started_at']}  {error}"
        )
    if not recent:
        typer.echo("（暂无执行记录）")


@app.command("metrics")
def metrics_command(
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """以 Prometheus 文本格式输出当前指标（与 /metrics 端点内容一致）。"""

    configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    database = Database(db_path)
    try:
        database.init()
        text = render_metrics(database.job_run_aggregates())
    except StoreError as exc:
        typer.secho(f"指标读取失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()
    typer.echo(text, nl=False)


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
