"""Typer 命令行入口：run / db stats / serve-mock。"""

from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.concurrent import (
    ThreadSafeDatabase,
    ThreadSafeRawStore,
    run_source_concurrent,
)
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.scheduler import (
    SchedulerConfig,
    create_scheduler,
    load_scheduler_yaml,
)
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

DEFAULT_CONFIG = Path("config/sources.yaml")
DEFAULT_SCHEDULER_CONFIG = Path("config/scheduler.yaml")
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
    scheduler_config: Path = typer.Option(
        DEFAULT_SCHEDULER_CONFIG, "--scheduler-config", help="调度器配置文件路径"
    ),
    scheduled: bool = typer.Option(
        False, "--scheduled/--no-scheduled", help="启用并发调度与限流（默认关闭，保持串行）"
    ),
    max_concurrency: Optional[int] = typer.Option(
        None, "--max-concurrency", min=1, help="覆盖全局并发上限"
    ),
    qps: Optional[float] = typer.Option(None, "--qps", min=0.0, help="覆盖全局 QPS 上限"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录"),
) -> None:
    """跑一轮采集。"""

    logger = configure_logging(verbose)
    try:
        sources = load_sources(config)
        selected = select_sources(sources, name=source, all_sources=all_sources)
        schedule_cfg = (
            _resolve_scheduler_config(
                scheduler_config,
                max_concurrency=max_concurrency,
                qps=qps,
            )
            if scheduled
            else None
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
            if not scheduled:
                # 原有串行路径，行为与调度器完全无关。
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
            else:
                assert schedule_cfg is not None
                scheduler = create_scheduler(schedule_cfg, fetcher, logger=logger)
                try:
                    summaries = _run_scheduled(
                        selected,
                        db=db,
                        raw_store=raw_store,
                        scheduler=scheduler,
                        fetcher=fetcher,
                        limit=limit,
                        dry_run=dry_run,
                        logger=logger,
                    )
                finally:
                    scheduler.close()
    except FetchError as exc:
        typer.secho(f"采集失败（整轮中止）: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    except StoreError as exc:
        typer.secho(f"存储失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        db.close()

    _print_summaries(summaries, dry_run=dry_run, concurrent=scheduled)


def _resolve_scheduler_config(
    path: Path,
    *,
    max_concurrency: int | None,
    qps: float | None,
) -> SchedulerConfig:
    """加载调度配置并应用 CLI 覆盖项。"""

    config = load_scheduler_yaml(path)
    updates: dict[str, object] = {}
    if max_concurrency is not None:
        updates["max_concurrency"] = max_concurrency
    if qps is not None and qps > 0:
        updates["qps"] = qps
    if updates:
        config = config.model_copy(update=updates)
    return config


def _run_scheduled(
    selected: list,
    *,
    db: Database,
    raw_store: RawStore,
    scheduler,
    fetcher: Fetcher,
    limit: int | None,
    dry_run: bool,
    logger,
) -> list[RunSummary]:
    """多源在同一调度池内并发运行；单源失败不影响其他源，结束后统一抛出。"""

    safe_db = ThreadSafeDatabase(db)
    safe_raw = ThreadSafeRawStore(raw_store)
    summaries: list[RunSummary] = []

    def _run_one(selected_source) -> RunSummary:
        return run_source_concurrent(
            selected_source,
            safe_db,
            safe_raw,
            scheduler,
            fetcher=fetcher,
            limit=limit,
            dry_run=dry_run,
            logger=logger,
        )

    if len(selected) == 1:
        return [_run_one(selected[0])]

    first_error: Exception | None = None
    with ThreadPoolExecutor(
        max_workers=len(selected), thread_name_prefix="crawl-source"
    ) as pool:
        future_map = {
            pool.submit(_run_one, selected_source): selected_source
            for selected_source in selected
        }
        for future, selected_source in future_map.items():
            try:
                summaries.append(future.result())
            except Exception as exc:  # noqa: BLE001 - 保留首个错误，其他源继续
                if first_error is None:
                    first_error = exc
                logger.error(
                    "source aborted source=%s error=%s",
                    selected_source.name,
                    exc.__class__.__name__,
                )
    summaries.sort(key=lambda item: item.source)
    if first_error is not None:
        raise first_error
    return summaries


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


def _print_summaries(
    summaries: list[RunSummary], *, dry_run: bool, concurrent: bool = False
) -> None:
    mode = "（dry-run，未写 raw 与数据库）" if dry_run else ""
    if concurrent:
        mode = f"{mode}（并发调度模式）" if mode else "（并发调度模式）"
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
