"""Typer 命令行入口：run / probe / db stats / serve-mock。"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import SourceConfig, load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.health import ProbeResult, probe_sources
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
    probe: Optional[bool] = typer.Option(
        None,
        "--probe/--no-probe",
        help="采集前做源健康探测（不传时按源配置 probe_enabled 决定）",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录"),
) -> None:
    """跑一轮采集；可选在采集前探测源健康状况。"""

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
        # 探测只读列表第 1 页、不落盘不写库；结果只用于日志与告警，不改变采集行为。
        _maybe_probe_before_run(selected, db=db, probe=probe, logger=logger)
        for selected_source in selected:
            # 每个源按自身 timeout/UA/headers 建 Fetcher，探测与采集规则保持一致。
            summaries.append(
                run_source(
                    selected_source,
                    db,
                    raw_store,
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


@app.command("probe")
def probe_command(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只探测指定源"),
    all_sources: bool = typer.Option(False, "--all", help="探测所有 enabled 源"),
    persist: bool = typer.Option(
        False,
        "--persist",
        help="把探测结果写入独立的 probe_records 表（默认不落盘不写库）",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
) -> None:
    """探测源健康状况：只请求列表第 1 页，输出健康评级。

    默认不写 raw、不写业务数据库；``--persist`` 时才把结果追加到
    ``probe_records``。探测失败不会中止命令，退出码为 0。
    """

    logger = configure_logging(verbose)
    try:
        sources = load_sources(config)
        selected = select_sources(sources, name=source, all_sources=all_sources)
    except ConfigError as exc:
        typer.secho(f"配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    database: Database | None = None
    try:
        if persist:
            database = Database(db_path)
            database.init()
        probes = probe_sources(selected, db=database, persist=persist, logger=logger)
    except StoreError as exc:
        typer.secho(f"探测结果写入失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        if database is not None:
            database.close()

    _print_probes(probes)


@db_app.command("health")
def db_health(
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看 probe_records 中的历史探测记录。"""

    configure_logging(verbose)
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    database = Database(db_path)
    try:
        database.init()
        rows = database.probe_history()
    except StoreError as exc:
        typer.secho(f"读取探测记录失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()

    typer.echo(
        f"{'source':<20}{'probed_at':<26}{'rating':<12}{'outcome':<16}"
        f"{'http':>6}{'elapsed_ms':>12}"
    )
    for row in rows:
        http_status = row["http_status"] if row["http_status"] is not None else "-"
        typer.echo(
            f"{row['source']:<20}{_format_dt(row['probed_at']):<26}"
            f"{row['rating']:<12}{row['outcome']:<16}{str(http_status):>6}"
            f"{row['elapsed_ms']:>12}"
        )


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


def _maybe_probe_before_run(
    sources: list[SourceConfig],
    *,
    db: Database,
    probe: bool | None,
    logger: logging.Logger,
) -> None:
    """按 CLI 开关 / 源配置决定是否在采集前探测。

    ``--probe`` 强制开启、``--no-probe`` 强制关闭；都不传时仅探测源配置里
    ``probe_enabled: true`` 的源。探测结果只写日志，不落 raw、不写数据库，
    也不改变采集流程。
    """

    if probe is False:
        return
    targets = sources if probe is True else [s for s in sources if s.probe_enabled]
    if not targets:
        return
    try:
        probe_sources(targets, db=db, persist=False, logger=logger)
    except StoreError as exc:  # 读取历史失败时退化为无历史探测
        logger.warning("probe history unavailable: %s", exc)


def _print_probes(probes: list[ProbeResult]) -> None:
    typer.echo(
        f"{'source':<20}{'rating':<12}{'outcome':<16}{'http':>6}"
        f"{'elapsed_ms':>12}{'failure_rate':>14}{'avg_ms':>10}"
    )
    for probe in probes:
        http_status = probe.http_status if probe.http_status is not None else "-"
        failure_rate = (
            "-" if probe.history_failure_rate is None else f"{probe.history_failure_rate:.3f}"
        )
        avg_ms = "-" if probe.avg_response_ms is None else f"{probe.avg_response_ms:.1f}"
        typer.echo(
            f"{probe.source:<20}{probe.rating:<12}{probe.outcome:<16}"
            f"{str(http_status):>6}{probe.elapsed_ms:>12}{failure_rate:>14}{avg_ms:>10}"
        )


def _format_dt(value: object) -> str:
    if value is None:
        return "-"
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S")  # type: ignore[no-any-return]
    return str(value)


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


if __name__ == "__main__":    app()
