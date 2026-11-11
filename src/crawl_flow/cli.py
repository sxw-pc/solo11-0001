"""Typer 命令行入口：run / watch hits / db stats / serve-mock。"""

from __future__ import annotations

import sys
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import AppConfig, load_app_config, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore
from crawl_flow.watch import WatchEngine
from crawl_flow.watch.engine import build_watch_engine

DEFAULT_CONFIG = Path("config/sources.yaml")
DEFAULT_DB = Path("data/crawl.db")
DEFAULT_RAW_DIR = Path("data/raw")

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="crawl-flow：公开数据采集 / 解析 / 归一化 / 入库 / 关键词布控管道",
)
db_app = typer.Typer(help="数据库运维命令", no_args_is_help=True)
app.add_typer(db_app, name="db")
watch_app = typer.Typer(help="关键词布控命令", no_args_is_help=True)
app.add_typer(watch_app, name="watch")


def _load_config(config: Path) -> AppConfig:
    """加载并校验整份配置（含布控规则，非法规则启动即失败）。"""

    return load_app_config(config)


def _build_engine(config: AppConfig, logger) -> WatchEngine | None:
    """汇总全局规则与各源规则，构建布控引擎。"""

    sources_rules: dict[str, list] = {}
    for source in config.sources:
        rules = list(config.watch.rules)
        if source.watch is not None:
            rules = rules + list(source.watch.rules)
        sources_rules[source.name] = rules
    return build_watch_engine(config.watch, sources_rules, logger=logger)


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
    """跑一轮采集（归一化后、入库前自动过布控引擎）。"""

    logger = configure_logging(verbose)
    try:
        app_config = _load_config(config)
        selected = select_sources(app_config.sources, name=source, all_sources=all_sources)
        watch_engine = _build_engine(app_config, logger)
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
                        watch_engine=watch_engine,
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


def _parse_time_bound(value: Optional[str], *, is_end: bool) -> Optional[datetime]:
    """把 CLI 的日期/时间字符串解析成 UTC 无时区 datetime，便于和库里比较。

    支持 ``YYYY-MM-DD``（起始取当天 00:00、结束取当天 23:59:59）与
    ``YYYY-MM-DDTHH:MM:SS``（可带时区，带时区时转 UTC）。
    """

    if value is None:
        return None
    text = value.strip()
    try:
        if "T" in text or " " in text:
            parsed = datetime.fromisoformat(text)
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(timezone.utc).replace(tzinfo=None)
            return parsed
        day = datetime.strptime(text, "%Y-%m-%d")
        return datetime.combine(day, time.max.replace(microsecond=0)) if is_end else day
    except ValueError as exc:
        raise ConfigError(f"无法解析时间 {value!r}，请使用 YYYY-MM-DD 或 ISO 日期时间") from exc


@watch_app.command("hits")
def watch_hits_command(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="按源名过滤"),
    rule: Optional[str] = typer.Option(None, "--rule", "-r", help="按布控规则名过滤"),
    start: Optional[str] = typer.Option(None, "--start", help="命中时间下界 YYYY-MM-DD[THH:MM:SS]"),
    end: Optional[str] = typer.Option(None, "--end", help="命中时间上界 YYYY-MM-DD[THH:MM:SS]"),
    limit: int = typer.Option(50, "--limit", min=1, max=1000, help="最多返回 N 条"),
    offset: int = typer.Option(0, "--offset", min=0, help="跳过前 N 条（分页）"),
    as_json: bool = typer.Option(False, "--json", help="以 JSON 输出，便于脚本消费"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """按源 / 规则 / 时间范围翻查布控命中清单。"""

    configure_logging(verbose)
    try:
        start_dt = _parse_time_bound(start, is_end=False)
        end_dt = _parse_time_bound(end, is_end=True)
    except ConfigError as exc:
        typer.secho(f"参数错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    database = Database(db_path)
    try:
        database.init()
        rows = database.query_watch_hits(
            source=source,
            rule_name=rule,
            start=start_dt,
            end=end_dt,
            limit=limit,
            offset=offset,
        )
        total = database.count_watch_hits(
            source=source,
            rule_name=rule,
            start=start_dt,
            end=end_dt,
        )
    except StoreError as exc:
        typer.secho(f"查询失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()

    if as_json:
        import json

        payload = [
            {
                "source": row.source,
                "external_id": row.external_id,
                "rule_name": row.rule_name,
                "field": row.field,
                "rule_type": row.rule_type,
                "matched": row.matched,
                "context": row.context,
                "hit_at": row.hit_at.isoformat(sep=" "),
            }
            for row in rows
        ]
        typer.echo(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    typer.echo(f"命中 {total} 条，本次返回 {len(rows)} 条（offset={offset} limit={limit}）")
    if not rows:
        return
    for row in rows:
        typer.echo("-" * 88)
        typer.echo(
            f"hit_at={row.hit_at.isoformat(sep=' ')} source={row.source} "
            f"external_id={row.external_id}"
        )
        typer.echo(f"rule={row.rule_name} type={row.rule_type} field={row.field}")
        typer.echo(f"matched={row.matched}")
        typer.echo(f"context={row.context}")


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


if __name__ == "__main__":
    app()
