"""Typer 命令行入口：run / db stats / rules check / serve-mock。"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.rules.engine import RuleSet, load_rule_set
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

DEFAULT_CONFIG = Path("config/sources.yaml")
DEFAULT_RULES = Path("config/rules.yaml")
DEFAULT_DB = Path("data/crawl.db")
DEFAULT_RAW_DIR = Path("data/raw")

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="crawl-flow：公开数据采集 / 解析 / 归一化 / 入库管道",
)
db_app = typer.Typer(help="数据库运维命令", no_args_is_help=True)
app.add_typer(db_app, name="db")
rules_app = typer.Typer(help="规则引擎运维命令", no_args_is_help=True)
app.add_typer(rules_app, name="rules")


@app.command("run")
def run_command(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只采集指定源"),
    all_sources: bool = typer.Option(False, "--all", help="采集所有 enabled 源"),
    limit: Optional[int] = typer.Option(None, "--limit", min=1, help="每个源最多处理 N 条"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只解析，不写 raw 与数据库"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
    config: Path = typer.Option(DEFAULT_CONFIG, "--config", help="源配置文件路径"),
    rules: Path = typer.Option(DEFAULT_RULES, "--rules", help="规则配置文件路径"),
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

    # 规则配置有问题不阻断采集：明确提示后降级为默认清洗逻辑。
    rule_set = _load_rule_set_or_degrade(rules)

    db = Database(db_path)
    raw_store = RawStore(raw_dir)
    summaries: list[RunSummary] = []
    try:
        db.init()
        with Fetcher() as fetcher:
            for selected_source in selected:
                engine = rule_set.engine_for(selected_source.name) if rule_set else None
                if rule_set is not None and engine is None:
                    logger.info(
                        "event=rules_disabled source=%s reason=该源未配置规则，使用默认清洗",
                        selected_source.name,
                    )
                summaries.append(
                    run_source(
                        selected_source,
                        db,
                        raw_store,
                        fetcher=fetcher,
                        limit=limit,
                        dry_run=dry_run,
                        logger=logger,
                        rule_engine=engine,
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


def _load_rule_set_or_degrade(rules_path: Path) -> RuleSet | None:
    """加载规则配置；文件缺失或校验失败时提示并返回 None（默认清洗）。"""

    if not rules_path.exists():
        if rules_path != DEFAULT_RULES:
            typer.secho(
                f"规则配置不存在: {rules_path}，本轮使用默认清洗逻辑",
                fg=typer.colors.YELLOW,
                err=True,
            )
        return None
    try:
        rule_set = load_rule_set(str(rules_path))
    except ConfigError as exc:
        typer.secho(
            f"规则配置错误: {exc}，本轮使用默认清洗逻辑",
            fg=typer.colors.YELLOW,
            err=True,
        )
        return None
    return rule_set


@rules_app.command("check")
def rules_check(
    rules: Path = typer.Option(DEFAULT_RULES, "--rules", help="规则配置文件路径"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """启动时校验规则配置：语法、算子参数、正则全部检查，不跑采集。"""

    configure_logging(verbose)
    try:
        rule_set = load_rule_set(str(rules))
    except ConfigError as exc:
        typer.secho(f"规则配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    typer.secho(f"规则配置有效: {rules}", fg=typer.colors.GREEN)
    for line in rule_set.describe():
        typer.echo(f"  {line}")


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
            f"skipped={summary.skipped} low_quality={summary.low_quality} "
            f"elapsed_ms={summary.elapsed_ms}"
        )
    if summaries:
        typer.echo(
            f"total ok={sum(s.ok for s in summaries)} "
            f"failed={sum(s.failed for s in summaries)} "
            f"skipped={sum(s.skipped for s in summaries)} "
            f"low_quality={sum(s.low_quality for s in summaries)}"
        )


if __name__ == "__main__":
    app()
