"""Typer 命令行入口：run / db stats / serve-mock / rules check。"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, RuleConfigError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.rules import RuleEngine, load_rules
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
    rules: Path = typer.Option(
        DEFAULT_RULES, "--rules", help="规则配置文件路径（缺失或校验失败时降级为默认清洗）"
    ),
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

    rule_engine = _load_rule_engine(rules)

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
                        rule_engine=rule_engine,
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


def _load_rule_engine(path: Path) -> RuleEngine | None:
    """启动时加载并编译规则；配置有问题时降级为默认清洗，不中断任务。"""

    logger = logging.getLogger("crawl_flow.rules")
    if not path.is_file():
        logger.info("rules config not found, use default cleaning path=%s", path)
        return None
    try:
        rule_config = load_rules(path)
        engine = RuleEngine(rule_config, logger=logger)
    except RuleConfigError as exc:
        typer.secho(
            f"规则配置错误: {exc}（本轮采集降级为默认清洗逻辑）",
            fg=typer.colors.YELLOW,
            err=True,
        )
        logger.warning("rules config invalid, fallback to default cleaning: %s", exc)
        return None
    logger.info("rules loaded path=%s sources=%d", path, len(rule_config.sources))
    return engine


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


@rules_app.command("check")
def rules_check(
    config: Path = typer.Option(DEFAULT_RULES, "--config", help="规则配置文件路径"),
) -> None:
    """启动前校验规则配置：语法、算子参数、正则与日期格式。"""

    try:
        rule_config = load_rules(config)
        RuleEngine(rule_config)  # 编译全部规则，暴露算子名/参数等语义错误
    except RuleConfigError as exc:
        typer.secho(f"规则配置错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    typer.secho(f"规则配置有效: {config}", fg=typer.colors.GREEN)
    typer.echo(
        f"defaults threshold={rule_config.defaults.threshold} "
        f"on_quality_failure={rule_config.defaults.on_quality_failure}"
    )
    for name, source_rules in sorted(rule_config.sources.items()):
        rule_count = sum(len(field.rules) for field in source_rules.fields.values())
        typer.echo(
            f"  source={name} fields={len(source_rules.fields)} rules={rule_count}"
        )
    if rule_config.default is not None:
        rule_count = sum(len(field.rules) for field in rule_config.default.fields.values())
        typer.echo(
            f"  default 段 fields={len(rule_config.default.fields)} rules={rule_count}"
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
