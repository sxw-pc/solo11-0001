"""Typer 命令行入口：run / db stats / serve-mock。

CLI 是唯一的错误边界：参数与路径在发请求前由
:mod:`crawl_flow.cli_validation` 统一校验，所有可预期失败都在这里转成
可读的一行错误（不打印堆栈），参数/配置/存储/采集类失败退出码 2，
``db stats`` 在库文件不存在时退出码 1。
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import click
import typer

from crawl_flow.cli_validation import (
    as_absolute_path,
    validate_config_path,
    validate_db_path,
    validate_limit,
    validate_raw_dir,
)
from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import CliArgumentError, ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

DEFAULT_CONFIG = Path("config/sources.yaml")
DEFAULT_DB = Path("data/crawl.db")
DEFAULT_RAW_DIR = Path("data/raw")

# 关闭 Click Path 自带的 exists/readable 解析期校验：文件系统可达性统一由
# cli_validation / config 层处理，保证错误出口与文案收敛到一处。
# 元变量仍是 PATH，--help 输出不变。
PATH_TYPE = click.Path(
    path_type=Path, exists=False, file_okay=True, dir_okay=True, readable=False
)

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="crawl-flow：公开数据采集 / 解析 / 归一化 / 入库管道",
)
db_app = typer.Typer(help="数据库运维命令", no_args_is_help=True)
app.add_typer(db_app, name="db")


def _fail(message: str) -> "typer.Exit":
    """统一错误出口：可读的一行 stderr，退出码 2，不带堆栈。"""

    typer.secho(message, fg=typer.colors.RED, err=True)
    return typer.Exit(code=2)


@app.command("run")
def run_command(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="只采集指定源"),
    all_sources: bool = typer.Option(False, "--all", help="采集所有 enabled 源"),
    limit: Optional[int] = typer.Option(None, "--limit", min=1, help="每个源最多处理 N 条"),
    dry_run: bool = typer.Option(False, "--dry-run", help="只解析，不写 raw 与数据库"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
    config: Path = typer.Option(
        DEFAULT_CONFIG, "--config", help="源配置文件路径", click_type=PATH_TYPE
    ),
    db_path: Path = typer.Option(
        DEFAULT_DB, "--db", help="SQLite 数据库路径", click_type=PATH_TYPE
    ),
    raw_dir: Path = typer.Option(
        DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录", click_type=PATH_TYPE
    ),
) -> None:
    """跑一轮采集。"""

    logger = configure_logging(verbose)

    # 统一校验层：任何非法值 / 不可用路径都在发请求前失败，dry-run 与正常模式一致。
    try:
        limit = validate_limit(limit)
        config_path = validate_config_path(config)
        resolved_db_path = validate_db_path(db_path)
        resolved_raw_dir = validate_raw_dir(raw_dir)
        sources = load_sources(config_path)
        selected = select_sources(sources, name=source, all_sources=all_sources)
    except CliArgumentError as exc:
        raise _fail(f"参数错误: {exc}") from exc
    except ConfigError as exc:
        raise _fail(f"配置错误: {exc}") from exc
    except OSError as exc:
        raise _fail(f"参数错误: 路径不可访问: {exc}") from exc

    db: Optional[Database] = None
    raw_store = RawStore(resolved_raw_dir)
    summaries: list[RunSummary] = []
    try:
        db = Database(resolved_db_path)
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
        raise _fail(f"采集失败（整轮中止）: {exc}") from exc
    except StoreError as exc:
        raise _fail(f"存储失败: {exc}") from exc
    except OSError as exc:
        # 兜底：运行期 I/O 失败同样只给可读信息，不向用户打印堆栈。
        raise _fail(f"运行失败: {exc.__class__.__name__}: {exc}") from exc
    finally:
        if db is not None:
            db.close()

    _print_summaries(summaries, dry_run=dry_run)


@db_app.command("stats")
def db_stats(
    db_path: Path = typer.Option(
        DEFAULT_DB, "--db", help="SQLite 数据库路径", click_type=PATH_TYPE
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """查看每个源的 records / raw 统计。"""

    configure_logging(verbose)
    resolved_db_path = as_absolute_path(db_path)
    if not resolved_db_path.exists():
        typer.secho(f"数据库不存在: {resolved_db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    database = Database(resolved_db_path)
    try:
        database.init()
        stats = database.stats()
    except StoreError as exc:
        raise _fail(f"统计失败: {exc}") from exc
    except OSError as exc:
        raise _fail(f"统计失败: {exc.__class__.__name__}: {exc}") from exc
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
