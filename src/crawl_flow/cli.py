"""Typer 命令行入口：run / db stats / raw search / raw export / serve-mock。"""

from __future__ import annotations

import json
import sys
from datetime import datetime, time
from pathlib import Path
from typing import Optional

import typer

from crawl_flow.config import load_sources, select_sources
from crawl_flow.errors import ConfigError, FetchError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.models import RawRecord
from crawl_flow.observability.logging import configure_logging
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.db import (
    DEFAULT_SEARCH_LIMIT,
    MAX_SEARCH_LIMIT,
    Database,
    RawSearchFilter,
    normalize_naive_utc,
)
from crawl_flow.storage.raw_store import (
    DEFAULT_BODY_SCAN_LIMIT,
    RawContent,
    RawStore,
)

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
raw_app = typer.Typer(help="原始响应检索与导出命令", no_args_is_help=True)
app.add_typer(raw_app, name="raw")


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


def _parse_time_bound(value: str | None, *, end_of_day: bool) -> datetime | None:
    """解析检索时间参数：完整 ISO 时间，或 ``YYYY-MM-DD``（结束边界取当天 23:59:59.999999）。"""

    if value is None:
        return None
    text = value.strip()
    try:
        if "T" not in text and " " not in text and len(text) == 10:
            day = datetime.strptime(text, "%Y-%m-%d").date()
            return datetime.combine(
                day, time.max if end_of_day else time.min
            )
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise typer.BadParameter(
            f"时间格式应为 YYYY-MM-DD 或 ISO 8601 时间，实际为: {value!r}"
        ) from exc
    if parsed.tzinfo is not None:
        parsed = normalize_naive_utc(parsed)
    return parsed


def _build_filter(
    *,
    source: str | None,
    external_id: str | None,
    status: str | None,
    error_keyword: str | None,
    start_text: str | None,
    end_text: str | None,
) -> RawSearchFilter:
    if status is not None and status not in {"ok", "failed"}:
        raise typer.BadParameter("--status 只允许 ok 或 failed")
    return RawSearchFilter(
        source=source,
        external_id=external_id,
        status=status,
        error_keyword=error_keyword,
        start=_parse_time_bound(start_text, end_of_day=False),
        end=_parse_time_bound(end_text, end_of_day=True),
    )


def _serialize_raw(record: RawRecord) -> dict[str, object]:
    return {
        "source": record.source,
        "external_id": record.external_id,
        "url": record.url,
        "http_status": record.http_status,
        "fetched_at": record.fetched_at.isoformat() if record.fetched_at else None,
        "status": record.status,
        "error": record.error,
        "raw_path": record.raw_path,
    }


def _open_database(db_path: Path) -> Database:
    if not db_path.exists():
        typer.secho(f"数据库不存在: {db_path}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)
    database = Database(db_path)
    database.init()
    return database


def _run_body_search(
    database: Database,
    raw_store: RawStore,
    search_filter: RawSearchFilter,
    *,
    body_keyword: str,
    limit: int,
    offset: int,
    max_scan: int,
) -> tuple[list[RawRecord], int, list[RawContent], int, bool]:
    """正文关键词检索：SQL 先筛候选，再分批按需读 raw 文件做正文子串匹配。

    返回 (命中行, 候选总数, 坏文件读取结果, 已扫描文件数, 是否达到扫描上限)。
    """

    matched: list[RawRecord] = []
    read_errors: list[RawContent] = []
    scanned = 0
    page_size = max(limit, DEFAULT_SEARCH_LIMIT)
    page_offset = offset
    total = database.search_raw(search_filter, limit=1, offset=offset).total
    remaining = max(total - offset, 0)
    scan_cap = min(max_scan, remaining)
    while len(matched) < limit and scanned < scan_cap:
        batch_limit = min(page_size, scan_cap - scanned)
        page = database.search_raw(
            search_filter, limit=batch_limit, offset=page_offset
        )
        if not page.rows:
            break
        result = raw_store.filter_by_body(page.rows, body_keyword)
        matched.extend(result.matched)
        read_errors.extend(result.read_errors)
        scanned += len(page.rows)
        page_offset += len(page.rows)
        if len(page.rows) < batch_limit:
            break
    truncated = scanned >= scan_cap and offset + scanned < total
    return matched[:limit], total, read_errors, scanned, truncated


@raw_app.command("search")
def raw_search(
    source: Optional[str] = typer.Option(None, "--source", "-s", help="按源名称精确过滤"),
    external_id: Optional[str] = typer.Option(
        None, "--external-id", "--id", help="按外部 ID 精确过滤"
    ),
    status: Optional[str] = typer.Option(
        None, "--status", help="按采集状态过滤：ok / failed"
    ),
    error_keyword: Optional[str] = typer.Option(
        None, "--error-keyword", help="按错误摘要关键词过滤（数据库 LIKE，不读 raw 文件）"
    ),
    body_keyword: Optional[str] = typer.Option(
        None,
        "--body-keyword",
        help="按响应正文关键词过滤（按需加载 raw 文件全文检索，代价较高）",
    ),
    start: Optional[str] = typer.Option(
        None, "--from", help="采集时间下界，YYYY-MM-DD 或 ISO 8601（含边界）"
    ),
    end: Optional[str] = typer.Option(
        None, "--to", help="采集时间上界，YYYY-MM-DD 或 ISO 8601（含边界）"
    ),
    limit: int = typer.Option(
        DEFAULT_SEARCH_LIMIT, "--limit", min=1, max=MAX_SEARCH_LIMIT, help="最多返回条数"
    ),
    offset: int = typer.Option(0, "--offset", min=0, help="跳过前 N 条（分页）"),
    format: str = typer.Option("table", "--format", help="输出格式：table / json"),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录"),
    max_scan: int = typer.Option(
        DEFAULT_BODY_SCAN_LIMIT,
        "--max-scan",
        min=1,
        help="正文检索最多扫描的候选 raw 文件数",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """按源/时间/状态/错误关键词/外部 ID 检索 raw_records，可选正文全文匹配。"""

    configure_logging(verbose)
    if format not in {"table", "json"}:
        typer.secho("--format 只允许 table 或 json", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2)
    try:
        search_filter = _build_filter(
            source=source,
            external_id=external_id,
            status=status,
            error_keyword=error_keyword,
            start_text=start,
            end_text=end,
        )
    except typer.BadParameter as exc:
        typer.secho(f"参数错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    database = _open_database(db_path)
    raw_store = RawStore(raw_dir)
    try:
        if body_keyword is None:
            page = database.search_raw(search_filter, limit=limit, offset=offset)
            rows, total = page.rows, page.total
            read_errors: list[RawContent] = []
            scanned, truncated = 0, False
        else:
            try:
                rows, total, read_errors, scanned, truncated = _run_body_search(
                    database,
                    raw_store,
                    search_filter,
                    body_keyword=body_keyword,
                    limit=limit,
                    offset=offset,
                    max_scan=max_scan,
                )
            except StoreError as exc:
                typer.secho(f"检索失败: {exc}", fg=typer.colors.RED, err=True)
                raise typer.Exit(code=2) from exc
    except StoreError as exc:
        typer.secho(f"检索失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()

    for content in read_errors:
        typer.secho(
            f"警告: 跳过无法读取的 raw 文件 path={content.path} reason={content.error}",
            fg=typer.colors.YELLOW,
            err=True,
        )
    if truncated:
        typer.secho(
            f"警告: 正文检索已达扫描上限 {scanned}，仍有候选未检查，可调大 --max-scan",
            fg=typer.colors.YELLOW,
            err=True,
        )

    if format == "json":
        typer.echo(
            json.dumps(
                {
                    "total": total,
                    "returned": len(rows),
                    "limit": limit,
                    "offset": offset,
                    "body_scanned": scanned,
                    "read_errors": [
                        {"path": item.path, "error": item.error} for item in read_errors
                    ],
                    "results": [_serialize_raw(record) for record in rows],
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    typer.echo(
        f"{'source':<18}{'external_id':<16}{'status':<8}{'http':>6}  fetched_at            error"
    )
    for record in rows:
        error_text = (record.error or "")[:48]
        fetched = record.fetched_at.isoformat(sep=" ") if record.fetched_at else "-"
        typer.echo(
            f"{record.source:<18}{record.external_id:<16}{record.status:<8}"
            f"{(record.http_status if record.http_status is not None else '-'):>6}  "
            f"{fetched:<20}{error_text}"
        )
    scope = f"，正文扫描 {scanned} 个 raw 文件" if body_keyword is not None else ""
    typer.echo(f"共 {total} 条满足条件，返回 {len(rows)} 条（limit={limit}）{scope}")


@raw_app.command("export")
def raw_export(
    output: Path = typer.Option(..., "--output", "-o", help="导出文件路径（.json）"),
    source: Optional[str] = typer.Option(None, "--source", "-s", help="按源名称精确过滤"),
    external_id: Optional[str] = typer.Option(
        None, "--external-id", "--id", help="按外部 ID 精确过滤"
    ),
    status: Optional[str] = typer.Option(
        None, "--status", help="按采集状态过滤：ok / failed"
    ),
    error_keyword: Optional[str] = typer.Option(
        None, "--error-keyword", help="按错误摘要关键词过滤（数据库 LIKE，不读 raw 文件）"
    ),
    body_keyword: Optional[str] = typer.Option(
        None,
        "--body-keyword",
        help="按响应正文关键词过滤（按需加载 raw 文件全文检索，代价较高）",
    ),
    start: Optional[str] = typer.Option(
        None, "--from", help="采集时间下界，YYYY-MM-DD 或 ISO 8601（含边界）"
    ),
    end: Optional[str] = typer.Option(
        None, "--to", help="采集时间上界，YYYY-MM-DD 或 ISO 8601（含边界）"
    ),
    limit: int = typer.Option(
        DEFAULT_SEARCH_LIMIT, "--limit", min=1, max=MAX_SEARCH_LIMIT, help="最多导出条数"
    ),
    offset: int = typer.Option(0, "--offset", min=0, help="跳过前 N 条（分页）"),
    with_content: bool = typer.Option(
        False,
        "--with-content",
        help="同时导出完整 raw 响应正文；默认只导出元信息，避免大文件占满磁盘",
    ),
    max_scan: int = typer.Option(
        DEFAULT_BODY_SCAN_LIMIT,
        "--max-scan",
        min=1,
        help="正文检索最多扫描的候选 raw 文件数",
    ),
    db_path: Path = typer.Option(DEFAULT_DB, "--db", help="SQLite 数据库路径"),
    raw_dir: Path = typer.Option(DEFAULT_RAW_DIR, "--raw-dir", help="原始响应目录"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="输出 DEBUG 日志"),
) -> None:
    """把检索结果导出为 JSON；默认仅元信息，--with-content 才包含完整响应。"""

    configure_logging(verbose)
    try:
        search_filter = _build_filter(
            source=source,
            external_id=external_id,
            status=status,
            error_keyword=error_keyword,
            start_text=start,
            end_text=end,
        )
    except typer.BadParameter as exc:
        typer.secho(f"参数错误: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    database = _open_database(db_path)
    raw_store = RawStore(raw_dir)
    read_errors: list[RawContent] = []
    try:
        if body_keyword is None:
            page = database.search_raw(search_filter, limit=limit, offset=offset)
            rows, total, scanned = page.rows, page.total, 0
        else:
            rows, total, read_errors, scanned, _ = _run_body_search(
                database,
                raw_store,
                search_filter,
                body_keyword=body_keyword,
                limit=limit,
                offset=offset,
                max_scan=max_scan,
            )

        exported: list[dict[str, object]] = []
        for record in rows:
            item = _serialize_raw(record)
            if with_content:
                content = raw_store.read_for_record(record)
                if content.ok:
                    item["content"] = content.payload
                else:
                    item["content"] = None
                    item["content_error"] = content.error
                    read_errors.append(content)
            exported.append(item)
    except StoreError as exc:
        typer.secho(f"导出失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    finally:
        database.close()

    payload = {
        "total": total,
        "exported": len(exported),
        "with_content": with_content,
        "body_scanned": scanned,
        "read_errors": [
            {"path": item.path, "error": item.error} for item in read_errors
        ],
        "results": exported,
    }
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        temp_path = output.with_name(f"{output.name}.tmp")
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        temp_path.replace(output)
    except OSError as exc:
        typer.secho(f"导出文件写入失败 path={output} error={exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc

    for content in read_errors:
        typer.secho(
            f"警告: 无法读取 raw 文件 path={content.path} reason={content.error}",
            fg=typer.colors.YELLOW,
            err=True,
        )
    mode = "元信息+完整响应" if with_content else "仅元信息"
    typer.echo(
        f"已导出 {len(exported)}/{total} 条（{mode}）到 {output}"
        + (f"，正文扫描 {scanned} 个 raw 文件" if body_keyword is not None else "")
    )


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
