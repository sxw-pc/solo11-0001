"""autogen 子命令：适配器生成、适配校验、周期调度、样本治理与博客 mock。

所有失败都以非零退出码 + 明确中文原因呈现，杜绝静默失败：

- 退出码 3：站点不在能力边界内（登录 / 验证码 / 未开启的动态渲染）；
- 退出码 4：沙箱拦截到恶意样本；
- 退出码 5：结构识别 / 回放校验等生成失败；
- 退出码 6：校验发现结构漂移（broken/drifted），报告内含调整建议。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional

import typer
import yaml

from crawl_flow.autogen.constants import (
    DEFAULT_MIN_INTERVAL_SECONDS,
    DEFAULT_SAMPLE_MAX_BYTES,
    DEFAULT_SAMPLE_VERSIONS,
    DEFAULT_VALIDATION_INTERVAL_SECONDS,
)
from crawl_flow.autogen.errors import (
    GenerationError,
    SandboxViolation,
    UnsupportedSiteError,
)
from crawl_flow.autogen.generator import AdapterGenerator
from crawl_flow.autogen.limiter import reset_global_limiter
from crawl_flow.autogen.polite import PoliteFetcher
from crawl_flow.autogen.sample_store import SampleStore, site_key_for
from crawl_flow.autogen.scheduler import ValidationScheduler
from crawl_flow.autogen.spec import AdapterSpec
from crawl_flow.autogen.validator import AdapterValidator, ValidationReport

autogen_app = typer.Typer(
    name="autogen",
    no_args_is_help=True,
    help="站点适配器自动生成 / 适配校验 / 样本治理",
)

DEFAULT_SAMPLE_ROOT = Path("data/autogen_samples")


def _parse_fields(items: list[str]) -> list[dict[str, str]]:
    """把 ``title`` 或 ``category:分类,栏目`` 形式的声明解析成字段列表。"""

    fields: list[dict[str, str]] = []
    for raw in items:
        if ":" in raw:
            name, hints = raw.split(":", 1)
            fields.append({"name": name.strip(), "keywords": hints.strip()})
        else:
            fields.append({"name": raw.strip()})
    return fields


def _load_spec(path: Path) -> AdapterSpec:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "adapter_spec" in payload:
        payload = payload["adapter_spec"]
    if not isinstance(payload, dict):
        raise typer.BadParameter(f"规格文件格式不正确: {path}")
    return AdapterSpec.from_dict(payload)


def _print_report(report: ValidationReport) -> None:
    typer.echo(f"校验结果: {report.status}")
    typer.echo(
        f"样本: 列表页 {report.sampled_list_pages} 个, 详情页 {report.sampled_details} 个"
    )
    if report.findings:
        typer.echo("结构变更明细:")
        for finding in report.findings:
            typer.echo(f"  - [{finding.component}] {finding.detail}")
            typer.echo(f"      原选择器: {finding.expected_selector}")
    if report.suggestions:
        typer.echo("可落地的调整建议:")
        for suggestion in report.suggestions:
            typer.echo(
                f"  - [{suggestion.component}] 置信度={suggestion.confidence} "
                f"{suggestion.rationale}"
            )
            typer.echo(f"      {suggestion.old_selector}  →  {suggestion.new_selector}")
    if not report.findings and not report.suggestions:
        typer.echo("适配器与当前站点结构一致，无需调整。")


@autogen_app.command("generate")
def generate_command(
    entry_url: str = typer.Argument(..., help="目标站点入口地址（列表页或包含列表入口的首页）"),
    fields: list[str] = typer.Option(
        ...,
        "--field",
        "-f",
        help="必填字段，可重复；格式 name 或 name:关键词1,关键词2；"
        "可选标准字段 title/content/published_at/category/tags",
    ),
    output: Path = typer.Option(
        Path("config/generated"), "--output", "-o", help="适配器规格输出目录"
    ),
    name: Optional[str] = typer.Option(None, "--name", "-n", help="生成源名称（默认取站点 host）"),
    sample_root: Path = typer.Option(DEFAULT_SAMPLE_ROOT, "--sample-root", help="样本留档目录"),
    min_interval: float = typer.Option(
        DEFAULT_MIN_INTERVAL_SECONDS,
        "--min-interval",
        min=0.0,
        help="自动爬取最小请求间隔秒数，默认 2.0（每 2 秒至多 1 次）",
    ),
    enable_dynamic_rendering: bool = typer.Option(
        False,
        "--enable-dynamic-rendering",
        help="显式开启动态渲染站点支持（默认关闭，需额外安装浏览器依赖）",
    ),
    yaml_source: bool = typer.Option(
        True,
        "--yaml-source/--spec-only",
        help="同时输出可直接加入 sources.yaml 的源片段（默认开启）",
    ),
) -> None:
    """自动爬取样本并生成可直接运行的采集适配器配置。"""

    reset_global_limiter(min_interval_seconds=min_interval)
    source_name = name or site_key_for(entry_url).replace(".", "_")
    generator = AdapterGenerator(
        sample_store=SampleStore(
            sample_root,
            keep_versions=DEFAULT_SAMPLE_VERSIONS,
            max_bytes=DEFAULT_SAMPLE_MAX_BYTES,
        ),
        enable_dynamic_rendering=enable_dynamic_rendering,
    )
    try:
        spec = generator.generate(entry_url, _parse_fields(fields))
    except UnsupportedSiteError as exc:
        typer.secho(f"不支持的站点: {exc.reason}", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(code=3) from exc
    except SandboxViolation as exc:
        typer.secho(f"沙箱校验拦截: {exc.reason}", fg=typer.colors.RED, err=True)
        for threat in exc.threats:
            typer.secho(f"  威胁: {threat}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=4) from exc
    except GenerationError as exc:
        typer.secho(f"适配器生成失败: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=5) from exc

    output.mkdir(parents=True, exist_ok=True)
    spec_path = output / f"{source_name}.spec.json"
    spec_path.write_text(
        json.dumps(spec.model_dump(mode="json"), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    typer.secho(f"适配器规格已生成: {spec_path}", fg=typer.colors.GREEN)

    if yaml_source:
        source_path = output / f"{source_name}.source.yaml"
        source_path.write_text(
            yaml.safe_dump(
                {"sources": [spec.to_source_config_dict(source_name)]},
                allow_unicode=True,
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        typer.secho(f"可直接接入管道的源配置: {source_path}", fg=typer.colors.GREEN)
    typer.echo(f"样本已留档至: {sample_root / site_key_for(entry_url)}")


@autogen_app.command("validate")
def validate_command(
    spec_file: Path = typer.Argument(..., help="适配器规格文件（.spec.json 或源片段 yaml）"),
    entry_url: Optional[str] = typer.Option(None, "--entry-url", help="覆盖规格中的站点地址"),
    min_interval: float = typer.Option(
        DEFAULT_MIN_INTERVAL_SECONDS, "--min-interval", min=0.0, help="最小请求间隔秒数"
    ),
) -> None:
    """用最新站点样本校验适配器，漂移时输出具体调整建议。"""

    reset_global_limiter(min_interval_seconds=min_interval)
    spec = _load_spec(spec_file)
    report = AdapterValidator(fetcher=PoliteFetcher()).validate(spec, entry_url=entry_url)
    _print_report(report)
    if report.changed:
        raise typer.Exit(code=6)


@autogen_app.command("validate-schedule")
def validate_schedule_command(
    specs: list[Path] = typer.Argument(..., help="一个或多个适配器规格文件"),
    interval: int = typer.Option(
        DEFAULT_VALIDATION_INTERVAL_SECONDS,
        "--interval",
        min=60,
        help="校验周期秒数，默认 3600（1 小时）",
    ),
    min_interval: float = typer.Option(
        DEFAULT_MIN_INTERVAL_SECONDS, "--min-interval", min=0.0, help="最小请求间隔秒数"
    ),
    run_once: bool = typer.Option(False, "--once", help="只跑一轮后退出（供外部调度器接管）"),
) -> None:
    """启动周期适配校验（低优先级，与采集共享限速，不抢占业务资源）。"""

    reset_global_limiter(min_interval_seconds=min_interval)

    def _on_result(job_name: str, report: ValidationReport) -> None:
        typer.echo(f"[{job_name}] status={report.status} findings={len(report.findings)}")
        if report.changed:
            _print_report(report)

    scheduler = ValidationScheduler(interval_seconds=interval, on_result=_on_result)
    for spec_file in specs:
        spec = _load_spec(spec_file)
        scheduler.register(spec_file.stem, spec)

    if run_once:
        reports = scheduler.run_once()
        if any(report.changed for report in reports):
            raise typer.Exit(code=6)
        return

    typer.echo(f"周期校验已启动，间隔 {interval} 秒；Ctrl+C 退出。")
    scheduler.start()
    try:
        scheduler._stop.wait()
    except KeyboardInterrupt:  # pragma: no cover - 交互信号
        typer.echo("收到退出信号，停止调度器。")
        scheduler.stop()


@autogen_app.command("samples")
def samples_command(
    entry_url: str = typer.Argument(..., help="生成时使用的入口地址"),
    sample_root: Path = typer.Option(DEFAULT_SAMPLE_ROOT, "--sample-root"),
) -> None:
    """查看某站点的样本版本与占用空间。"""

    store = SampleStore(sample_root)
    versions = store.list_versions(entry_url)
    if not versions:
        typer.secho(f"未找到 {entry_url} 的样本", fg=typer.colors.YELLOW)
        raise typer.Exit(code=1)
    typer.echo(f"站点 {entry_url} 共 {len(versions)} 个保留版本（上限 {DEFAULT_SAMPLE_VERSIONS}）:")
    for version in versions:
        size = sum(f.stat().st_size for f in version.path.rglob("*") if f.is_file())
        typer.echo(f"  - {version.version_id}  {size} 字节  {version.path}")
    typer.echo(f"样本库总占用: {store.total_bytes()} / {DEFAULT_SAMPLE_MAX_BYTES} 字节")


@autogen_app.command("serve-blog")
def serve_blog_command(
    port: int = typer.Option(8020, "--port", min=1, max=65535),
    host: str = typer.Option("127.0.0.1", "--host"),
    variant: str = typer.Option("v1", "--variant", help="站点结构变体 v1 / v2（模拟改版）"),
) -> None:
    """启动公开静态博客 mock 站点（自动生成 / 校验演练用）。"""

    # autogen/cli.py 比主 cli.py 深一层，仓库根在 parents[3]
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    try:
        from mock_site.blog.app import create_app
    except ImportError as exc:  # pragma: no cover
        typer.secho(f"无法导入博客 mock 站点: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1) from exc

    typer.echo(f"blog mock site ({variant}): http://{host}:{port}")
    create_app(default_variant=variant).run(host=host, port=port, debug=False, use_reloader=False)
