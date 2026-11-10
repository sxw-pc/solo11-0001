"""render 渲染能力：配置开关、可插拔渲染器、JS 注入正文、失败隔离与缺依赖。

全程离线：不安装、不启动 Playwright，渲染用本文件内的 :class:`FakeRenderer`
替身完成；缺依赖路径通过 factory 未注册后端与临时配置直接构造来覆盖。
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.config import SourceConfig, load_sources
from crawl_flow.errors import RendererDependencyError, RendererError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.pipeline.run import run_source
from crawl_flow.render import Renderer, create_renderer, register_renderer
from crawl_flow.render.playwright_renderer import PlaywrightRenderer, playwright_available
from crawl_flow.sources import create_adapter
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

# 提取内联脚本里 injected 数组的字符串字面量，模拟浏览器执行 JS 注入的结果。
_INJECTED_ARRAY_PATTERN = re.compile(r"var injected\s*=\s*\[(.*?)\]\.join", re.DOTALL)
_STRING_PATTERN = re.compile(r'"([^"]*)"')


class FakeRenderer(Renderer):
    """测试替身：用静态 HTTP 取回页面，再「执行」内联注入脚本。

    - 记录每次渲染的 URL，便于断言列表页不走渲染器；
    - ``fail_urls`` 中的 URL 抛 :class:`RendererError`，模拟单条渲染失败；
    - ``close_called`` 用于验证管道负责渲染器生命周期。
    """

    name = "fake"

    def __init__(self, fetcher: Fetcher, *, fail_urls: set[str] | None = None) -> None:
        self._fetcher = fetcher
        self.fail_urls = fail_urls or set()
        self.rendered_urls: list[str] = []
        self.close_called = False

    def render(self, url: str) -> FetchResult:
        self.rendered_urls.append(url)
        if url in self.fail_urls:
            raise RendererError(f"渲染失败 url={url} error=模拟渲染器故障")
        result = self._fetcher.get(url)
        return FetchResult(
            url=result.url,
            status_code=result.status_code,
            headers=result.headers,
            body=self._execute_inline_script(result.body),
            elapsed_ms=result.elapsed_ms,
        )

    def close(self) -> None:
        self.close_called = True

    @staticmethod
    def _execute_inline_script(body: str) -> str:
        """把内联脚本中的 innerHTML 注入应用到静态空壳页面上。"""

        match = _INJECTED_ARRAY_PATTERN.search(body)
        if match is None:
            return body
        fragments = _STRING_PATTERN.findall(match.group(1))
        injected = "".join(fragments)
        return body.replace('<div class="content" id="js-content"></div>',
                            f'<div class="content" id="js-content">{injected}</div>')


@pytest.fixture
def render_source(mock_base_url: str) -> SourceConfig:
    return SourceConfig(
        name="demo_html_render",
        type="html",
        base_url=mock_base_url,
        render=True,
        list_path="/js-list",
        detail_path="/item/{id}",
    )


@pytest.fixture
def fake_renderer(fetcher: Fetcher) -> FakeRenderer:
    return FakeRenderer(fetcher)


# ---------- 配置模型 ----------

def test_render_defaults_to_false(make_source: Any) -> None:
    assert make_source("plain").render is False


def test_render_flag_validated(mock_base_url: str) -> None:
    source = SourceConfig.model_validate(
        {
            "name": "render_src",
            "type": "html",
            "base_url": mock_base_url,
            "render": True,
            "list_path": "/js-list",
            "detail_path": "/item/{id}",
        }
    )
    assert source.render is True


def test_render_flag_rejects_non_boolean(mock_base_url: str) -> None:
    with pytest.raises(Exception):
        SourceConfig.model_validate(
            {
                "name": "render_src",
                "type": "html",
                "base_url": mock_base_url,
                "render": "yes",
            }
        )


def test_render_flag_loads_from_yaml(write_config: Any, source_payload: Any) -> None:
    path = write_config(
        [
            source_payload(
                "demo_html_render",
                "html",
                render=True,
                list_path="/js-list",
                detail_path="/item/{id}",
            )
        ]
    )
    sources = load_sources(path)
    assert sources[0].render is True


# ---------- 渲染器可插拔与统一接口 ----------

def test_renderer_registry_is_pluggable(fetcher: Fetcher) -> None:
    sentinel = FakeRenderer(fetcher)
    register_renderer("fake-test", lambda **_: sentinel)
    try:
        assert create_renderer("fake-test") is sentinel
    finally:
        from crawl_flow.render.factory import _RENDERER_FACTORIES

        _RENDERER_FACTORIES.pop("fake-test", None)


def test_unknown_renderer_backend_raises_dependency_error() -> None:
    with pytest.raises(RendererDependencyError) as excinfo:
        create_renderer("not-a-real-backend")
    assert "未知的渲染后端" in str(excinfo.value)


def test_playwright_module_imports_without_playwright_installed() -> None:
    # 模块本身可导入，导入动作不触发 playwright 加载
    import crawl_flow.render.playwright_renderer as module

    assert module is not None
    if not playwright_available():
        with pytest.raises(RendererDependencyError) as excinfo:
            PlaywrightRenderer()
        message = str(excinfo.value)
        assert "playwright" in message
        assert "pip install" in message
        assert "playwright install chromium" in message
        assert excinfo.value.__cause__ is None  # 不是裸 ImportError 冒泡


# ---------- 适配器只依赖统一接口 ----------

def test_adapter_uses_renderer_only_for_detail(
    render_source: SourceConfig,
    fetcher: Fetcher,
    fake_renderer: FakeRenderer,
) -> None:
    adapter = create_adapter(render_source, fetcher, fake_renderer)

    pages = list(adapter.iter_list_pages())
    assert [item.external_id for item in pages[0].items] == ["js-1", "js-2"]
    assert fake_renderer.rendered_urls == []  # 列表页始终走静态抓取

    result = adapter.fetch_detail("js-1")
    assert len(fake_renderer.rendered_urls) == 1
    assert fake_renderer.rendered_urls[0].endswith("/item/js-1")
    fields = adapter.parse_detail(result, "js-1")
    assert "静态 HTTP 抓取只能拿到空壳" in fields["content"]


def test_non_render_source_never_calls_renderer(
    html_source: SourceConfig,
    fetcher: Fetcher,
    fake_renderer: FakeRenderer,
) -> None:
    adapter = create_adapter(html_source, fetcher, fake_renderer)
    adapter.fetch_detail("html-1")
    assert fake_renderer.rendered_urls == []


# ---------- 端到端：JS 注入正文写入 records ----------

def test_render_source_captures_js_injected_content(
    render_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    fake_renderer: FakeRenderer,
) -> None:
    # 对照：静态抓取同一详情页，div.content 是空壳（注入文字只存在于 <script> 中）
    from bs4 import BeautifulSoup

    static = fetcher_get(render_source, "js-1")
    static_soup = BeautifulSoup(static.body, "lxml")
    assert static_soup.select_one("div.content").get_text(strip=True) == ""

    summary = run_source(
        render_source,
        db,
        raw_store,
        renderer=fake_renderer,
    )

    assert (summary.ok, summary.failed, summary.skipped) == (2, 0, 0)
    assert db.count_records("demo_html_render") == 2
    assert db.count_raw("demo_html_render", "ok") == 2
    assert fake_renderer.close_called is False  # 注入的渲染器由调用方管理

    record = db_load_record(db, "js-1")
    assert record.title == "JS 渲染详情（一）"
    assert record.category == "技术"
    assert "静态 HTTP 抓取只能拿到空壳" in record.content
    assert "渲染器执行页面脚本后" in record.content
    assert record.url.endswith("/item/js-1")
    # 渲染器只用于详情，共渲染 2 次
    assert len(fake_renderer.rendered_urls) == 2


def test_pipeline_owns_and_closes_created_renderer(
    render_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[FakeRenderer] = []

    def _factory(**kwargs: object) -> Renderer:
        instance = FakeRenderer(Fetcher(timeout=5.0))
        created.append(instance)
        return instance

    monkeypatch.setattr(
        "crawl_flow.render.factory._RENDERER_FACTORIES",
        {"playwright": _factory},
    )

    summary = run_source(render_source, db, raw_store, limit=1)
    assert summary.ok == 1
    assert len(created) == 1
    assert created[0].close_called is True


# ---------- 单条渲染失败走详情失败路径 ----------

def test_single_render_failure_is_isolated(
    render_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
    fetcher: Fetcher,
) -> None:
    failing = FakeRenderer(
        fetcher,
        fail_urls={f"{render_source.base_url}/item/js-1"},
    )
    summary = run_source(render_source, db, raw_store, renderer=failing)

    assert (summary.ok, summary.failed) == (1, 1)
    assert db.count_records("demo_html_render") == 1  # 只有 js-2 入库
    assert db.count_raw("demo_html_render", "failed") == 1

    failed = db_load_raw(db, "js-1")
    assert failed.status == "failed"
    assert failed.http_status is None
    assert failed.error is not None and "模拟渲染器故障" in failed.error
    assert failed.raw_path is not None
    payload = Path(failed.raw_path).read_text(encoding="utf-8")
    assert "模拟渲染器故障" in payload

    succeeded = db_load_record(db, "js-2")
    assert "第二条 JS 渲染正文" in succeeded.content


def test_dependency_error_is_not_swallowed_as_item_failure(
    render_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
) -> None:
    class MissingDepRenderer(Renderer):
        name = "missing-dep"

        def render(self, url: str) -> FetchResult:
            raise RendererDependencyError("未安装可选依赖 'playwright'")

    with pytest.raises(RendererDependencyError) as excinfo:
        run_source(
            render_source,
            db,
            raw_store,
            renderer=MissingDepRenderer(),
        )
    assert "playwright" in str(excinfo.value)
    # 缺依赖不产生任何 failed 行：它不是单条可恢复错误
    assert db.count_raw("demo_html_render") == 0
    assert db.count_records("demo_html_render") == 0


# ---------- CLI：缺依赖退出码 2，无 ImportError 堆栈 ----------

def test_cli_missing_render_dependency_exits_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [
            source_payload(
                "demo_html_render",
                "html",
                render=True,
                list_path="/js-list",
                detail_path="/item/{id}",
            )
        ]
    )
    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_html_render",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "render.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )

    assert result.exit_code == 2, result.output
    assert "渲染依赖缺失" in result.output
    assert "playwright" in result.output
    assert "Traceback (most recent call last)" not in result.output
    assert "ImportError" not in result.output


# ---------- 辅助函数 ----------

def fetcher_get(source: SourceConfig, external_id: str) -> FetchResult:
    with Fetcher(timeout=5.0) as client:
        adapter = create_adapter(source, client)
        return client.get(adapter.detail_url(external_id))


def db_load_record(db: Database, external_id: str) -> Any:
    from crawl_flow.models import RecordRow
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    with Session(db.engine) as session:
        return session.execute(
            select(RecordRow).where(RecordRow.external_id == external_id)
        ).scalar_one()


def db_load_raw(db: Database, external_id: str) -> Any:
    from crawl_flow.models import RawRecordRow
    from sqlalchemy import select
    from sqlalchemy.orm import Session

    with Session(db.engine) as session:
        return session.execute(
            select(RawRecordRow).where(RawRecordRow.external_id == external_id)
        ).scalar_one()
