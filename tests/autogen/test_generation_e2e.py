"""端到端：自动生成 → 现有管道采集 → 输出格式与手写适配器一致 → 改版漂移校验。"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
import yaml

from crawl_flow.autogen.errors import (
    GenerationError,
    SandboxViolation,
    UnsupportedSiteError,
)
from crawl_flow.autogen.generator import AdapterGenerator
from crawl_flow.autogen.polite import PoliteFetcher
from crawl_flow.autogen.sample_store import SampleStore
from crawl_flow.autogen.validator import AdapterValidator
from crawl_flow.config import SourceConfig, load_sources
from crawl_flow.fetch.client import Fetcher
from crawl_flow.pipeline.normalize import normalize_record
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore
from mock_site.blog.posts_data import POSTS

REQUIRED_FIELDS = ["title", "content", "published_at", "category", "tags"]


def _make_generator(sample_root: Path) -> AdapterGenerator:
    return AdapterGenerator(
        sample_store=SampleStore(sample_root),
        fetcher=PoliteFetcher(fetcher=Fetcher(timeout=5.0)),
    )


def test_generate_adapter_within_five_minutes(blog_server: str, sample_root: Path) -> None:
    started = time.perf_counter()
    spec = _make_generator(sample_root).generate(
        blog_server + "/posts", REQUIRED_FIELDS
    )
    elapsed = time.perf_counter() - started

    assert elapsed < 300  # 需求：5 分钟内生成可用适配器
    assert spec.engine == "generic_html"
    assert spec.list.pagination == "next_link"
    assert "{id}" in spec.detail.detail_path
    assert set(spec.detail.fields) == set(REQUIRED_FIELDS)


def test_generated_adapter_collects_all_public_content(
    blog_server: str, sample_root: Path, tmp_path: Path
) -> None:
    spec = _make_generator(sample_root).generate(blog_server + "/posts", REQUIRED_FIELDS)
    source = SourceConfig.model_validate(spec.to_source_config_dict("blog_auto"))

    db = Database(tmp_path / "crawl.db")
    db.init()
    try:
        summary = run_source(
            source,
            db,
            RawStore(tmp_path / "raw"),
            fetcher=Fetcher(timeout=5.0),
        )
        assert summary.failed == 0
        stats = db.stats()
        row = next(r for r in stats["sources"] if r["source"] == "blog_auto")
        assert row["records"] == len(POSTS)  # 全部公开内容
    finally:
        db.close()


def test_output_format_identical_to_handwritten_adapter(
    blog_server: str, sample_root: Path
) -> None:
    """生成适配器与「手写等价适配器」的归一化结果字段结构完全一致。"""

    spec = _make_generator(sample_root).generate(blog_server + "/posts", REQUIRED_FIELDS)
    source = SourceConfig.model_validate(spec.to_source_config_dict("blog_auto"))

    from crawl_flow.sources import create_adapter

    fetcher = Fetcher(timeout=5.0)
    generated = create_adapter(source, fetcher)
    list_result = fetcher.get(generated.list_url(1))
    items = generated.parse_list(list_result)
    assert items, "生成适配器必须能解析列表条目"

    detail = fetcher.get(items[0].url)
    fields = generated.parse_detail(detail, items[0].external_id)
    record = normalize_record(
        source,
        items[0].external_id,
        fields,
        detail_url=detail.url,
        fetched_at=__import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ),
    )

    # 与手写适配器共享同一 Record 模型与归一化逻辑：字段集合、类型完全一致
    assert set(record.model_fields) == {
        "source",
        "external_id",
        "title",
        "category",
        "tags",
        "published_at",
        "content",
        "url",
        "fetched_at",
    }
    assert isinstance(record.tags, list) and record.tags
    assert record.title and record.content and record.published_at is not None
    assert "<p>" not in record.content  # 与 demo_html 一样走 html_to_text 去标签
    fetcher.close()


def test_generated_yaml_loads_in_pipeline_config(
    blog_server: str, sample_root: Path, tmp_path: Path
) -> None:
    spec = _make_generator(sample_root).generate(blog_server + "/posts", REQUIRED_FIELDS)
    config_path = tmp_path / "sources.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"sources": [spec.to_source_config_dict("blog_auto")]},
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    sources = load_sources(config_path)
    assert len(sources) == 1
    assert sources[0].type == "auto_html"
    assert sources[0].adapter_spec["engine"] == "generic_html"


def test_drift_detection_and_suggestions_after_site_change(
    blog_server_factory, sample_root: Path
) -> None:
    base, app = blog_server_factory("v1")
    spec = _make_generator(sample_root).generate(base + "/posts", REQUIRED_FIELDS)

    # 模拟站点列表页与详情页结构小幅调整
    app.config["BLOG_VARIANT"] = "v2"
    report = AdapterValidator(fetcher=PoliteFetcher(fetcher=Fetcher(timeout=5.0))).validate(spec)

    assert report.status == "drifted"
    components = {f.component for f in report.findings}
    assert "list_item" in components
    assert any(c.startswith("detail_scope") for c in components)

    suggestions = {s.component: s for s in report.suggestions}
    assert "list_item" in suggestions
    assert "blog-cards" in suggestions["list_item"].new_selector
    title_suggestion = suggestions.get("field:title")
    assert title_suggestion is not None
    assert "headline" in title_suggestion.new_selector
    # 建议必须可直接落地：新选择器在 v2 页面上确实命中
    import bs4

    html = PoliteFetcher(fetcher=Fetcher(timeout=5.0))._fetcher.get(  # noqa: SLF001
        base + "/posts/spring-park-flowers"
    )
    soup = bs4.BeautifulSoup(html.body, "lxml")
    assert soup.select_one(title_suggestion.new_selector) is not None


def test_healthy_site_passes_validation(blog_server: str, sample_root: Path) -> None:
    spec = _make_generator(sample_root).generate(blog_server + "/posts", REQUIRED_FIELDS)
    report = AdapterValidator(fetcher=PoliteFetcher(fetcher=Fetcher(timeout=5.0))).validate(spec)
    assert report.status == "healthy"
    assert report.findings == []


def test_login_required_site_rejected(blog_server: str, sample_root: Path) -> None:
    with pytest.raises(UnsupportedSiteError) as excinfo:
        _make_generator(sample_root).generate(
            blog_server + "/login-required/posts", REQUIRED_FIELDS
        )
    assert excinfo.value.reason_code == "login_required"


def test_captcha_site_rejected(blog_server: str, sample_root: Path) -> None:
    with pytest.raises(UnsupportedSiteError) as excinfo:
        _make_generator(sample_root).generate(
            blog_server + "/captcha/posts", REQUIRED_FIELDS
        )
    assert excinfo.value.reason_code == "captcha_required"


def test_js_only_rejected_by_default_and_opt_in_explicit(
    blog_server: str, sample_root: Path
) -> None:
    # 默认关闭：明确报不支持，而不是生成无效适配器
    with pytest.raises(UnsupportedSiteError) as excinfo:
        _make_generator(sample_root).generate(
            blog_server + "/js-only/posts", REQUIRED_FIELDS
        )
    assert excinfo.value.reason_code == "dynamic_rendering_disabled"

    # 显式开启：进入渲染分支，环境无浏览器依赖时给出 renderer_unavailable 明确原因
    with pytest.raises(UnsupportedSiteError) as excinfo:
        AdapterGenerator(
            sample_store=SampleStore(sample_root / "opt-in"),
            fetcher=PoliteFetcher(fetcher=Fetcher(timeout=5.0)),
            enable_dynamic_rendering=True,
        ).generate(blog_server + "/js-only/posts", REQUIRED_FIELDS)
    assert excinfo.value.reason_code == "renderer_unavailable"


def test_malicious_site_blocked_by_sandbox(blog_server: str, sample_root: Path) -> None:
    with pytest.raises(SandboxViolation) as excinfo:
        _make_generator(sample_root).generate(blog_server + "/evil/posts", REQUIRED_FIELDS)
    assert excinfo.value.threats  # 带具体威胁清单，不会静默失败


def test_missing_required_field_gives_explicit_reason(
    blog_server: str, sample_root: Path
) -> None:
    with pytest.raises(GenerationError) as excinfo:
        _make_generator(sample_root).generate(
            blog_server + "/posts", ["title", "content", "category", "nonexistent_field"]
        )
    assert "nonexistent_field" in str(excinfo.value)


def test_title_is_mandatory(blog_server: str, sample_root: Path) -> None:
    with pytest.raises(GenerationError, match="title"):
        _make_generator(sample_root).generate(blog_server + "/posts", ["content"])


def test_generation_persists_sample_versions(blog_server: str, sample_root: Path) -> None:
    generator = _make_generator(sample_root)
    generator.generate(blog_server + "/posts", REQUIRED_FIELDS)
    store = SampleStore(sample_root)
    versions = store.list_versions(blog_server + "/posts")
    assert len(versions) == 1
    detail_files = list((versions[0].path / "detail").glob("*.html"))
    assert len(detail_files) >= 2
    assert (versions[0].path / "spec.json").is_file()
