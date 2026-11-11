"""selectors 配置化：新结构站点只改配置即可接入，含失败选择器与配置校验。

覆盖：
- 结构不同的 html_alt fixture 仅靠 selectors 覆盖即可解析五类字段并入库；
- title / content 选择器未命中走单条 failed，摘要带未命中的选择器名；
- 选择器缺失（null）、空字符串、非法类型、未知键分别报可读 ConfigError，
  报错带源名与字段路径，且不影响同文件中的其他源；
- dry-run 与正式执行结果一致，解析结果可被现有归一化链路原样消费。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from crawl_flow.config import DEFAULT_SELECTORS, SourceConfig, load_sources
from crawl_flow.errors import ConfigError, ParseError
from crawl_flow.fetch.types import FetchResult
from crawl_flow.models import RecordRow
from crawl_flow.pipeline.normalize import normalize_record
from crawl_flow.pipeline.run import run_source
from crawl_flow.sources.demo_html import DemoHtmlAdapter, HtmlDocument, HtmlSelectors

ALT_SELECTORS = {
    "title": "h2.headline",
    "category": "em.channel",
    "tags": "ul.keyword-list",
    "date": "time.publish-time",
    "content": "article.article-body",
}


def _make_default_html_source() -> SourceConfig:
    return SourceConfig(
        name="legacy_html",
        type="html",
        base_url="http://127.0.0.1:1",
    )


@pytest.fixture
def alt_source(make_source: Any) -> Any:
    return make_source(
        "demo_html_alt",
        "html",
        list_path="/portal/list",
        detail_path="/portal/article/{id}",
        selectors=dict(ALT_SELECTORS),
    )


@pytest.fixture
def alt_adapter(alt_source: Any, fetcher: Any) -> DemoHtmlAdapter:
    return DemoHtmlAdapter(alt_source, fetcher)


# ---------- 统一解析入口：只改配置即可接入结构不同的站点 ----------


def test_default_selectors_match_legacy_contract() -> None:
    assert DEFAULT_SELECTORS == {
        "title": "h1.title",
        "category": "span.category",
        "tags": ".tags",
        "date": "span.date",
        "content": "div.content",
    }
    assert HtmlSelectors.from_config(
        _make_default_html_source()
    ) == HtmlSelectors(**DEFAULT_SELECTORS)


def test_alt_list_parses_non_item_url_ids(alt_adapter: DemoHtmlAdapter) -> None:
    result = alt_adapter.fetcher.get(alt_adapter.list_url(1))
    items = alt_adapter.parse_list(result)

    assert len(items) == 5
    assert [item.external_id for item in items] == [
        "alt-1",
        "alt-2",
        "alt-3",
        "alt-4",
        "alt-5",
    ]
    assert items[0].url.endswith("/portal/article/alt-1")


def test_alt_detail_extracts_all_five_fields(
    alt_adapter: DemoHtmlAdapter, alt_source: Any
) -> None:
    result = alt_adapter.fetch_detail("alt-1")
    fields = alt_adapter.parse_detail(result, "alt-1")

    assert set(fields) == {"title", "category", "tags", "published_at", "content"}
    assert fields["title"] == "城市观察第 1 期：科研领域月度记录"
    assert fields["category"] == "科研"
    assert fields["tags"] == ["观察", "数据", "专题"]
    assert fields["published_at"] == "2024-03-01 08:00"
    assert "城市观察正文" in fields["content"]

    # 归一化链路原样消费解析结果。
    normalized = normalize_record(
        alt_source,
        "alt-1",
        fields,
        detail_url=result.url,
        fetched_at=datetime(2024, 3, 1),
    )
    assert normalized.title.startswith("城市观察第 1 期")
    assert normalized.category == "科研"
    assert normalized.tags == ["观察", "数据", "专题"]
    assert normalized.published_at == datetime(2024, 3, 1, 8, 0)
    assert "<p>" not in normalized.content


def test_partial_override_falls_back_to_defaults(
    fetcher: Any, make_source: Any
) -> None:
    source = make_source(
        "demo_html_partial",
        "html",
        selectors={"title": "h2.headline"},
    )
    adapter = DemoHtmlAdapter(source, fetcher)

    assert adapter._selectors.title == "h2.headline"  # noqa: SLF001
    assert adapter._selectors.content == DEFAULT_SELECTORS["content"]  # noqa: SLF001
    assert adapter._selectors.tags == DEFAULT_SELECTORS["tags"]  # noqa: SLF001


def test_alt_source_persists_five_fields_end_to_end(
    alt_source: Any, db: Any, raw_store: Any
) -> None:
    summary = run_source(alt_source, db, raw_store, limit=2)

    assert (summary.ok, summary.failed, summary.skipped) == (2, 0, 13)
    assert db.count_records("demo_html_alt") == 2
    assert db.count_raw("demo_html_alt") == 2

    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "alt-2")
        ).scalar_one()

    assert row.source == "demo_html_alt"
    assert row.title == "城市观察第 2 期：民生领域月度记录"
    assert row.category == "民生"
    assert row.tags == ["数据", "专题", "记录"]
    assert row.published_at == datetime(2024, 3, 2, 8, 0)
    assert "城市观察正文" in row.content
    assert row.url.endswith("/portal/article/alt-2")


# ---------- 失败选择器：单条 failed，摘要带未命中的选择器名 ----------


def test_missing_title_selector_hit_records_failed(
    alt_adapter: DemoHtmlAdapter, alt_source: Any
) -> None:
    broken = alt_source.model_copy(
        update={"selectors": alt_source.selectors.model_copy(update={"title": "h1.title"})}
    )
    adapter = DemoHtmlAdapter(broken, alt_adapter.fetcher)
    result = adapter.fetch_detail("alt-1")

    with pytest.raises(ParseError) as excinfo:
        adapter.parse_detail(result, "alt-1")
    assert excinfo.value.source == "demo_html_alt"
    assert excinfo.value.external_id == "alt-1"
    assert "h1.title" in excinfo.value.reason
    assert "title" in excinfo.value.reason


def test_missing_content_selector_hit_records_failed(
    alt_adapter: DemoHtmlAdapter, alt_source: Any
) -> None:
    broken = alt_source.model_copy(
        update={
            "selectors": alt_source.selectors.model_copy(
                update={"content": "div.content"}
            )
        }
    )
    adapter = DemoHtmlAdapter(broken, alt_adapter.fetcher)
    result = adapter.fetch_detail("alt-1")

    with pytest.raises(ParseError) as excinfo:
        adapter.parse_detail(result, "alt-1")
    assert "div.content" in excinfo.value.reason
    assert "content" in excinfo.value.reason


def test_failed_selector_summary_names_selector(
    alt_source: Any, db: Any, raw_store: Any
) -> None:
    broken = alt_source.model_copy(
        update={
            "selectors": alt_source.selectors.model_copy(
                update={"content": "div.not-exist"}
            )
        }
    )
    summary = run_source(broken, db, raw_store, limit=2)

    assert summary.ok == 0
    assert summary.failed == 2
    for outcome in summary.results:
        assert outcome.status == "failed"
        assert "div.not-exist" in (outcome.error or "")
    assert db.count_records("demo_html_alt") == 0
    assert db.count_raw("demo_html_alt", "failed") == 2


def test_optional_selectors_missing_yield_empty_fields(
    alt_adapter: DemoHtmlAdapter, alt_source: Any
) -> None:
    # category / tags / date 未命中不是致命错误，返回空值交给归一化处理。
    broken = alt_source.model_copy(
        update={
            "selectors": alt_source.selectors.model_copy(
                update={
                    "category": "em.missing",
                    "tags": "ul.missing",
                    "date": "time.missing",
                }
            )
        }
    )
    adapter = DemoHtmlAdapter(broken, alt_adapter.fetcher)
    result = adapter.fetch_detail("alt-1")
    fields = adapter.parse_detail(result, "alt-1")

    assert fields["category"] is None
    assert fields["tags"] == []
    assert fields["published_at"] is None
    assert fields["title"]
    assert fields["content"]


# ---------- 启动期配置校验：缺失 / 空串 / 非法类型 / 未知键 ----------


def test_empty_selectors_section_uses_defaults(
    write_config: Any, source_payload: Any
) -> None:
    path = write_config([source_payload("demo_html", "html", selectors={})])
    sources = load_sources(path)

    assert sources[0].selectors.resolved() == DEFAULT_SELECTORS


@pytest.mark.parametrize(
    ("selectors", "fragment"),
    [
        (None, "selectors 缺失"),
        ({"title": None}, "selectors.title 缺失"),
        ({"title": "   "}, "selectors.title 不能为空字符串"),
        ({"title": ""}, "selectors.title 不能为空字符串"),
        ({"title": 123}, "selectors.title 必须是 CSS 选择器字符串"),
        ({"title": ["h2.headline"]}, "实际类型为 list"),
        ("h2.headline", "selectors 必须是选择器映射"),
        ({"heading": "h2"}, "selectors.heading"),
    ],
)
def test_invalid_selector_config_raises_config_error(
    write_config: Any,
    source_payload: Any,
    selectors: Any,
    fragment: str,
) -> None:
    path = write_config(
        [source_payload("bad_selectors", "html", selectors=selectors)]
    )
    with pytest.raises(ConfigError) as excinfo:
        load_sources(path)

    message = str(excinfo.value)
    assert "bad_selectors" in message
    assert fragment in message


def test_config_error_carries_source_name_and_field_path(
    write_config: Any, source_payload: Any
) -> None:
    path = write_config(
        [
            source_payload("good_one", "html", selectors=dict(ALT_SELECTORS)),
            source_payload("bad_one", "html", selectors={"date": 7}),
        ]
    )
    with pytest.raises(ConfigError) as excinfo:
        load_sources(path)

    message = str(excinfo.value)
    assert "sources[2](name=bad_one)" in message
    assert "selectors.date" in message
    assert "good_one" not in message


def test_bad_source_does_not_block_other_sources(
    write_config: Any, source_payload: Any
) -> None:
    # 去掉坏源后，同文件中的好源可以正常加载与运行。
    path = write_config([source_payload("good_one", "html", selectors=dict(ALT_SELECTORS))])
    sources = load_sources(path)

    assert [source.name for source in sources] == ["good_one"]
    assert sources[0].selectors.resolved()["title"] == "h2.headline"


# ---------- dry-run 与正式执行一致 ----------


def test_dry_run_matches_real_run_outcomes(
    alt_source: Any, db: Any, raw_store: Any
) -> None:
    broken = alt_source.model_copy(
        update={
            "selectors": alt_source.selectors.model_copy(
                update={"title": "h1.missing"}
            )
        }
    )

    dry = run_source(broken, db, raw_store, limit=2, dry_run=True)
    real = run_source(broken, db, raw_store, limit=2, dry_run=False)

    assert (dry.ok, dry.failed) == (0, 2)
    assert (real.ok, real.failed) == (0, 2)
    for dry_outcome, real_outcome in zip(dry.results, real.results, strict=True):
        assert dry_outcome.status == real_outcome.status == "failed"
        assert dry_outcome.error == real_outcome.error
        assert "h1.missing" in (dry_outcome.error or "")


def test_dry_run_healthy_source_writes_nothing(
    alt_source: Any, db: Any, raw_store: Any
) -> None:
    summary = run_source(alt_source, db, raw_store, limit=2, dry_run=True)

    assert (summary.ok, summary.failed) == (2, 0)
    assert db.count_records() == 0
    assert db.count_raw() == 0


def test_list_and_detail_share_html_document_entry(
    alt_adapter: DemoHtmlAdapter, alt_source: Any
) -> None:
    list_result = alt_adapter.fetcher.get(alt_adapter.list_url(1))
    detail_result = alt_adapter.fetch_detail("alt-1")
    selectors = HtmlSelectors.from_config(alt_source)

    list_doc = HtmlDocument(list_result, alt_source, selectors)
    detail_doc = HtmlDocument(detail_result, alt_source, selectors)

    assert list_doc.parse_list()[0].external_id == "alt-1"
    assert detail_doc.parse_detail("alt-1")["title"] == (
        "城市观察第 1 期：科研领域月度记录"
    )
    # 统一入口拿到的 soup 类型一致，证明两套解析走同一条构造路径。
    assert type(list_doc.soup) is type(detail_doc.soup)


def test_fetch_result_with_unknown_body_uses_parse_error_path(
    alt_source: Any,
) -> None:
    adapter = DemoHtmlAdapter(
        alt_source,
        fetcher=_NullFetcher(),
    )
    result = FetchResult(
        url="http://mock/portal/article/alt-1",
        status_code=200,
        body="<html><body><p>什么字段都没有</p></body></html>",
    )
    with pytest.raises(ParseError) as excinfo:
        adapter.parse_detail(result, "alt-1")
    assert "h2.headline" in excinfo.value.reason


class _NullFetcher:
    def get(self, url: str, params: Any = None) -> FetchResult:  # pragma: no cover
        raise AssertionError("该用例不应发起网络请求")
