"""demo_html 适配器：列表链接、中文详情、标签与日期。"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.pipeline.normalize import normalize_record
from crawl_flow.sources.demo_html import DemoHtmlAdapter


@pytest.fixture
def adapter(html_source: SourceConfig, fetcher: Fetcher) -> DemoHtmlAdapter:
    return DemoHtmlAdapter(html_source, fetcher)


def test_parse_list_extracts_ids_and_urls(adapter: DemoHtmlAdapter) -> None:
    result = adapter.fetcher.get(adapter.list_url(1))
    items = adapter.parse_list(result)

    assert len(items) == 5
    assert [item.external_id for item in items] == [
        "html-1",
        "html-2",
        "html-3",
        "html-4",
        "html-5",
    ]
    assert items[0].url.endswith("/item/html-1")


def test_parse_detail_maps_chinese_fields(adapter: DemoHtmlAdapter, html_source: SourceConfig) -> None:
    result = adapter.fetch_detail("html-2")
    fields = adapter.parse_detail(result, "html-2")
    record = normalize_record(
        html_source,
        "html-2",
        fields,
        detail_url=result.url,
        fetched_at=datetime(2024, 3, 2, tzinfo=timezone.utc),
    )

    assert record.title == "开源数据库性能基准测试汇总"
    assert record.category == "技术"
    assert record.tags == ["数据库", "性能", "基线"]
    assert record.published_at == datetime(2024, 3, 2)
    assert "三款开源数据库" in record.content
    assert "<p>" not in record.content
    assert "\n" in record.content


def test_detail_missing_title_raises_parse_error(adapter: DemoHtmlAdapter) -> None:
    result = FetchResult(
        url="http://mock/item/html-1",
        status_code=200,
        body="<html><body><div class='content'>只有正文</div></body></html>",
    )
    with pytest.raises(ParseError) as excinfo:
        adapter.parse_detail(result, "html-1")
    assert excinfo.value.external_id == "html-1"
    assert "h1.title" in excinfo.value.reason


def test_list_without_table_raises_parse_error(adapter: DemoHtmlAdapter) -> None:
    result = FetchResult(
        url="http://mock/list?page=1",
        status_code=200,
        body="<html><body><p>维护中</p></body></html>",
    )
    with pytest.raises(ParseError) as excinfo:
        adapter.parse_list(result)
    assert "table.items" in excinfo.value.reason
