"""demo_api 适配器：分页、字段与错误路径。"""

from __future__ import annotations

import pytest

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.sources.demo_api import DemoApiAdapter


@pytest.fixture
def adapter(api_source: SourceConfig, fetcher: Fetcher) -> DemoApiAdapter:
    return DemoApiAdapter(api_source, fetcher)


def test_iter_list_pages_collects_all_items(adapter: DemoApiAdapter) -> None:
    pages = list(adapter.iter_list_pages())
    # 总数 15、每页 5 恰好整除：取满 3 页即终止，不再多请求第 4 页空页。
    assert [page.page for page in pages] == [1, 2, 3]
    assert [len(page.items) for page in pages] == [5, 5, 5]

    ids = [item.external_id for page in pages for item in page.items]
    assert len(ids) == 15
    assert len(set(ids)) == 15
    assert ids[0] == "api-1"
    assert pages[0].items[0].url.endswith("/api/items/api-1")


def test_parse_detail_returns_source_fields(adapter: DemoApiAdapter) -> None:
    result = adapter.fetch_detail("api-3")
    fields = adapter.parse_detail(result, "api-3")

    assert fields["title"] == "社区图书馆借阅数据分析"
    assert fields["category"] == "文化"
    assert fields["tags"] == ["图书馆", "借阅", "统计"]
    assert fields["published_at"].startswith("2024-03-03T09:30:00")
    assert "<p>" in fields["content"]


def test_empty_list_page_stops_pagination(api_source: SourceConfig, fetcher: Fetcher) -> None:
    source = api_source.model_copy(update={"list_path": "/api/items?empty=1"})
    pages = list(DemoApiAdapter(source, fetcher).iter_list_pages())
    assert len(pages) == 1
    assert pages[0].items == []


def test_detail_id_mismatch_raises_parse_error(adapter: DemoApiAdapter) -> None:
    result = FetchResult(
        url="http://mock/api/items/api-1",
        status_code=200,
        body='{"id": "api-9", "title": "错位数据"}',
    )
    with pytest.raises(ParseError) as excinfo:
        adapter.parse_detail(result, "api-1")
    assert excinfo.value.source == "demo_api"
    assert "不一致" in excinfo.value.reason


def test_invalid_json_raises_parse_error(adapter: DemoApiAdapter) -> None:
    result = FetchResult(
        url="http://mock/api/items/api-1",
        status_code=200,
        body="<html>not json</html>",
    )
    with pytest.raises(ParseError) as excinfo:
        adapter.parse_detail(result, "api-1")
    assert "JSON 解析失败" in excinfo.value.reason
