"""GenericHtmlAdapter 运行时行为：声明式解析、危险内容摘除、与管道接口一致。"""

from __future__ import annotations

import pytest

from crawl_flow.autogen.spec import AdapterSpec
from crawl_flow.errors import ParseError
from crawl_flow.fetch.types import FetchResult
from crawl_flow.sources.generic_html import GenericHtmlAdapter, sanitize_soup
from bs4 import BeautifulSoup


def _spec() -> AdapterSpec:
    return AdapterSpec.from_dict(
        {
            "base_url": "http://mock.local",
            "list": {
                "list_path": "/posts",
                "item_selector": "section.post-list > article.post-card",
                "link_selector": "a.post-link",
                "id_pattern": r"/posts/([^/?#]+)(?:[?#]|$)",
                "pagination": "next_link",
                "next_selector": "nav.pager > a",
            },
            "detail": {
                "detail_path": "/posts/{id}",
                "scope_selector": "article.entry",
                "fields": {
                    "title": {"selector": "h1.entry-title", "mode": "text"},
                    "content": {"selector": "div.entry-content", "mode": "html"},
                    "tags": {"selector": "ul.entry-tags > li", "mode": "tags_text"},
                },
            },
        }
    )


class _Source:
    name = "generic_test"
    adapter_spec = _spec().model_dump(mode="json")
    base_url = "http://mock.local"


def _adapter() -> GenericHtmlAdapter:
    return GenericHtmlAdapter(_Source(), fetcher=None)  # type: ignore[arg-type]


def test_sanitize_strips_scripts_events_and_dangerous_protocols() -> None:
    soup = BeautifulSoup(
        """
        <div>
          <script>alert(1)</script>
          <img src="x" onerror="alert(2)">
          <a href="javascript:alert(3)" onclick="x()">link</a>
          <iframe src="http://evil"></iframe>
          <p>safe text</p>
        </div>
        """,
        "lxml",
    )
    sanitize_soup(soup)
    assert soup.find("script") is None
    assert soup.find("iframe") is None
    img = soup.find("img")
    assert img is not None
    assert img.get("src") == "x"  # 安全属性保留
    assert "onerror" not in img.attrs  # 内联事件摘除
    anchor = soup.find("a")
    assert anchor is not None and "href" not in anchor.attrs and "onclick" not in anchor.attrs
    assert soup.get_text(strip=True) == "linksafe text"


def test_parse_list_extracts_items_and_ids() -> None:
    html = """
    <html><body><section class="post-list">
      <article class="post-card"><a class="post-link" href="/posts/slug-a">A</a></article>
      <article class="post-card"><a class="post-link" href="/posts/slug-b">B</a></article>
    </section></body></html>
    """
    items = _adapter().parse_list(
        FetchResult(url="http://mock.local/posts", status_code=200, headers={}, body=html, elapsed_ms=0)
    )
    assert [i.external_id for i in items] == ["slug-a", "slug-b"]
    assert items[0].url == "http://mock.local/posts/slug-a"


def test_parse_detail_text_html_and_tags_modes() -> None:
    html = """
    <html><body><article class="entry">
      <h1 class="entry-title">标题</h1>
      <ul class="entry-tags"><li>标签一</li><li>标签二</li></ul>
      <div class="entry-content"><p>正文段落</p></div>
    </article></body></html>
    """
    fields = _adapter().parse_detail(
        FetchResult(url="http://mock.local/posts/x", status_code=200, headers={}, body=html, elapsed_ms=0),
        external_id="x",
    )
    assert fields["title"] == "标题"
    assert "<p>正文段落</p>" in fields["content"]
    assert fields["tags"] == ["标签一", "标签二"]


def test_missing_required_field_raises_parse_error() -> None:
    html = '<html><body><article class="entry"><div class="entry-content">x</div></article></body></html>'
    with pytest.raises(ParseError) as excinfo:
        _adapter().parse_detail(
            FetchResult(url="http://mock.local/posts/x", status_code=200, headers={}, body=html, elapsed_ms=0),
            external_id="x",
        )
    assert "title" in excinfo.value.reason


def test_malicious_detail_is_neutralized_at_runtime() -> None:
    # 即使恶意样本绕过生成期沙箱，运行期解析也会摘除脚本，不产生代码执行
    executed: list[str] = []
    html = """
    <html><body><article class="entry">
      <h1 class="entry-title" onclick="executed.append('event')">标题</h1>
      <div class="entry-content"><p>正文</p></div>
      <ul class="entry-tags"><li>t</li></ul>
      <script>executed.append('script')</script>
    </article></body></html>
    """
    fields = _adapter().parse_detail(
        FetchResult(url="http://mock.local/posts/x", status_code=200, headers={}, body=html, elapsed_ms=0),
        external_id="x",
    )
    assert fields["title"] == "标题"
    assert executed == []


def test_query_param_pagination_uses_super_iterator() -> None:
    spec = _spec()
    spec.list.pagination = "query_param"
    spec.list.page_param = "page"
    spec.list.next_selector = None

    class _Source2:
        name = "generic_test"
        adapter_spec = spec.model_dump(mode="json")
        base_url = "http://mock.local"

    adapter = GenericHtmlAdapter(_Source2(), fetcher=None)  # type: ignore[arg-type]
    assert adapter.list_params(3) == {"page": 3}
