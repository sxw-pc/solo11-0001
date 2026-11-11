"""demo_html：分页 HTML 列表 + HTML 详情适配器。"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from crawl_flow.fetch.types import FetchResult
from crawl_flow.sources.base import ListItem, SourceAdapter, build_url

DETAIL_ID_PATTERN = re.compile(r"/item/([^/?#]+)")


class DemoHtmlAdapter(SourceAdapter):
    """对接 ``GET /list?page=N`` 与 ``GET /item/{id}``。"""

    def list_url(self, page: int) -> str:
        return build_url(self.source.base_url, self.source.list_path or "/list")

    def detail_url(self, external_id: str) -> str:
        return self._id_url(external_id)

    def parse_list(self, result: FetchResult) -> list[ListItem]:
        soup = self._soup(result)
        table = soup.select_one("table.items")
        if table is None:
            raise self._parse_error("list", "未找到 table.items")
        rows = table.select("tbody tr")

        items: list[ListItem] = []
        for index, row in enumerate(rows, start=1):
            link = row.select_one("a[href]")
            href = link.get("href") if link is not None else None
            if not isinstance(href, str):
                raise self._parse_error(f"list-row-{index}", "列表行缺少详情链接")
            match = DETAIL_ID_PATTERN.search(href)
            if match is None:
                raise self._parse_error(f"list-row-{index}", f"无法从链接提取 id: {href}")
            external_id = match.group(1).strip()
            if not external_id:
                raise self._parse_error(f"list-row-{index}", "详情链接 id 为空")
            items.append(
                ListItem(
                    external_id=external_id,
                    url=urljoin(f"{self.source.base_url}/", href),
                )
            )
        return items

    def list_response_page(self, result: FetchResult) -> int | None:
        """读取 ``table.items`` 上的 ``data-page`` 自报页码；缺失则不校验。"""

        soup = self._soup(result)
        table = soup.select_one("table.items")
        if table is None:
            return None
        raw_page = table.get("data-page")
        if raw_page is None:
            return None
        try:
            return int(str(raw_page).strip())
        except ValueError:
            raise self._parse_error("list", f"列表 data-page 不是整数: {raw_page}")

    def list_has_next(self, result: FetchResult) -> bool | None:
        """HTML 列表通过「下一页」链接声明翻页：没有链接即末页。

        表格缺失等结构问题留给 :meth:`parse_list` 抛 :class:`ParseError`，
        此处只在能解析出表格时给出下一页信号。
        """

        soup = self._soup(result)
        if soup.select_one("table.items") is None:
            return None
        next_link = soup.select_one("a[href*='page=']")
        return next_link is not None

    def parse_detail(self, result: FetchResult, external_id: str) -> dict[str, Any]:
        soup = self._soup(result)
        title_el = soup.select_one("h1.title")
        title = title_el.get_text(strip=True) if title_el is not None else ""
        if not title:
            raise self._parse_error(external_id, "详情缺少 h1.title")

        content_el = soup.select_one("div.content")
        if content_el is None:
            raise self._parse_error(external_id, "详情缺少 div.content")

        return {
            "title": title,
            "category": self._text_or_none(soup, "span.category"),
            "tags": self._extract_tags(soup),
            "published_at": self._text_or_none(soup, "span.date"),
            "content": content_el.decode_contents(),
        }

    def _soup(self, result: FetchResult) -> BeautifulSoup:
        return BeautifulSoup(result.body, "lxml")

    @staticmethod
    def _text_or_none(soup: BeautifulSoup, selector: str) -> str | None:
        element = soup.select_one(selector)
        if element is None:
            return None
        text = element.get_text(" ", strip=True)
        return text or None

    @staticmethod
    def _extract_tags(soup: BeautifulSoup) -> list[str] | str:
        container = soup.select_one(".tags")
        if container is None:
            return []
        entries = container.select("li")
        if entries:
            return [
                text
                for entry in entries
                if (text := entry.get_text(" ", strip=True))
            ]
        return container.get_text(" ", strip=True)
