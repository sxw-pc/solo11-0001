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
        text = result.body
        soup = BeautifulSoup(text, "lxml")
        if text.strip() and soup.find() is None:
            # lxml 对个别畸形文档会产出空树；换内置解析器再试一次。
            # 非致命语法错误不应直接判失败——能提取必填字段就照常处理。
            soup = BeautifulSoup(text, "html.parser")
        return soup

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
