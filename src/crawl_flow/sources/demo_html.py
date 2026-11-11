"""分页 HTML 列表 + HTML 详情适配器。

选择器不再写死在代码里：``SourceConfig.selectors`` 提供五项详情字段
（title / category / tags / date / content）的 CSS 选择器，缺省沿用
:data:`crawl_flow.config.DEFAULT_SELECTORS`。接入结构不同的 HTML 站点时
只需在源配置里覆盖选择器，无需复制适配器。

列表页与详情页共用统一解析入口 :class:`HtmlDocument`。
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from crawl_flow.errors import ParseError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.sources.base import ListItem, SourceAdapter, build_url

if TYPE_CHECKING:
    from crawl_flow.config import SourceConfig

#: 列表表格与详情链接是站点接入约定，不进入可配置选择器段。
LIST_TABLE_SELECTOR = "table.items"
LIST_LINK_SELECTOR = "a[href]"
ITEM_ID_PATTERN = re.compile(r"/item/([^/?#]+)")


@dataclass(frozen=True)
class HtmlSelectors:
    """详情页五项字段最终生效的 CSS 选择器（全局默认 + 单源覆盖）。"""

    title: str
    category: str
    tags: str
    date: str
    content: str

    @classmethod
    def from_config(cls, source: "SourceConfig") -> "HtmlSelectors":
        return cls(**source.selectors.resolved())


class HtmlDocument:
    """列表页与详情页共用的解析入口。

    ``parse`` 把 :class:`FetchResult` 统一转成 :class:`BeautifulSoup`，
    列表抽取与详情字段抽取都基于同一份 soup 与同一套配置化选择器。
    """

    def __init__(
        self,
        result: FetchResult,
        source: "SourceConfig",
        selectors: HtmlSelectors,
    ) -> None:
        self.result = result
        self.source = source
        self.selectors = selectors
        self.soup = BeautifulSoup(result.body, "lxml")

    # ---- 列表页 ----
    def parse_list(self) -> list[ListItem]:
        table = self.soup.select_one(LIST_TABLE_SELECTOR)
        if table is None:
            raise self._parse_error("list", f"未找到 {LIST_TABLE_SELECTOR}")
        rows = table.select("tbody tr")

        items: list[ListItem] = []
        for index, row in enumerate(rows, start=1):
            link = row.select_one(LIST_LINK_SELECTOR)
            href = link.get("href") if link is not None else None
            if not isinstance(href, str):
                raise self._parse_error(f"list-row-{index}", "列表行缺少详情链接")
            external_id = self._extract_external_id(href)
            if external_id is None:
                raise self._parse_error(
                    f"list-row-{index}", f"无法从链接提取 id: {href}"
                )
            items.append(
                ListItem(
                    external_id=external_id,
                    url=urljoin(f"{self.source.base_url}/", href),
                )
            )
        return items

    # ---- 详情页 ----
    def parse_detail(self, external_id: str) -> dict[str, Any]:
        title = self._text_or_none(self.selectors.title)
        if not title:
            raise self._parse_error(
                external_id,
                f"选择器 {self.selectors.title} 未命中标题（title）",
            )

        content_el = self.soup.select_one(self.selectors.content)
        if content_el is None:
            raise self._parse_error(
                external_id,
                f"选择器 {self.selectors.content} 未命中正文（content）",
            )

        return {
            "title": title,
            "category": self._text_or_none(self.selectors.category),
            "tags": self._extract_tags(self.selectors.tags),
            "published_at": self._text_or_none(self.selectors.date),
            "content": content_el.decode_contents(),
        }

    # ---- 通用工具 ----
    def _extract_external_id(self, href: str) -> str | None:
        """从详情链接提取 id。

        优先匹配 ``/item/{id}`` 约定；兼容其他路径结构时，退化为取路径
        最后一个非空段，使新站点只改选择器/路径配置即可接入。
        """

        match = ITEM_ID_PATTERN.search(href)
        if match is not None:
            external_id = match.group(1).strip()
            if external_id:
                return external_id

        path = href.split("?", 1)[0].split("#", 1)[0].rstrip("/")
        segment = path.rsplit("/", 1)[-1].strip()
        return segment or None

    def _text_or_none(self, selector: str) -> str | None:
        element = self.soup.select_one(selector)
        if element is None:
            return None
        text = element.get_text(" ", strip=True)
        return text or None

    def _extract_tags(self, selector: str) -> list[str] | str:
        container = self.soup.select_one(selector)
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

    def _parse_error(self, external_id: str, reason: str) -> ParseError:
        return ParseError(self.source.name, external_id, reason)


class DemoHtmlAdapter(SourceAdapter):
    """对接 ``GET /list?page=N`` 与 ``GET /item/{id}`` 的 HTML 适配器。

    详情选择器全部来自源配置；站点结构不同时新增一份配置即可复用本类。
    """

    def __init__(self, source: "SourceConfig", fetcher: Fetcher) -> None:
        super().__init__(source, fetcher)
        self._selectors = HtmlSelectors.from_config(source)

    def list_url(self, page: int) -> str:
        return build_url(self.source.base_url, self.source.list_path or "/list")

    def detail_url(self, external_id: str) -> str:
        return self._id_url(external_id)

    def parse_list(self, result: FetchResult) -> list[ListItem]:
        return HtmlDocument(result, self.source, self._selectors).parse_list()

    def parse_detail(self, result: FetchResult, external_id: str) -> dict[str, Any]:
        return HtmlDocument(result, self.source, self._selectors).parse_detail(
            external_id
        )
