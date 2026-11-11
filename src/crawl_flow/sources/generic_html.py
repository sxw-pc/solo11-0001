"""generic_html：解释 :class:`AdapterSpec` 的声明式 HTML 适配器。

与手写适配器（demo_html 等）实现同一个 :class:`SourceAdapter` 接口，
``parse_list`` 返回 :class:`ListItem`、``parse_detail`` 返回标准源字段字典，
因此接入现有管道时 ``run.py`` / ``normalize.py`` 不需要任何修改，
运行输出格式与手写适配器完全一致。

安全设计（纵深防御，即使样本绕过了生成期沙箱也不会触发本地代码执行）：

- 仅用 BeautifulSoup + lxml 的 HTML 解析器做**静态解析**，不存在任何脚本执行引擎；
- 解析后统一摘除 ``<script>`` / ``<style>`` 元素、内联事件属性与 ``javascript:`` 链接；
- 选择器与属性名在 :class:`AdapterSpec` 加载时已经过白名单字符校验。
"""

from __future__ import annotations

import re
from typing import Any, Iterator
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from crawl_flow.autogen.spec import AdapterSpec
from crawl_flow.errors import ParseError
from crawl_flow.fetch.types import FetchResult
from crawl_flow.sources.base import (
    ListItem,
    ListPage,
    MAX_PAGES,
    SourceAdapter,
    build_url,
)

# 内联事件属性（onclick / onerror / onload ...）统一摘除。
_INLINE_EVENT = re.compile(r"^on[a-z]+$", re.IGNORECASE)
# 危险协议链接。
_DANGEROUS_PROTOCOL = re.compile(r"^\s*(javascript|vbscript|data)\s*:", re.IGNORECASE)
_DANGEROUS_NODES = ("script", "style", "iframe", "object", "embed")


def sanitize_soup(soup: BeautifulSoup) -> BeautifulSoup:
    """摘除所有可触发代码执行的元素与属性（运行期纵深防御）。"""

    for element in soup.find_all(_DANGEROUS_NODES):
        element.decompose()
    for element in soup.find_all(True):
        if not isinstance(element, Tag):
            continue
        for attr_name in list(element.attrs):
            if _INLINE_EVENT.match(attr_name):
                del element.attrs[attr_name]
                continue
            value = element.attrs.get(attr_name)
            if attr_name.lower() in {"href", "src"} and isinstance(value, str):
                if _DANGEROUS_PROTOCOL.match(value):
                    del element.attrs[attr_name]
    return soup


class GenericHtmlAdapter(SourceAdapter):
    """按 ``adapter_spec`` 声明的选择器完成列表翻页与详情提取。"""

    def __init__(self, source: Any, fetcher: Any) -> None:
        super().__init__(source, fetcher)
        if not source.adapter_spec:
            raise ParseError(source.name, "spec", "auto_html 源缺少 adapter_spec")
        self.spec = AdapterSpec.from_dict(source.adapter_spec)
        self._id_pattern = re.compile(self.spec.list.id_pattern)

    # ---------- URL ----------

    def list_url(self, page: int) -> str:
        return build_url(self.source.base_url, self.spec.list.list_path)

    def list_params(self, page: int) -> dict[str, object]:
        if self.spec.list.pagination == "query_param":
            return {self.spec.list.page_param or "page": page}
        return {}

    def detail_url(self, external_id: str) -> str:
        return build_url(
            self.source.base_url,
            self.spec.detail.detail_path.format(id=external_id),
        )

    # ---------- 列表 ----------

    def parse_list(self, result: FetchResult) -> list[ListItem]:
        soup = self._soup(result)
        containers = soup.select(self.spec.list.item_selector)
        # 选择器未命中视为空页：让通用翻页逻辑自然终止，
        # 而不是把站点改版导致的整页结构变化升级为整轮失败。
        if not containers:
            return []

        items: list[ListItem] = []
        seen: set[str] = set()
        for index, container in enumerate(containers, start=1):
            link = container.select_one(self.spec.list.link_selector)
            href = link.get(self.spec.list.link_attr) if link is not None else None
            if not isinstance(href, str) or not href:
                raise self._parse_error(f"list-row-{index}", "列表条目缺少详情链接")
            absolute = urljoin(result.url, href)
            match = self._id_pattern.search(absolute)
            if match is None:
                raise self._parse_error(f"list-row-{index}", f"无法从链接提取 id: {absolute}")
            external_id = match.group(1).strip()
            if not external_id or external_id in seen:
                continue
            seen.add(external_id)
            items.append(ListItem(external_id=external_id, url=absolute))
        return items

    def iter_list_pages(self) -> Iterator[ListPage]:
        """按规格的分页模式翻页；``next_link`` 模式在此自行跟踪链接。"""

        if self.spec.list.pagination == "query_param":
            yield from super().iter_list_pages()
            return

        url: str | None = self.list_url(1)
        page = 1
        while url is not None and page <= MAX_PAGES:
            result = self.fetcher.get(url)
            if result.status_code >= 400:
                from crawl_flow.errors import FetchError

                raise FetchError(
                    f"列表页请求失败 source={self.source.name} page={page} "
                    f"status={result.status_code}",
                    url=result.url,
                    status_code=result.status_code,
                )
            items = self.parse_list(result)
            yield ListPage(page=page, items=items)
            if not items:
                return
            next_el = self._soup(result).select_one(self.spec.list.next_selector or "")
            next_href = next_el.get("href") if next_el is not None else None
            url = urljoin(result.url, next_href) if isinstance(next_href, str) and next_href else None
            page += 1

    # ---------- 详情 ----------

    def parse_detail(self, result: FetchResult, external_id: str) -> dict[str, Any]:
        soup = self._soup(result)
        scope = soup.select_one(self.spec.detail.scope_selector)
        if scope is None:
            raise self._parse_error(
                external_id,
                f"详情容器选择器未命中: {self.spec.detail.scope_selector}",
            )

        fields: dict[str, Any] = {}
        for name, rule in self.spec.detail.fields.items():
            value = self._extract(scope, rule.selector, rule.mode, rule.attr)
            if value in (None, "", []) and not rule.optional:
                raise self._parse_error(external_id, f"必填字段 {name} 提取为空: {rule.selector}")
            fields[name] = value if value is not None else self._empty_for(rule.mode)
        return fields

    @staticmethod
    def _empty_for(mode: str) -> Any:
        return [] if mode == "tags_text" else None

    def _extract(self, scope: Tag, selector: str, mode: str, attr: str | None) -> Any:
        if mode == "tags_text":
            return [
                text
                for element in scope.select(selector)
                if (text := element.get_text(" ", strip=True))
            ]
        element = scope.select_one(selector)
        if element is None:
            return None
        if mode == "text":
            return element.get_text(" ", strip=True) or None
        if mode == "html":
            return element.decode_contents()
        if mode == "attr":
            value = element.get(attr or "")
            return value.strip() if isinstance(value, str) else None
        raise self._parse_error("detail", f"未知提取模式: {mode}")

    # ---------- 基础设施 ----------

    def _soup(self, result: FetchResult) -> BeautifulSoup:
        return sanitize_soup(BeautifulSoup(result.body, "lxml"))
