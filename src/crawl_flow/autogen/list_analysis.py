"""列表页结构分析：条目容器、详情链接、分页规则与 external_id 模式。"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urljoin, urlparse

from bs4 import BeautifulSoup, Tag

from crawl_flow.autogen.errors import GenerationError
from crawl_flow.autogen.selectors import (
    build_selector,
    relative_selector,
    repeated_children,
)


@dataclass(frozen=True)
class ListStructure:
    item_selector: str
    link_selector: str
    link_attr: str
    id_pattern: str
    pagination: str  # "next_link" | "query_param"
    next_selector: str | None
    page_param: str | None
    sample_urls: list[str]


# 下一页链接的识别信号（文本 + rel 属性 + href 形态）
_NEXT_TEXT = ("下一页", "下页", "next", "›", "»", "older", "more")
_NEXT_REL = {"next"}
# 常见页码查询参数
_PAGE_PARAMS = ("page", "p", "page_num", "page_no", "pn")
# 从路径末段提取 id 的兜底正则
_FALLBACK_ID_PATTERN = r"/([^/?#]+)(?:[?#]|$)"


def analyze_list(pages: list[tuple[str, BeautifulSoup]]) -> ListStructure:
    """基于多页列表样本识别列表结构。``pages`` 为 (页面URL, soup) 列表。"""

    if not pages:
        raise GenerationError("没有可用的列表页样本", stage="list_analysis")

    first_url, first_soup = pages[0]
    item_selector, items = _select_item_container(first_soup)
    link_selector, link_attr = _select_link(items[0])

    hrefs = [_collect_hrefs(soup, item_selector, link_selector, link_attr, base_url=url)
             for url, soup in pages]
    flat_hrefs = [href for group in hrefs for href in group]
    if not flat_hrefs:
        raise GenerationError("列表条目内未找到任何详情链接", stage="list_analysis")

    id_pattern = _infer_id_pattern(flat_hrefs)
    pagination, next_selector, page_param = _detect_pagination(pages)

    return ListStructure(
        item_selector=item_selector,
        link_selector=link_selector,
        link_attr=link_attr,
        id_pattern=id_pattern,
        pagination=pagination,
        next_selector=next_selector,
        page_param=page_param,
        sample_urls=flat_hrefs,
    )


# ---------- 条目容器 ----------


def _select_item_container(soup: BeautifulSoup) -> tuple[str, list[Tag]]:
    candidates = repeated_children(soup)
    if not candidates:
        raise GenerationError(
            "未识别到重复的列表条目结构（静态 HTML 中缺少成组重复元素）",
            stage="list_analysis",
        )

    def score(entry: tuple[str, list[Tag]]) -> tuple[int, int]:
        key, members = entry
        with_link = sum(1 for m in members if m.find("a", href=True))
        text_len = sum(len(m.get_text(strip=True)) for m in members)
        # 优先：含链接多、文本量适中（避免选到整页 body）、条目数量多
        return (with_link, text_len)

    best_key, best_members = max(candidates.items(), key=score)
    if not any(m.find("a", href=True) for m in best_members):
        raise GenerationError(
            "识别到的重复条目内没有详情链接，无法生成列表提取规则",
            stage="list_analysis",
        )
    return build_selector(best_members[0]), best_members


# ---------- 链接 ----------


def _select_link(item: Tag) -> tuple[str, str]:
    link = item.find("a", href=True)
    if link is None:
        raise GenerationError("列表条目缺少 a[href] 详情链接", stage="list_analysis")
    return relative_selector(item, link), "href"


def _collect_hrefs(
    soup: BeautifulSoup,
    item_selector: str,
    link_selector: str,
    link_attr: str,
    *,
    base_url: str,
) -> list[str]:
    hrefs: list[str] = []
    for item in soup.select(item_selector):
        link = item.select_one(link_selector)
        href = link.get(link_attr) if link is not None else None
        if isinstance(href, str) and href:
            hrefs.append(urljoin(base_url, href))
    return hrefs


# ---------- id 模式 ----------


def _infer_id_pattern(hrefs: list[str]) -> str:
    paths = [urlparse(h).path for h in hrefs]
    # 1) 末段 slug/id：/posts/some-slug 或 /item/123
    last_segments = {p.rstrip("/").rsplit("/", 1)[-1] for p in paths if p.strip("/")}
    if len(last_segments) == len([p for p in paths if p.strip("/")]) and len(last_segments) >= 1:
        distinct = last_segments
        if len(distinct) >= 2 or _looks_like_id(next(iter(distinct))):
            return _FALLBACK_ID_PATTERN
    # 2) 查询参数 ?id=xxx
    for href in hrefs:
        query = parse_qs(urlparse(href).query)
        for key, values in query.items():
            if key.lower() in {"id", "item_id", "article_id"} and values:
                return rf"[?&]{re.escape(key)}=([^&#]+)"
    return _FALLBACK_ID_PATTERN


def _looks_like_id(segment: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", segment))


# ---------- 分页 ----------


def _detect_pagination(
    pages: list[tuple[str, BeautifulSoup]],
) -> tuple[str, str | None, str | None]:
    first_url, first_soup = pages[0]

    for anchor in first_soup.find_all("a", href=True):
        rel = anchor.get("rel")
        rel_values = rel if isinstance(rel, list) else [str(rel)]
        text = anchor.get_text(" ", strip=True).lower()
        if any(v.lower() in _NEXT_REL for v in rel_values) or any(
            hint in text for hint in _NEXT_TEXT
        ):
            selector = build_selector(anchor)
            return "next_link", selector, None

    # 退化为查询参数分页：对比多页 URL，或在当前页链接里找 page 参数
    if len(pages) >= 2:
        first_query = parse_qs(urlparse(first_url).query)
        second_query = parse_qs(urlparse(pages[1][0]).query)
        for param in _PAGE_PARAMS:
            if param in second_query and first_query.get(param) != second_query.get(param):
                return "query_param", None, param
    for anchor in first_soup.find_all("a", href=True):
        query = parse_qs(urlparse(str(anchor["href"])).query)
        for param in _PAGE_PARAMS:
            if param in query:
                return "query_param", None, param

    raise GenerationError(
        "未识别到分页规则（既没有下一页链接，也没有页码查询参数）",
        stage="list_analysis",
    )
