"""适配器自动生成编排：入口 URL + 必填字段 → 可直接运行的 AdapterSpec。

流程（每一步失败都抛带明确原因的领域异常，不静默失败）：

1. 能力边界检测（登录 / 验证码 / 动态渲染开关）；
2. 限速爬取最多 3 个列表页，识别条目结构与分页规则；
3. 限速爬取最多 5 个详情样本，沙箱扫描恶意内容，定位必填字段；
4. 组装声明式 :class:`AdapterSpec`，沙箱静态审计；
5. 用样本回放校验规格可提取全部必填字段；
6. 样本版本化留档（最近 3 版 / 总量 100MB 自动清理），返回规格。
"""

from __future__ import annotations

import logging
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawl_flow.autogen.constants import (
    DEFAULT_SAMPLE_MAX_BYTES,
    DEFAULT_SAMPLE_VERSIONS,
    DYNAMIC_RENDER_ENABLED_BY_DEFAULT,
    GENERATION_MAX_DETAIL_SAMPLES,
    GENERATION_MAX_LIST_PAGES,
)
from crawl_flow.autogen.detail_analysis import RequiredField, analyze_detail
from crawl_flow.autogen.detector import detect_javascript_only, ensure_site_supported
from crawl_flow.autogen.errors import GenerationError
from crawl_flow.autogen.limiter import PRIORITY_CRAWL
from crawl_flow.autogen.list_analysis import analyze_list
from crawl_flow.autogen.polite import PoliteFetcher
from crawl_flow.autogen.sample_store import SampleStore
from crawl_flow.autogen.sandbox import assert_sample_safe, audit_spec
from crawl_flow.autogen.spec import AdapterSpec

LOGGER_NAME = "crawl_flow.autogen.generator"
LOGGER = logging.getLogger(LOGGER_NAME)


class AdapterGenerator:
    """适配器自动生成器。"""

    def __init__(
        self,
        *,
        fetcher: PoliteFetcher | None = None,
        sample_store: SampleStore | None = None,
        sample_root: str | Path = "data/autogen_samples",
        enable_dynamic_rendering: bool = DYNAMIC_RENDER_ENABLED_BY_DEFAULT,
    ) -> None:
        self._fetcher = fetcher or PoliteFetcher()
        self._owns_fetcher = fetcher is None
        self._store = sample_store or SampleStore(
            sample_root,
            keep_versions=DEFAULT_SAMPLE_VERSIONS,
            max_bytes=DEFAULT_SAMPLE_MAX_BYTES,
        )
        self._enable_dynamic = enable_dynamic_rendering

    def generate(
        self,
        entry_url: str,
        required_fields: list[RequiredField | dict[str, object] | str],
    ) -> AdapterSpec:
        """执行端到端生成，返回通过沙箱与回放校验的适配器规格。"""

        fields = [_coerce_field(item) for item in required_fields]
        _validate_field_names(fields)
        LOGGER.info("adapter generation start url=%s fields=%s", entry_url, [f.name for f in fields])

        try:
            # 入口页先取原始 HTML（暂不沙箱扫描），优先做能力边界检测：
            # 登录 / 验证码直接拒绝；JS-only 站点在显式开启渲染时走浏览器。
            entry_html = self._fetch_html(entry_url, stage="entry", scan=False)
            entry_soup = BeautifulSoup(entry_html, "lxml")
            ensure_site_supported(entry_soup, enable_dynamic_rendering=self._enable_dynamic)

            if detect_javascript_only(entry_soup) and self._enable_dynamic:
                # 显式开启动态渲染：用浏览器取渲染后 DOM，渲染结果同样过沙箱。
                from crawl_flow.autogen.renderer import BrowserRenderer

                with BrowserRenderer() as renderer:
                    rendered = renderer.render_html(entry_url)
                assert_sample_safe(rendered, source=entry_url)
                entry_html, entry_soup = rendered, BeautifulSoup(rendered, "lxml")
            else:
                # 通过能力检测后再补沙箱：入口页若仍带内联事件 / 危险协议则拦截。
                assert_sample_safe(entry_html, source=entry_url)

            list_url = self._discover_list_url(entry_url, entry_soup)
            list_pages, list_raw = self._crawl_list_samples(list_url)
            structure = analyze_list(list_pages)

            detail_samples, detail_raw = self._crawl_detail_samples(structure.sample_urls)
            scope_selector, field_rules = analyze_detail(detail_samples, fields)

            detail_path = _build_detail_path(structure.sample_urls[0], structure.id_pattern)
            parsed_list = urlparse(list_url)
            base_url = f"{parsed_list.scheme}://{parsed_list.netloc}"
            spec = AdapterSpec.from_dict(
                {
                    "engine": "generic_html",
                    "base_url": base_url,
                    "list": {
                        "list_path": urlparse(list_url).path or "/",
                        "item_selector": structure.item_selector,
                        "link_selector": structure.link_selector,
                        "link_attr": structure.link_attr,
                        "id_pattern": structure.id_pattern,
                        "pagination": structure.pagination,
                        "page_param": structure.page_param,
                        "next_selector": structure.next_selector,
                    },
                    "detail": {
                        "detail_path": detail_path,
                        "scope_selector": scope_selector,
                        "fields": {name: rule.model_dump() for name, rule in field_rules.items()},
                    },
                    "capabilities": {
                        "dynamic_rendering": "enabled" if self._enable_dynamic else "disabled",
                        "list_path_discovered": parsed_list.path or "/",
                    },
                }
            )
            audit_spec(spec)
            self._replay_validation(spec, list_pages, detail_samples)

            self._store.create_version(
                entry_url,
                list_samples=list_raw,
                detail_samples=detail_raw,
                spec=spec.model_dump(mode="json"),
                metadata={"required_fields": [f.name for f in fields]},
            )
            LOGGER.info("adapter generation ok url=%s", entry_url)
            return spec
        finally:
            if self._owns_fetcher:
                self._fetcher.close()

    # ---------- 爬取 ----------

    def _fetch_html(self, url: str, *, stage: str, scan: bool = True) -> str:
        result = self._fetcher.get(url, priority=PRIORITY_CRAWL)
        if result.status_code >= 400:
            raise GenerationError(
                f"页面请求失败 HTTP {result.status_code}",
                stage=stage,
                detail=url,
            )
        content_type = result.headers.get("content-type", "")
        if "html" not in content_type and "text" not in content_type:
            raise GenerationError(
                f"响应不是 HTML 页面（content-type={content_type or 'unknown'}）",
                stage=stage,
                detail=url,
            )
        # 入口页需要先做能力边界检测（JS-only 站点天然含 <script>），
        # 由调用方决定扫描时机；其余样本一律先过沙箱再解析。
        if scan:
            assert_sample_safe(result.body, source=url)
        return result.body

    def _discover_list_url(self, entry_url: str, soup: BeautifulSoup) -> str:
        """入口页本身即列表页时直接使用；否则在导航链接里找最像列表的入口。"""

        # 入口页若已含成组重复条目，直接把它当列表页
        from crawl_flow.autogen.selectors import repeated_children

        if repeated_children(soup):
            return entry_url
        candidates: list[tuple[int, str]] = []
        for anchor in soup.find_all("a", href=True):
            href = str(anchor["href"])
            text = anchor.get_text(" ", strip=True).lower()
            score = 0
            if any(word in href.lower() for word in ("list", "posts", "blog", "articles", "news")):
                score += 2
            if any(word in text for word in ("列表", "文章", "更多", "all", "list", "posts")):
                score += 1
            if score:
                candidates.append((score, urljoin(entry_url, href)))
        if not candidates:
            raise GenerationError(
                "入口页不是列表页，且未发现指向文章列表的导航链接",
                stage="list_discovery",
            )
        return max(candidates, key=lambda item: item[0])[1]

    def _crawl_list_samples(
        self, list_url: str
    ) -> tuple[list[tuple[str, BeautifulSoup]], dict[str, str]]:
        pages: list[tuple[str, BeautifulSoup]] = []
        raw: dict[str, str] = {}
        url: str | None = list_url
        for index in range(1, GENERATION_MAX_LIST_PAGES + 1):
            if url is None:
                break
            html = self._fetch_html(url, stage=f"list-page-{index}")
            raw[f"page_{index}.html"] = html
            pages.append((url, BeautifulSoup(html, "lxml")))
            url = self._guess_next_url(url, pages[-1][1], index)
        if not pages:
            raise GenerationError("列表样本爬取为空", stage="list_crawl")
        return pages, raw

    def _guess_next_url(self, current_url: str, soup: BeautifulSoup, index: int) -> str | None:
        for anchor in soup.find_all("a", href=True):
            rel = anchor.get("rel")
            rel_values = rel if isinstance(rel, list) else [str(rel)]
            text = anchor.get_text(" ", strip=True).lower()
            if "next" in [v.lower() for v in rel_values] or any(
                hint in text for hint in ("下一页", "下页", "next", "›", "»")
            ):
                return urljoin(current_url, str(anchor["href"]))
        return None

    def _crawl_detail_samples(
        self, urls: list[str]
    ) -> tuple[list[tuple[str, BeautifulSoup]], dict[str, str]]:
        samples: list[tuple[str, BeautifulSoup]] = []
        raw: dict[str, str] = {}
        for url in urls[:GENERATION_MAX_DETAIL_SAMPLES]:
            html = self._fetch_html(url, stage="detail")
            raw[f"{urlparse(url).path.strip('/').replace('/', '_') or 'detail'}.html"] = html
            samples.append((url, BeautifulSoup(html, "lxml")))
        if len(samples) < 2:
            raise GenerationError(
                f"详情样本不足（至少需要 2 个，实际 {len(samples)} 个），无法稳定识别字段结构",
                stage="detail_crawl",
            )
        return samples, raw

    # ---------- 回放校验 ----------

    def _replay_validation(
        self,
        spec: AdapterSpec,
        list_pages: list[tuple[str, BeautifulSoup]],
        detail_samples: list[tuple[str, BeautifulSoup]],
    ) -> None:
        """用 GenericHtmlAdapter 对样本做离线回放，确认规则真的能提取出数据。"""

        from crawl_flow.sources.generic_html import GenericHtmlAdapter

        class _OfflineSource:
            name = "sandbox-replay"
            adapter_spec = spec.model_dump(mode="json")
            base_url = spec.base_url

        adapter = GenericHtmlAdapter(_OfflineSource(), fetcher=None)  # type: ignore[arg-type]
        from crawl_flow.fetch.types import FetchResult

        # 列表回放
        first_url, first_soup = list_pages[0]
        list_items = adapter.parse_list(
            FetchResult(url=first_url, status_code=200, headers={}, body=str(first_soup), elapsed_ms=0)
        )
        if not list_items:
            raise GenerationError("回放校验失败：列表规则未提取到任何条目", stage="replay")

        # 详情回放：每个必填字段都要在每个样本上有值
        required = {name for name, rule in spec.detail.fields.items() if not rule.optional}
        for url, soup in detail_samples:
            fields = adapter.parse_detail(
                FetchResult(url=url, status_code=200, headers={}, body=str(soup), elapsed_ms=0),
                external_id="replay",
            )
            missing = sorted(name for name in required if not fields.get(name))
            if missing:
                raise GenerationError(
                    f"回放校验失败：必填字段提取为空 {missing}",
                    stage="replay",
                    detail=url,
                )


# ---------- 辅助 ----------


def _coerce_field(item: RequiredField | dict[str, object] | str) -> RequiredField:
    if isinstance(item, RequiredField):
        return item
    if isinstance(item, str):
        return RequiredField(name=item)
    name = item.get("name")
    if not isinstance(name, str) or not name.strip():
        raise GenerationError("必填字段声明缺少 name", stage="input")
    keywords = item.get("keywords", ())
    if isinstance(keywords, str):
        keywords = tuple(part.strip() for part in keywords.split(",") if part.strip())
    return RequiredField(name=name.strip(), keywords=tuple(keywords))


def _validate_field_names(fields: list[RequiredField]) -> None:
    allowed = set(__import__("crawl_flow.autogen.detail_analysis", fromlist=["FIELD_KEYWORDS"]).FIELD_KEYWORDS)
    unknown = sorted({f.name for f in fields} - allowed)
    if unknown:
        raise GenerationError(
            f"不支持的标准字段: {', '.join(unknown)}，可选: {', '.join(sorted(allowed))}",
            stage="input",
        )
    if "title" not in {f.name for f in fields}:
        raise GenerationError("必填字段必须包含 title（管道归一化要求标题非空）", stage="input")


def _build_detail_path(sample_url: str, id_pattern: str) -> str:
    """把样本详情 URL 还原成带 {id} 占位符的路径模板。"""

    parsed = urlparse(sample_url)
    if id_pattern.startswith("[?&]"):
        # 查询参数型：/detail?id=xxx → /detail?id={id}
        import re

        match = re.search(id_pattern, sample_url)
        if match:
            param = sample_url[match.start() + 1 :].split("=", 1)[0]
            return f"{parsed.path}?{param}={{id}}"
    segments = parsed.path.rstrip("/").split("/")
    if segments:
        segments[-1] = "{id}"
    return "/".join(segments) or "/{id}"
