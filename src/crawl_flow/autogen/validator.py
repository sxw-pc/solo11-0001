"""定期适配校验：用最新站点样本校验已有适配器，检测结构漂移并给出调整建议。

校验任务与正常采集共享同一个 :class:`RateLimiter`，并固定以
``PRIORITY_VALIDATION`` 低优先级入场，因此不会抢占正常业务任务的资源。

输出 :class:`ValidationReport`，包含：

- ``status``：``healthy`` / ``drifted`` / ``broken``；
- ``findings``：每个失效选择器的具体现象（在第几页、哪个字段）；
- ``suggestions``：可直接落地的修复建议（新选择器、对应字段与置信度），
  建议由同一份结构分析器重新识别得到，可用于一键再生成。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawl_flow.autogen.constants import VALIDATION_MAX_DETAIL_SAMPLES
from crawl_flow.autogen.detail_analysis import RequiredField, analyze_detail
from crawl_flow.autogen.errors import AutoGenError
from crawl_flow.autogen.limiter import PRIORITY_VALIDATION
from crawl_flow.autogen.list_analysis import analyze_list
from crawl_flow.autogen.polite import PoliteFetcher
from crawl_flow.autogen.sandbox import assert_sample_safe
from crawl_flow.autogen.spec import AdapterSpec

LOGGER_NAME = "crawl_flow.autogen.validator"


@dataclass(frozen=True)
class DriftFinding:
    component: str  # list_item / list_link / pagination / detail_scope / field:<name>
    expected_selector: str
    detail: str


@dataclass(frozen=True)
class AdjustmentSuggestion:
    component: str
    old_selector: str
    new_selector: str
    confidence: str  # high / medium
    rationale: str


@dataclass
class ValidationReport:
    entry_url: str
    status: str  # healthy / drifted / broken
    findings: list[DriftFinding] = field(default_factory=list)
    suggestions: list[AdjustmentSuggestion] = field(default_factory=list)
    sampled_list_pages: int = 0
    sampled_details: int = 0

    @property
    def changed(self) -> bool:
        return self.status in {"drifted", "broken"}


class AdapterValidator:
    """对已落盘的 :class:`AdapterSpec` 做在线适配校验。"""

    def __init__(self, *, fetcher: PoliteFetcher | None = None) -> None:
        self._fetcher = fetcher or PoliteFetcher()
        self._owns_fetcher = fetcher is None
        self._log = logging.getLogger(LOGGER_NAME)

    def validate(self, spec: AdapterSpec, *, entry_url: str | None = None) -> ValidationReport:
        target = entry_url or self._entry_from_spec(spec)
        parsed = urlparse(target)
        # 允许用 entry_url 指向同结构的其他地址（改版演练 / 域名迁移）：
        # base 与列表路径都以传入地址为准，规格中的地址仅在未覆盖时使用。
        self._base = f"{parsed.scheme}://{parsed.netloc}"
        self._list_path = parsed.path or spec.list.list_path
        report = ValidationReport(entry_url=target, status="healthy")
        try:
            list_pages = self._crawl_lists(spec, target, report)
            detail_pages, detail_urls = self._crawl_details(spec, list_pages, report)
            self._check_list(spec, list_pages, report)
            self._check_detail(spec, detail_pages, report)
            if report.findings:
                report.status = "drifted"
                self._build_suggestions(spec, list_pages, detail_pages, report)
        except AutoGenError as exc:
            # 样本都拿不到或结构彻底无法识别 → broken，并给出原因
            report.status = "broken"
            report.findings.append(
                DriftFinding(component="site", expected_selector="-", detail=str(exc))
            )
        finally:
            if self._owns_fetcher:
                self._fetcher.close()
        self._log.info(
            "validation done url=%s status=%s findings=%d suggestions=%d",
            target,
            report.status,
            len(report.findings),
            len(report.suggestions),
        )
        return report

    # ---------- 爬取（低优先级） ----------

    def _get(self, url: str) -> str:
        result = self._fetcher.get(url, priority=PRIORITY_VALIDATION)
        if result.status_code >= 400:
            raise AutoGenError(f"校验请求失败 HTTP {result.status_code}: {url}")
        assert_sample_safe(result.body, source=url)
        return result.body

    def _crawl_lists(
        self, spec: AdapterSpec, target: str, report: ValidationReport
    ) -> list[tuple[str, BeautifulSoup]]:
        pages: list[tuple[str, BeautifulSoup]] = []
        url: str | None = self._list_url(spec)
        for index in range(2):  # 校验只取前两页，保持轻量
            if url is None:
                break
            html = self._get(url)
            pages.append((url, BeautifulSoup(html, "lxml")))
            url = self._next_url(spec, pages[-1][1], url)
        report.sampled_list_pages = len(pages)
        if not pages:
            raise AutoGenError("未取到任何列表页样本")
        return pages

    def _crawl_details(
        self,
        spec: AdapterSpec,
        list_pages: list[tuple[str, BeautifulSoup]],
        report: ValidationReport,
    ) -> tuple[list[tuple[str, BeautifulSoup]], list[str]]:
        # 直接按规格的列表规则取条目；若规则已失效，则从页面中收集任意内链兜底
        urls: list[str] = []
        first_url, first_soup = list_pages[0]
        for item in first_soup.select(spec.list.item_selector):
            link = item.select_one(spec.list.link_selector)
            href = link.get(spec.list.link_attr) if link is not None else None
            if isinstance(href, str) and href:
                urls.append(urljoin(first_url, href))
        if not urls:
            for anchor in first_soup.find_all("a", href=True):
                href = str(anchor["href"])
                if urlparse(href).path and not href.startswith(("#", "javascript:")):
                    urls.append(urljoin(first_url, href))
        urls = list(dict.fromkeys(urls))[:VALIDATION_MAX_DETAIL_SAMPLES]

        pages: list[tuple[str, BeautifulSoup]] = []
        for url in urls:
            html = self._get(url)
            pages.append((url, BeautifulSoup(html, "lxml")))
        report.sampled_details = len(pages)
        return pages, urls

    # ---------- 结构核对 ----------

    def _check_list(
        self,
        spec: AdapterSpec,
        pages: list[tuple[str, BeautifulSoup]],
        report: ValidationReport,
    ) -> None:
        url, soup = pages[0]
        if not soup.select(spec.list.item_selector):
            report.findings.append(
                DriftFinding(
                    component="list_item",
                    expected_selector=spec.list.item_selector,
                    detail=f"列表条目选择器在 {url} 未命中任何元素",
                )
            )
            return
        for index, item in enumerate(soup.select(spec.list.item_selector)):
            if not item.select_one(spec.list.link_selector):
                report.findings.append(
                    DriftFinding(
                        component="list_link",
                        expected_selector=spec.list.link_selector,
                        detail=f"第 {index + 1} 个条目内详情链接选择器未命中",
                    )
                )
                break
        if spec.list.pagination == "next_link":
            if len(pages) < 2 and not soup.select_one(spec.list.next_selector or ""):
                report.findings.append(
                    DriftFinding(
                        component="pagination",
                        expected_selector=spec.list.next_selector or "",
                        detail="下一页链接选择器未命中，分页规则可能已变更",
                    )
                )

    def _check_detail(
        self,
        spec: AdapterSpec,
        pages: list[tuple[str, BeautifulSoup]],
        report: ValidationReport,
    ) -> None:
        for url, soup in pages:
            scope = soup.select_one(spec.detail.scope_selector)
            if scope is None:
                report.findings.append(
                    DriftFinding(
                        component="detail_scope",
                        expected_selector=spec.detail.scope_selector,
                        detail=f"详情容器选择器在 {url} 未命中",
                    )
                )
                continue
            for name, rule in spec.detail.fields.items():
                if not scope.select(rule.selector):
                    report.findings.append(
                        DriftFinding(
                            component=f"field:{name}",
                            expected_selector=rule.selector,
                            detail=f"字段 {name} 的选择器在 {url} 未命中",
                        )
                    )

    # ---------- 调整建议 ----------

    def _build_suggestions(
        self,
        spec: AdapterSpec,
        list_pages: list[tuple[str, BeautifulSoup]],
        detail_pages: list[tuple[str, BeautifulSoup]],
        report: ValidationReport,
    ) -> None:
        affected = {f.component for f in report.findings}

        if {"list_item", "list_link", "pagination"} & affected:
            try:
                fresh_list = analyze_list(list_pages)
                if fresh_list.item_selector != spec.list.item_selector:
                    report.suggestions.append(
                        AdjustmentSuggestion(
                            component="list_item",
                            old_selector=spec.list.item_selector,
                            new_selector=fresh_list.item_selector,
                            confidence="high",
                            rationale="重新识别到的重复条目容器选择器",
                        )
                    )
                if fresh_list.link_selector != spec.list.link_selector:
                    report.suggestions.append(
                        AdjustmentSuggestion(
                            component="list_link",
                            old_selector=spec.list.link_selector,
                            new_selector=fresh_list.link_selector,
                            confidence="high",
                            rationale="重新识别到的条目内详情链接选择器",
                        )
                    )
                if (
                    spec.list.pagination == "next_link"
                    and fresh_list.next_selector
                    and fresh_list.next_selector != spec.list.next_selector
                ):
                    report.suggestions.append(
                        AdjustmentSuggestion(
                            component="pagination",
                            old_selector=spec.list.next_selector or "",
                            new_selector=fresh_list.next_selector,
                            confidence="medium",
                            rationale="重新识别到的下一页链接选择器",
                        )
                    )
            except AutoGenError:
                pass  # 列表彻底无法识别时保持只有 findings 的 broken/drifted 结论

        if any(component.startswith(("detail_scope", "field:")) for component in affected):
            try:
                required = [
                    RequiredField(name=name) for name in spec.detail.fields
                ]
                scope_selector, fresh_rules = analyze_detail(detail_pages, required)
                if scope_selector != spec.detail.scope_selector:
                    report.suggestions.append(
                        AdjustmentSuggestion(
                            component="detail_scope",
                            old_selector=spec.detail.scope_selector,
                            new_selector=scope_selector,
                            confidence="high",
                            rationale="重新识别到的详情正文容器选择器",
                        )
                    )
                for name, rule in fresh_rules.items():
                    old = spec.detail.fields[name].selector
                    if rule.selector != old:
                        report.suggestions.append(
                            AdjustmentSuggestion(
                                component=f"field:{name}",
                                old_selector=old,
                                new_selector=rule.selector,
                                confidence="high",
                                rationale=f"字段 {name} 在最新样本上的稳定选择器",
                            )
                        )
            except AutoGenError:
                pass

    # ---------- URL 辅助 ----------

    def _entry_from_spec(self, spec: AdapterSpec) -> str:
        return f"{spec.base_url}{spec.list.list_path}"

    def _list_url(self, spec: AdapterSpec) -> str:
        base = getattr(self, "_base", spec.base_url)
        path = getattr(self, "_list_path", spec.list.list_path)
        return f"{base}{path}"

    def _next_url(
        self, spec: AdapterSpec, soup: BeautifulSoup, current_url: str
    ) -> str | None:
        if spec.list.pagination != "next_link":
            return None
        anchor = soup.select_one(spec.list.next_selector or "")
        href = anchor.get("href") if anchor is not None else None
        return urljoin(current_url, href) if isinstance(href, str) and href else None
