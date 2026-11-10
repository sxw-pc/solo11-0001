"""基于 Playwright 的渲染器（可选依赖）。

Playwright 不进入核心依赖：未安装时本模块仍可正常导入，只有真正实例化
:class:`PlaywrightRenderer` 才会抛出带安装指引的
:class:`~crawl_flow.errors.RendererDependencyError`，而不是裸 ``ImportError``。
"""

from __future__ import annotations

import importlib.util
import time

from crawl_flow.errors import RendererDependencyError, RendererError
from crawl_flow.fetch.types import FetchResult
from crawl_flow.render.base import Renderer

PACKAGE_NAME = "playwright"
INSTALL_HINT = (
    "渲染源需要可选依赖 Playwright，请先安装：\n"
    "  pip install 'crawl-flow[render]'\n"
    "  python -m playwright install chromium"
)
DEFAULT_USER_AGENT = "crawl-flow/0.1 (+https://example.invalid/crawl-flow) render"


def playwright_available() -> bool:
    """探测 Playwright Python 包是否已安装，不触发导入。"""

    return importlib.util.find_spec(PACKAGE_NAME) is not None


class PlaywrightRenderer(Renderer):
    """用 Chromium 加载页面并返回 JS 执行后的最终 HTML。"""

    name = "playwright"

    def __init__(
        self,
        *,
        timeout: float = 30.0,
        user_agent: str = DEFAULT_USER_AGENT,
        headless: bool = True,
    ) -> None:
        if not playwright_available():
            raise RendererDependencyError(
                f"未安装可选依赖 {PACKAGE_NAME!r}，无法运行 render 源。{INSTALL_HINT}"
            )
        try:
            from playwright.sync_api import sync_playwright
        except ImportError as exc:  # pragma: no cover - find_spec 已兜底，防御并发卸载等场景
            raise RendererDependencyError(
                f"导入 {PACKAGE_NAME!r} 失败，无法运行 render 源。{INSTALL_HINT}"
            ) from exc

        self._timeout_ms = int(timeout * 1000)
        self._playwright = sync_playwright().start()
        try:
            self._browser = self._playwright.chromium.launch(
                headless=headless,
                timeout=self._timeout_ms,
            )
        except Exception as exc:
            self._playwright.stop()
            raise RendererDependencyError(
                f"Playwright 浏览器内核不可用（可能尚未执行 "
                f"`python -m playwright install chromium`）：{exc.__class__.__name__}: {exc}\n"
                f"{INSTALL_HINT}"
            ) from exc
        self._user_agent = user_agent

    def render(self, url: str) -> FetchResult:
        """打开 ``url`` 并返回渲染后 DOM；导航失败抛 :class:`RendererError`。"""

        from playwright.sync_api import Error as PlaywrightError

        started = time.perf_counter()
        context = self._browser.new_context(
            user_agent=self._user_agent,
            timeout=self._timeout_ms,
        )
        page = context.new_page()
        try:
            try:
                response = page.goto(url, wait_until="load", timeout=self._timeout_ms)
            except PlaywrightError as exc:
                raise RendererError(
                    f"渲染失败 url={url} error={exc.__class__.__name__}: {exc}"
                ) from exc
            if response is None:
                raise RendererError(f"渲染失败 url={url} error=未获得导航响应")
            body = page.content()
            return FetchResult(
                url=page.url,
                status_code=response.status,
                headers=dict(response.headers),
                body=body,
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )
        finally:
            context.close()

    def close(self) -> None:
        try:
            self._browser.close()
        finally:
            self._playwright.stop()
