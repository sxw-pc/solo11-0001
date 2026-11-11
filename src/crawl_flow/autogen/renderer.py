"""可选的浏览器动态渲染器（默认关闭，必须显式开启）。

渲染依赖（Playwright）不进默认依赖清单：

- 未开启动态渲染时本模块完全不会被调用；
- 显式开启但环境未安装 Playwright/浏览器时，抛出带安装指引的
  :class:`UnsupportedSiteError`，而不是静默降级成静态抓取；
- 渲染结果同样要经过沙箱扫描，浏览器仅用于取渲染后 DOM，不执行站点脚本以外的动作。
"""

from __future__ import annotations

from crawl_flow.autogen.errors import UnsupportedSiteError

INSTALL_HINT = (
    "动态渲染需要可选依赖：pip install playwright && playwright install chromium；"
    "默认安装不包含浏览器组件"
)


class BrowserRenderer:
    """Playwright 无头浏览器的薄封装；构造即校验依赖是否可用。"""

    def __init__(self, *, timeout_ms: int = 15000) -> None:
        try:
            from playwright.sync_api import sync_playwright  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - 取决于运行环境
            raise UnsupportedSiteError(
                f"已显式开启动态渲染，但环境缺少渲染依赖。{INSTALL_HINT}",
                reason_code="renderer_unavailable",
            ) from exc
        self._sync_playwright = sync_playwright
        self._timeout_ms = timeout_ms
        self._playwright = None
        self._browser = None

    def __enter__(self) -> "BrowserRenderer":
        self._playwright = self._sync_playwright().start()
        try:
            self._browser = self._playwright.chromium.launch(headless=True)
        except Exception as exc:  # pragma: no cover - 浏览器未安装等环境问题
            self._playwright.stop()
            raise UnsupportedSiteError(
                f"浏览器启动失败。{INSTALL_HINT} 原始错误: {exc}",
                reason_code="renderer_unavailable",
            ) from exc
        return self

    def render_html(self, url: str) -> str:
        """打开页面并返回 JS 执行后的 DOM 序列化结果。"""

        if self._browser is None:  # pragma: no cover - 防御性
            raise UnsupportedSiteError(
                f"渲染器未初始化。{INSTALL_HINT}",
                reason_code="renderer_unavailable",
            )
        page = self._browser.new_page()
        try:
            page.goto(url, timeout=self._timeout_ms, wait_until="networkidle")
            return page.content()
        finally:
            page.close()

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        if self._browser is not None:
            self._browser.close()
        if self._playwright is not None:
            self._playwright.stop()
