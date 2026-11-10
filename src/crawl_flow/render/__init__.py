"""可选页面渲染能力（Playwright 后端，依赖按需安装）。"""

from crawl_flow.render.base import Renderer
from crawl_flow.render.factory import (
    DEFAULT_RENDERER,
    available_renderers,
    create_renderer,
    register_renderer,
)

__all__ = [
    "DEFAULT_RENDERER",
    "Renderer",
    "available_renderers",
    "create_renderer",
    "register_renderer",
]
