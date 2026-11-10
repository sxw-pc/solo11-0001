"""渲染器注册表与工厂：管道只依赖 :class:`~crawl_flow.render.base.Renderer`。"""

from __future__ import annotations

from collections.abc import Callable

from crawl_flow.errors import RendererDependencyError
from crawl_flow.render.base import Renderer

DEFAULT_RENDERER = "playwright"

# 工厂延迟到实例化时才导入具体后端，保证未安装 Playwright 也能导入本包。
_RENDERER_FACTORIES: dict[str, Callable[..., Renderer]] = {}


def register_renderer(name: str, factory: Callable[..., Renderer]) -> None:
    """注册一个渲染后端，便于在不改管道代码的前提下插拔实现。"""

    _RENDERER_FACTORIES[name] = factory


def available_renderers() -> list[str]:
    """返回已注册的渲染后端名称。"""

    return sorted(_RENDERER_FACTORIES)


def _playwright_factory(**kwargs: object) -> Renderer:
    from crawl_flow.render.playwright_renderer import PlaywrightRenderer

    return PlaywrightRenderer(**kwargs)


register_renderer("playwright", _playwright_factory)


def create_renderer(kind: str = DEFAULT_RENDERER, **kwargs: object) -> Renderer:
    """按名称创建渲染器；未知后端抛 :class:`RendererDependencyError`。"""

    factory = _RENDERER_FACTORIES.get(kind)
    if factory is None:
        known = ", ".join(available_renderers()) or "（无）"
        raise RendererDependencyError(f"未知的渲染后端: {kind}，已注册: {known}")
    return factory(**kwargs)
