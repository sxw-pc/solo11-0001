"""源适配器注册表与工厂。"""

from __future__ import annotations

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ConfigError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.render.base import Renderer
from crawl_flow.sources.base import ListItem, ListPage, SourceAdapter
from crawl_flow.sources.demo_api import DemoApiAdapter
from crawl_flow.sources.demo_html import DemoHtmlAdapter

ADAPTERS: dict[str, type[SourceAdapter]] = {
    "api": DemoApiAdapter,
    "html": DemoHtmlAdapter,
}


def create_adapter(
    source: SourceConfig,
    fetcher: Fetcher,
    renderer: Renderer | None = None,
) -> SourceAdapter:
    """按 ``source.type`` 实例化适配器；新增类型时在这里注册。"""

    adapter_cls = ADAPTERS.get(source.type)
    if adapter_cls is None:
        raise ConfigError(f"未知的源类型: {source.type}")
    return adapter_cls(source, fetcher, renderer)


__all__ = [
    "ADAPTERS",
    "ListItem",
    "ListPage",
    "SourceAdapter",
    "create_adapter",
]
