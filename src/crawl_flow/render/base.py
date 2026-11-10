"""渲染器统一接口。

管道与适配器只依赖本模块的 :class:`Renderer` 抽象，不感知具体的浏览器
实现；新增渲染后端只需实现该接口并在 ``render.factory`` 注册。
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from crawl_flow.fetch.types import FetchResult


class Renderer(ABC):
    """页面渲染器接口：把一个 URL 渲染成与静态抓取同构的 :class:`FetchResult`。"""

    name: str = "base"

    @abstractmethod
    def render(self, url: str) -> FetchResult:
        """打开 ``url``、执行页面脚本后返回最终 DOM 的抓取结果。

        传输/导航失败抛 :class:`~crawl_flow.errors.RendererError`；
        HTTP 错误状态码不在此抛错，交由调用方按既有规则判断。
        """

    def close(self) -> None:
        """释放浏览器等底层资源，默认无操作。"""

    def __enter__(self) -> "Renderer":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
