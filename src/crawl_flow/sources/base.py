"""适配器抽象基类与翻页通用逻辑。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterator
from urllib.parse import quote

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.types import FetchResult
from crawl_flow.sources.pagination import (
    PaginationState,
    PaginationStateMachine,
)

MAX_PAGES = 500


def build_url(base_url: str, path: str) -> str:
    """拼接 base_url 与路径，兼容路径中已经带有查询串的情况。"""

    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


@dataclass(frozen=True)
class ListItem:
    """列表页中的一条索引信息。"""

    external_id: str
    url: str


@dataclass(frozen=True)
class ListPage:
    """一页列表的解析结果。"""

    page: int
    items: list[ListItem] = field(default_factory=list)


class SourceAdapter(ABC):
    """源适配器接口：列表翻页、列表解析、详情解析、详情 URL。

    子类必须实现 :meth:`list_url` / :meth:`parse_list` /
    :meth:`parse_detail` / :meth:`detail_url` 四个抽象方法。
    :meth:`list_response_page` / :meth:`list_total_items` /
    :meth:`list_has_next` 是翻页状态机使用的可选钩子，默认全部返回
    ``None``（不提供对应信号），子类按需覆写，不改变适配器扩展接口。
    """

    def __init__(self, source: SourceConfig, fetcher: Fetcher) -> None:
        self.source = source
        self.fetcher = fetcher

    # ---- 子类必须实现 ----
    @abstractmethod
    def list_url(self, page: int) -> str:
        """第 ``page`` 页列表的 URL。"""

    @abstractmethod
    def parse_list(self, result: FetchResult) -> list[ListItem]:
        """解析列表页，返回当页条目；空页返回空列表。"""

    @abstractmethod
    def parse_detail(self, result: FetchResult, external_id: str) -> dict[str, Any]:
        """解析详情页，返回源字段字典（键名供 field_map 使用）。"""

    @abstractmethod
    def detail_url(self, external_id: str) -> str:
        """详情页 URL。"""

    # ---- 通用能力 ----
    def list_params(self, page: int) -> dict[str, object]:
        """列表页查询参数，默认只有 ``page``。"""

        return {"page": page}

    @property
    def page_size_hint(self) -> int:
        """一整页的期望条数：实际条数小于它时状态机按短页终止。

        默认取配置里的 ``page_size``；不按固定页大小返回的适配器（如
        HTML 列表）可覆写为自身的每页条数约定。
        """

        return self.source.page_size

    def list_response_page(self, result: FetchResult) -> int | None:
        """从响应中提取站点自报的页码；不提供页码信号时返回 ``None``。

        返回值与请求页码不一致时，翻页状态机按抓取错误终止整轮。
        """

        return None

    def list_total_items(self, result: FetchResult) -> int | None:
        """从响应中提取站点自报的总条数；不提供总数时返回 ``None``。

        累计条目达到总数后状态机正常终止，不再请求下一页。
        """

        return None

    def list_has_next(self, result: FetchResult) -> bool | None:
        """响应是否显式声明还有下一页；无此信号时返回 ``None``。

        返回 ``False`` 时状态机在当前页处理完后正常终止。
        """

        return None

    def fetch_detail(self, external_id: str) -> FetchResult:
        """请求详情页；HTTP 状态码交由管道判断，不在此处抛错。"""

        return self.fetcher.get(self.detail_url(external_id))

    def iter_list_pages(
        self,
        *,
        state: PaginationState | None = None,
    ) -> Iterator[ListPage]:
        """从第 1 页开始翻页，终止规则由 :class:`PaginationStateMachine` 统一判定。

        ``state`` 可传入断点续采 / 失败重试保存下来的翻页状态；默认新建
        独立状态。每次调用都创建新的状态机实例，因此同一适配器上并发的
        两个翻页迭代互不共享可变状态。

        列表页 4xx/5xx 抛 :class:`~crawl_flow.errors.FetchError`，页码不
        一致 / 连续重复页抛 :class:`~crawl_flow.errors.PaginationError`，
        解析失败抛 :class:`~crawl_flow.errors.ParseError`，均终止整轮。
        """

        machine = PaginationStateMachine(self, state=state, max_pages=MAX_PAGES)
        yield from machine.run()

    def _parse_error(self, external_id: str, reason: str) -> ParseError:
        return ParseError(self.source.name, external_id, reason)

    def _id_url(self, external_id: str) -> str:
        return build_url(
            self.source.base_url,
            self.source.detail_path.format(id=quote(external_id, safe="")),
        )
