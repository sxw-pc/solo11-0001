"""适配器抽象基类与翻页通用逻辑。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Iterator
from urllib.parse import quote

from crawl_flow.config import SourceConfig
from crawl_flow.errors import FetchError, ParseError
from crawl_flow.fetch.client import DEFAULT_USER_AGENT, Fetcher
from crawl_flow.fetch.types import FetchResult

MAX_PAGES = 500


def build_fetcher(source: SourceConfig, *, timeout: float | None = None) -> Fetcher:
    """按源配置构建 Fetcher，复用 ``timeout_seconds``、``user_agent`` 与 ``headers``。"""

    return Fetcher(
        timeout=source.timeout_seconds if timeout is None else timeout,
        user_agent=source.user_agent or DEFAULT_USER_AGENT,
        headers=source.headers,
    )


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
    """源适配器接口：列表翻页、列表解析、详情解析、详情 URL。"""

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

    def fetch_detail(self, external_id: str) -> FetchResult:
        """请求详情页；HTTP 状态码交由管道判断，不在此处抛错。"""

        return self.fetcher.get(self.detail_url(external_id))

    def fetch_list_page(self, page: int, *, timeout: float | None = None) -> FetchResult:
        """请求一页列表；``timeout`` 可覆盖客户端超时（健康探测使用）。

        健康探测与正常翻页都走这一个方法，保证 URL、参数与请求头规则完全一致。
        """

        return self.fetcher.get(
            self.list_url(page),
            params=self.list_params(page),
            timeout=timeout,
        )

    def iter_list_pages(self) -> Iterator[ListPage]:
        """从第 1 页开始翻页，遇到空页终止。

        列表页 4xx/5xx 抛 :class:`FetchError`（整轮失败），解析失败抛
        :class:`ParseError`。
        """

        page = 1
        while page <= MAX_PAGES:
            result = self.fetch_list_page(page)
            if result.status_code >= 400:
                raise FetchError(
                    f"列表页请求失败 source={self.source.name} page={page} "
                    f"status={result.status_code}",
                    url=result.url,
                    status_code=result.status_code,
                )
            items = self.parse_list(result)
            yield ListPage(page=page, items=items)
            if not items:
                return
            page += 1
        raise FetchError(
            f"列表页超过最大翻页数 source={self.source.name} max_pages={MAX_PAGES}",
            url=self.list_url(page),
        )

    def _parse_error(self, external_id: str, reason: str) -> ParseError:
        return ParseError(self.source.name, external_id, reason)

    def _id_url(self, external_id: str) -> str:
        return build_url(
            self.source.base_url,
            self.source.detail_path.format(id=quote(external_id, safe="")),
        )
