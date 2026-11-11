"""翻页状态机：API 与 HTML 适配器共用的唯一翻页终止逻辑。

状态机只负责「什么时候继续、什么时候终止、按什么错误终止」，不关心响应
如何解析——解析由适配器通过三个可选钩子提供：

- :meth:`SourceAdapter.list_response_page`：响应里自报的页码（如 JSON 的
  ``page`` 字段）。返回 ``None`` 表示该适配器不提供页码，不做一致性校验。
- :meth:`SourceAdapter.list_total_items`：响应里自报的总条数。返回
  ``None`` 表示不提供总数，不按总数提前终止。
- :meth:`SourceAdapter.list_has_next`：响应是否明确声明还有下一页。返回
  ``None`` 表示不提供该信号，退化为空页 / 短页终止。

终止条件（按判定顺序）：

1. 列表页 HTTP 状态码 ≥ 400 → :class:`~crawl_flow.errors.FetchError`，
   整轮失败，且不会再发后续请求。
2. 响应页码与请求页码不一致 → :class:`~crawl_flow.errors.PaginationError`，
   立即终止整轮，不触发 500 页保护。
3. 空页（当页 0 条）→ 正常终止，不产生额外请求。
4. 连续重复页（相邻两页条目指纹完全相同且非空）→
   :class:`~crawl_flow.errors.PaginationError`，避免无限重复。
5. 总数已满足或站点声明没有下一页 → 正常终止，不再请求下一页
   （因此 ``limit`` 恰好是每页条数整数倍时不会多打一页空页）。
6. 当页条数小于每页条数（短页）→ 正常终止（无总数 / 无下一页信号时）。
7. 达到 :data:`~crawl_flow.sources.base.MAX_PAGES`（500）→
   :class:`~crawl_flow.errors.FetchError`，上限保护保留。

状态全部保存在 :class:`PaginationState` 中，可通过 :meth:`snapshot` /
:meth:`restore` 序列化复用，供断点续采与失败重试使用；每个翻页迭代持有
自己的状态实例，同一源并发两个翻页任务互不污染。
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from crawl_flow.errors import FetchError, PaginationError

if TYPE_CHECKING:
    from crawl_flow.sources.base import ListPage, SourceAdapter


@dataclass
class PaginationState:
    """翻页状态机的可序列化状态。

    所有字段都是内置类型，:meth:`to_dict` / :meth:`from_dict` 可直接用于
    断点持久化；恢复后的状态机与未中断的迭代行为一致。
    """

    page: int = 1
    """下一个待抓取的请求页码，从 1 开始；某页被接受后立即推进。

    因此在页边界（yield 之后）保存的快照里，``page`` 指向尚未抓取的下
    一页，恢复后不会重放已完成的页。
    """

    pages_fetched: int = 0
    """已完成的列表页请求次数，与 HTTP 客户端日志中的列表请求逐次对齐。"""

    items_seen: int = 0
    """累计已产出的列表条目数。"""

    total_items: int | None = None
    """站点自报的总条数；未提供时为 ``None``。"""

    last_fingerprint: tuple[str, ...] | None = None
    """上一页条目指纹，用于连续重复页检测。"""

    finished: bool = False
    """是否已正常终止（空页 / 总数满足 / 无下一页 / 短页）。"""

    stop_reason: str | None = None
    """终止原因，正常终止时记录，便于断点续采判断与排障。"""

    def to_dict(self) -> dict[str, Any]:
        """导出为可 JSON 序列化的字典。"""

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaginationState":
        """从 :meth:`to_dict` 的字典恢复状态；未知字段忽略以便向前兼容。"""

        known = set(cls.__dataclass_fields__)
        payload = {key: value for key, value in data.items() if key in known}
        fingerprint = payload.get("last_fingerprint")
        if fingerprint is not None:
            payload["last_fingerprint"] = tuple(fingerprint)
        return cls(**payload)


class PaginationStateMachine:
    """驱动单个适配器翻页的状态机，API 与 HTML 适配器共用。

    一次 :meth:`SourceAdapter.iter_list_pages` 调用对应一个状态机实例；
    实例之间不共享任何可变状态，因此同一源并发两个翻页任务互不污染。
    """

    def __init__(
        self,
        adapter: "SourceAdapter",
        *,
        state: PaginationState | None = None,
        max_pages: int,
    ) -> None:
        self._adapter = adapter
        self._state = state or PaginationState()
        self._max_pages = max_pages

    @property
    def state(self) -> PaginationState:
        return self._state

    def snapshot(self) -> dict[str, Any]:
        """导出当前状态快照（断点续采 / 失败重试复用）。"""

        return self._state.to_dict()

    @classmethod
    def restore(
        cls,
        adapter: "SourceAdapter",
        snapshot: dict[str, Any],
        *,
        max_pages: int,
    ) -> "PaginationStateMachine":
        """从快照恢复状态机，恢复后可继续翻页。"""

        return cls(adapter, state=PaginationState.from_dict(snapshot), max_pages=max_pages)

    def run(self) -> Iterator["ListPage"]:
        """逐页推进状态机并产出 :class:`~crawl_flow.sources.base.ListPage`。

        终止或出错时保证不再发起新的列表请求；列表页抓取错误以异常形式
        向上抛出，由管道按整轮失败处理。
        """

        from crawl_flow.sources.base import ListPage

        adapter = self._adapter
        source_name = adapter.source.name
        state = self._state

        while not state.finished:
            if state.page > self._max_pages:
                raise FetchError(
                    f"列表页超过最大翻页数 source={source_name} "
                    f"max_pages={self._max_pages}",
                    url=adapter.list_url(state.page),
                )

            page = state.page
            result = adapter.fetcher.get(
                adapter.list_url(page),
                params=adapter.list_params(page),
            )
            state.pages_fetched += 1
            if result.status_code >= 400:
                raise FetchError(
                    f"列表页请求失败 source={source_name} page={page} "
                    f"status={result.status_code}",
                    url=result.url,
                    status_code=result.status_code,
                )

            items = adapter.parse_list(result)

            # 1) 响应页码一致性：不一致属于抓取错误，立即终止整轮，
            #    不走 500 页保护。
            response_page = adapter.list_response_page(result)
            if response_page is not None and response_page != page:
                raise PaginationError(
                    f"列表页响应页码与请求页码不一致 source={source_name} "
                    f"request_page={page} response_page={response_page}",
                    url=result.url,
                )

            total = adapter.list_total_items(result)
            if total is not None and total >= 0:
                state.total_items = total

            fingerprint = tuple(item.external_id for item in items)

            # 2) 空页立即终止：空页本身就是末页信号，不再请求下一页。
            if not items:
                state.finished = True
                state.stop_reason = "empty_page"
                yield ListPage(page=page, items=items)
                return

            # 3) 连续重复页：相邻两页非空且指纹完全相同，判定为站点翻页
            #    失效，按抓取错误终止，避免一直重复到 500 页上限。
            if state.last_fingerprint is not None and fingerprint == state.last_fingerprint:
                raise PaginationError(
                    f"列表页连续重复 source={source_name} page={page} "
                    f"items={len(items)}",
                    url=result.url,
                )
            state.last_fingerprint = fingerprint
            state.items_seen += len(items)
            # 当前页已被接受，先推进下一次请求页码，保证迭代中途被放弃
            # （如断点续采）时快照里的 page 指向待抓取的下一页。
            state.page = page + 1

            yield ListPage(page=page, items=items)

            # 4) 总数满足：站点声明的条目已经全部取到，不再请求下一页。
            if state.total_items is not None and state.items_seen >= state.total_items:
                state.finished = True
                state.stop_reason = "total_reached"
                return

            # 5) 站点显式声明没有下一页（如 HTML 缺少「下一页」链接）。
            has_next = adapter.list_has_next(result)
            if has_next is False:
                state.finished = True
                state.stop_reason = "no_next_page"
                return

            # 6) 短页：无总数 / 无下一页信号时，不足一整页即视为末页。
            if len(items) < adapter.page_size_hint:
                state.finished = True
                state.stop_reason = "short_page"
                return

        state.stop_reason = state.stop_reason or "already_finished"
