"""demo_api：分页 JSON 列表 + JSON 详情适配器。"""

from __future__ import annotations

import json
from typing import Any

from crawl_flow.fetch.types import FetchResult
from crawl_flow.sources.base import ListItem, SourceAdapter, build_url


class DemoApiAdapter(SourceAdapter):
    """对接 ``GET /api/items?page=N&page_size=M`` 与 ``GET /api/items/{id}``。"""

    def list_url(self, page: int) -> str:
        return build_url(self.source.base_url, self.source.list_path or "/api/items")

    def list_params(self, page: int) -> dict[str, object]:
        return {"page": page, "page_size": self.source.page_size}

    def detail_url(self, external_id: str) -> str:
        return self._id_url(external_id)

    def parse_list(self, result: FetchResult) -> list[ListItem]:
        payload = self._load_json(result, "list")
        if not isinstance(payload, dict):
            raise self._parse_error("list", "响应顶层不是 JSON 对象")
        items = payload.get("items")
        if not isinstance(items, list):
            raise self._parse_error("list", "响应缺少 items 列表")

        parsed: list[ListItem] = []
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise self._parse_error(f"list[{index}]", "列表条目不是 JSON 对象")
            raw_id = item.get("id")
            if raw_id is None or str(raw_id).strip() == "":
                raise self._parse_error(f"list[{index}]", "列表条目缺少 id")
            external_id = str(raw_id).strip()
            parsed.append(ListItem(external_id=external_id, url=self.detail_url(external_id)))
        return parsed

    def parse_detail(self, result: FetchResult, external_id: str) -> dict[str, Any]:
        payload = self._load_json(result, external_id)
        if not isinstance(payload, dict):
            raise self._parse_error(external_id, "详情响应不是 JSON 对象")
        raw_id = payload.get("id")
        if raw_id is not None and str(raw_id).strip() != external_id:
            raise self._parse_error(
                external_id,
                f"详情 id 与列表不一致: {raw_id}",
            )
        title = payload.get("title")
        if not isinstance(title, str) or not title.strip():
            raise self._parse_error(external_id, "详情缺少 title")
        return payload

    def _load_json(self, result: FetchResult, external_id: str) -> Any:
        try:
            return json.loads(result.body)
        except json.JSONDecodeError as exc:
            raise self._parse_error(external_id, f"JSON 解析失败: {exc.msg}") from exc
