"""本地 mock 站点：提供 demo_api / demo_html 所需的 HTTP 接口。

默认只监听本地，不访问公网。支持 ``?fail=1``（详情 500，列表 503）、
``?empty=1``（空页）与任意 ``page`` / ``page_size`` 参数。
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, request

DEFAULT_FIXTURES = Path(__file__).resolve().parent / "fixtures"
HTML_CONTENT_TYPE = "text/html; charset=utf-8"
HTML_ID_PATTERN = re.compile(r"html-(\d+)")
EMPTY_LIST_HTML = """<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>公开数据列表（空）</title></head>
<body>
<table class="items">
  <thead><tr><th>标题</th><th>分类</th><th>标签</th><th>发布日期</th><th>详情</th></tr></thead>
  <tbody></tbody>
</table>
</body>
</html>
"""
INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>crawl-flow mock site</title></head>
<body>
<h1>crawl-flow mock site</h1>
<ul>
  <li><a href="/api/items?page=1&amp;page_size=5">API 列表</a></li>
  <li><a href="/api/items/api-1">API 详情示例</a></li>
  <li><a href="/list?page=1">HTML 列表</a></li>
  <li><a href="/item/html-1">HTML 详情示例</a></li>
</ul>
</body>
</html>
"""


def create_app(fixtures_dir: str | Path | None = None) -> Flask:
    """创建 Flask 应用；``fixtures_dir`` 便于测试指向仓库 fixtures。"""

    fixtures = Path(fixtures_dir) if fixtures_dir is not None else DEFAULT_FIXTURES
    api_items = _load_api_items(fixtures / "api")
    api_by_id = {str(item["id"]): item for item in api_items}

    app = Flask(__name__)
    app.json.ensure_ascii = False

    @app.get("/")
    def index() -> Response:
        return Response(INDEX_HTML, content_type=HTML_CONTENT_TYPE)

    @app.get("/api/items")
    def api_list() -> Any:
        if _flag("fail"):
            return jsonify({"error": "simulated list failure"}), 503
        page = _positive_int("page", 1)
        page_size = _positive_int("page_size", 5, maximum=100)
        page_items: list[dict[str, Any]] = []
        if not _flag("empty") and page >= 1:
            start = (page - 1) * page_size
            page_items = api_items[start : start + page_size]
        return jsonify(
            {
                "total": len(api_items),
                "page": page,
                "page_size": page_size,
                "items": page_items,
            }
        )

    @app.get("/api/items/<item_id>")
    def api_detail(item_id: str) -> Any:
        if _flag("fail"):
            return jsonify({"error": "simulated detail failure"}), 500
        item = api_by_id.get(item_id)
        if item is None:
            return jsonify({"error": "not found", "id": item_id}), 404
        return jsonify(item)

    @app.get("/list")
    def html_list() -> Response | tuple[str, int]:
        if _flag("fail"):
            return "simulated list failure", 503
        if _flag("empty"):
            return Response(EMPTY_LIST_HTML, content_type=HTML_CONTENT_TYPE)
        page = _positive_int("page", 1)
        path = fixtures / "html" / f"list_{page}.html"
        if not path.is_file():
            return Response(EMPTY_LIST_HTML, content_type=HTML_CONTENT_TYPE)
        return Response(path.read_text(encoding="utf-8"), content_type=HTML_CONTENT_TYPE)

    @app.get("/item/<item_id>")
    def html_detail(item_id: str) -> Response | tuple[str, int]:
        if _flag("fail"):
            return "<h1>模拟故障</h1>", 500
        match = HTML_ID_PATTERN.fullmatch(item_id)
        if match is None:
            return "not found", 404
        path = fixtures / "html" / f"item_{match.group(1)}.html"
        if not path.is_file():
            return "not found", 404
        return Response(path.read_text(encoding="utf-8"), content_type=HTML_CONTENT_TYPE)

    return app


def _load_api_items(directory: Path) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for path in sorted(directory.glob("page_*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        page_items = payload.get("items", [])
        if isinstance(page_items, list):
            items.extend(item for item in page_items if isinstance(item, dict))
    return items


def _flag(name: str) -> bool:
    return request.args.get(name, "").lower() in {"1", "true", "yes"}


def _positive_int(name: str, default: int, *, maximum: int | None = None) -> int:
    try:
        value = int(request.args.get(name, default))
    except (TypeError, ValueError):
        return default
    if value < 1:
        return default
    if maximum is not None and value > maximum:
        return maximum
    return value
