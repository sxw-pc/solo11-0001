"""本地 mock 站点：提供 demo_api / demo_html 所需的 HTTP 接口。

默认只监听本地，不访问公网。支持 ``?fail=1``（详情 500，列表 503）、
``?empty=1``（空页）与任意 ``page`` / ``page_size`` 参数。
``/lab/*`` 是编码与容错实验区：多编码、声明错误、畸形 HTML、缺字段与
完全不可解析的详情页，供端到端验证解码链与容错策略。
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
  <li><a href="/lab/list">编码与容错实验区</a></li>
</ul>
</body>
</html>
"""

# ---- 编码与容错实验区（/lab/*）----
# 一组覆盖多编码、声明错误、畸形 HTML、缺字段与不可解析的详情页，
# 列表页 /lab/list 汇总所有用例，供端到端验证解码链与容错策略。

LAB_TITLE = "城市内涝监测周报"
LAB_CATEGORY = "环境"
LAB_TAGS = "内涝、监测、周报"
LAB_DATE = "2024-03-06"
# 正文含 GBK 扩展字符「〇」（不在 GB2312 内），用于验证 gb2312 → gbk 归一。
LAB_CONTENT_P1 = "二〇二四年六月强降雨期间，城区十二处易涝点完成首轮巡查。"
LAB_CONTENT_P2 = "监测数据按小时汇总，包含积水深度、排涝时长与交通影响。"

UTF8_BOM = b"\xef\xbb\xbf"


def _lab_detail_html(meta_charset: str | None = "utf-8") -> str:
    meta = f'<meta charset="{meta_charset}">' if meta_charset else ""
    return f"""<!doctype html>
<html lang="zh-CN">
<head>{meta}<title>{LAB_TITLE}</title></head>
<body>
  <article class="item">
    <h1 class="title">{LAB_TITLE}</h1>
    <p class="meta">
      <span class="category">{LAB_CATEGORY}</span>
      <span class="date">{LAB_DATE}</span>
      <span class="tags">{LAB_TAGS}</span>
    </p>
    <div class="content">
      <p>{LAB_CONTENT_P1}</p>
      <p>{LAB_CONTENT_P2}</p>
    </div>
  </article>
</body>
</html>
"""


# 非致命语法错误：h1、p、div 等标签未闭合，但标题与正文都能提取。
LAB_MALFORMED_HTML = f"""<html><body>
<article class="item">
<h1 class="title">{LAB_TITLE}
<p class="meta">
<span class="category">{LAB_CATEGORY}</span>
<span class="date">{LAB_DATE}</span>
<span class="tags">{LAB_TAGS}</span>
<div class="content">
<p>{LAB_CONTENT_P1}
<p>{LAB_CONTENT_P2}
"""

# 只有必填字段（h1.title + div.content），可选字段全部缺失。
LAB_MINIMAL_HTML = f"""<!doctype html>
<html><body>
<h1 class="title">{LAB_TITLE}</h1>
<div class="content"><p>{LAB_CONTENT_P1}</p></div>
</body></html>
"""

# 完全提不出必填字段：缺 h1.title / 缺 div.content。
LAB_NO_TITLE_HTML = f"""<!doctype html>
<html><body>
<p class="meta"><span class="category">{LAB_CATEGORY}</span></p>
<div class="content"><p>{LAB_CONTENT_P1}</p></div>
</body></html>
"""

LAB_NO_CONTENT_HTML = f"""<!doctype html>
<html><body>
<h1 class="title">{LAB_TITLE}</h1>
<p class="meta"><span class="category">{LAB_CATEGORY}</span></p>
</body></html>
"""

# 完全不可解析：确定性二进制内容，不是 HTML。
LAB_GARBAGE_BYTES = b"\x89PNG\r\n\x1a\n" + bytes(range(256))

# 用例名 → (Content-Type, 原始响应字节)。测试直接引用以核对留档字节。
LAB_CASES: dict[str, tuple[str, bytes]] = {
    # 响应头声明 gbk，正文 GBK 字节
    "gbk": ("text/html; charset=gbk", _lab_detail_html("gbk").encode("gbk")),
    # 响应头声明 gb2312，正文含 GBK 扩展字符（验证 gb2312 按 GBK 解码）
    "gb2312": (
        "text/html; charset=gb2312",
        _lab_detail_html("gb2312").encode("gbk"),
    ),
    # 带 BOM 的 UTF-8
    "utf8-bom": (
        "text/html; charset=utf-8",
        UTF8_BOM + _lab_detail_html("utf-8").encode("utf-8"),
    ),
    # 响应头声明错误（谎称 utf-8），页面 meta 才是真的 gbk
    "wrong-header": (
        "text/html; charset=utf-8",
        _lab_detail_html("gbk").encode("gbk"),
    ),
    # 响应头声明了未知编码名，回退到页面 meta
    "bad-charset": (
        "text/html; charset=x-bogus-enc",
        _lab_detail_html("gbk").encode("gbk"),
    ),
    # 响应头不带 charset，只有页面 meta（gb2312）
    "meta-only": ("text/html", _lab_detail_html("gb2312").encode("gbk")),
    # 响应头 gbk 与页面 meta（谎称 utf-8）冲突，响应头优先
    "header-wins": (
        "text/html; charset=gbk",
        _lab_detail_html("utf-8").encode("gbk"),
    ),
    # 未闭合标签的畸形页面，必填字段可提取
    "malformed": (
        "text/html; charset=utf-8",
        LAB_MALFORMED_HTML.encode("utf-8"),
    ),
    # 可选字段（分类/标签/日期）缺失，只有必填字段
    "minimal": ("text/html; charset=utf-8", LAB_MINIMAL_HTML.encode("utf-8")),
    # 完全不可解析的二进制内容
    "garbage": ("text/html", LAB_GARBAGE_BYTES),
    # 缺必填字段 h1.title
    "no-title": ("text/html; charset=utf-8", LAB_NO_TITLE_HTML.encode("utf-8")),
    # 缺必填字段 div.content
    "no-content": (
        "text/html; charset=utf-8",
        LAB_NO_CONTENT_HTML.encode("utf-8"),
    ),
}

LAB_LIST_HTML = (
    """<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>编码与容错实验区</title></head>
<body>
<table class="items">
  <thead><tr><th>用例</th><th>详情</th></tr></thead>
  <tbody>
"""
    + "".join(
        f'    <tr><td>{case}</td><td><a href="/lab/item/{case}">查看详情</a></td></tr>\n'
        for case in LAB_CASES
    )
    + """  </tbody>
</table>
</body>
</html>
"""
)

EMPTY_LAB_LIST_HTML = """<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>编码与容错实验区（空）</title></head>
<body>
<table class="items">
  <thead><tr><th>用例</th><th>详情</th></tr></thead>
  <tbody></tbody>
</table>
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

    @app.get("/lab/list")
    def lab_list() -> Response:
        page = _positive_int("page", 1)
        html = LAB_LIST_HTML if page == 1 else EMPTY_LAB_LIST_HTML
        return Response(html, content_type=HTML_CONTENT_TYPE)

    @app.get("/lab/item/<case_id>")
    def lab_item(case_id: str) -> Response | tuple[str, int]:
        case = LAB_CASES.get(case_id)
        if case is None:
            return "not found", 404
        content_type, body = case
        return Response(body, content_type=content_type)

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
