"""本地 mock 站点：提供 demo_api / demo_html 所需的 HTTP 接口。

默认只监听本地，不访问公网。支持 ``?fail=1``（详情 500，列表 503）、
``?empty=1``（空页）与任意 ``page`` / ``page_size`` 参数。

另提供 ``/legacy`` 演练接口，用真实字节返回 GBK / GB2312 / UTF-8 BOM /
响应头声明错误 / 未闭合标签 / 缺少必填选择器等页面，用于验证解码链与
畸形 HTML 容错。
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
  <li><a href="/legacy/list?page=1">多编码 / 畸形页面演练</a></li>
</ul>
</body>
</html>
"""

LEGACY_LIST_HTML = """<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf-8"><title>多编码演练列表</title></head>
<body>
<table class="items">
  <thead><tr><th>页面类型</th><th>详情</th></tr></thead>
  <tbody>
    <tr><td>GBK（meta 声明）</td><td><a href="/legacy/item/legacy-gbk">查看详情</a></td></tr>
    <tr><td>GB2312（含 GBK 生僻字）</td><td><a href="/legacy/item/legacy-gb2312">查看详情</a></td></tr>
    <tr><td>UTF-8 带 BOM</td><td><a href="/legacy/item/legacy-utf8-bom">查看详情</a></td></tr>
    <tr><td>响应头 charset 声明错误</td><td><a href="/legacy/item/legacy-bad-header">查看详情</a></td></tr>
    <tr><td>未闭合标签（非致命）</td><td><a href="/legacy/item/legacy-malformed">查看详情</a></td></tr>
    <tr><td>缺少 div.content（必填选择器缺失）</td><td><a href="/legacy/item/legacy-no-content">查看详情</a></td></tr>
    <tr><td>完全不可解析</td><td><a href="/legacy/item/legacy-broken">查看详情</a></td></tr>
  </tbody>
</table>
</body>
</html>
"""

LEGACY_SHARED_TITLE = "社区卫生服务站运营月报"


def _legacy_page(
    *,
    title: str,
    meta_charset: str | None,
    content_html: str | None,
    category: str = "公告",
) -> str:
    """拼一张符合演示选择器的详情页；``content_html=None`` 时不渲染正文。"""

    meta = f'<meta charset="{meta_charset}">' if meta_charset else ""
    content = (
        f'    <div class="content">{content_html}</div>\n' if content_html is not None else ""
    )
    return (
        "<!doctype html>\n"
        '<html lang="zh-CN">\n'
        f"<head>{meta}<title>{title}</title></head>\n"
        "<body>\n"
        '  <article class="item">\n'
        f'    <h1 class="title">{title}</h1>\n'
        '    <p class="meta">\n'
        f'      <span class="category">{category}</span>\n'
        '      <span class="date">2024-04-01</span>\n'
        "    </p>\n"
        f"{content}"
        "  </article>\n"
        "</body>\n</html>\n"
    )


def _legacy_variants() -> dict[str, tuple[bytes, str]]:
    """演练页面：返回 (原始字节, Content-Type) 二元组。"""

    shared_content = "<p>本月门诊量与家庭医生签约情况汇总。</p>"
    gbk_page = _legacy_page(
        title=LEGACY_SHARED_TITLE,
        meta_charset="gbk",
        content_html=shared_content,
    ).encode("gbk")
    gb2312_page = _legacy_page(
        title="老旧厂区安全巡查记录",
        meta_charset="gb2312",
        # 「喆」只在 GBK 中有编码，用超集 GBK 解码才不会失败。
        content_html="<p>巡查负责人：张喆。本周未发现新增隐患。</p>",
        category="安全",
    ).encode("gbk")
    bom_page = _legacy_page(
        title=LEGACY_SHARED_TITLE,
        meta_charset="utf-8",
        content_html=shared_content,
    ).encode("utf-8-sig")
    bad_header_page = _legacy_page(
        title="秋季道路养护计划",
        meta_charset="gbk",
        content_html="<p>重点路段完成坑槽修补与标线复划。</p>",
        category="交通",
    ).encode("gbk")
    malformed_page = (
        "<!doctype html>\n"
        '<html lang="zh-CN">\n'
        '<head><meta charset="utf-8"><title>河道水质周报</title></head>\n'
        "<body>\n"
        '  <article class="item">\n'
        '    <h1 class="title">河道水质周报</h1>\n'
        '    <p class="meta"><span class="category">环境</span>\n'
        '    <div class="content">\n'
        "      <p>溶解氧与氨氮指标均达标。\n"
        "      <p>下游断面浊度较上周下降。\n"
    ).encode("utf-8")
    no_content_page = _legacy_page(
        title="临时通知",
        meta_charset="utf-8",
        content_html=None,
    ).encode("utf-8")
    broken_page = (
        "<!doctype html>\n"
        "<html>\n<body><p>页面不存在或已下线</p></body>\n</html>\n"
    ).encode("utf-8")

    return {
        "legacy-gbk": (gbk_page, "text/html"),
        "legacy-gb2312": (gb2312_page, "text/html"),
        "legacy-utf8-bom": (bom_page, "text/html"),
        # 声明 utf-8，实际是 GBK 字节：解码链应回退到 meta 的 gbk。
        "legacy-bad-header": (bad_header_page, "text/html; charset=utf-8"),
        "legacy-malformed": (malformed_page, "text/html; charset=utf-8"),
        "legacy-no-content": (no_content_page, "text/html; charset=utf-8"),
        "legacy-broken": (broken_page, "text/html; charset=utf-8"),
    }


LEGACY_VARIANTS = _legacy_variants()


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

    @app.get("/legacy/list")
    def legacy_list() -> Response:
        page = _positive_int("page", 1)
        if page >= 2:
            return Response(EMPTY_LIST_HTML, content_type=HTML_CONTENT_TYPE)
        return Response(LEGACY_LIST_HTML, content_type=HTML_CONTENT_TYPE)

    @app.get("/legacy/item/<item_id>")
    def legacy_detail(item_id: str) -> Response | tuple[str, int]:
        variant = LEGACY_VARIANTS.get(item_id)
        if variant is None:
            return "not found", 404
        content, content_type = variant
        return Response(content, content_type=content_type)

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
