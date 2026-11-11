"""生成 html_alt 测试 fixture：与现有 html fixture 结构完全不同的站点。

列表仍遵循 table.items / a[href] 接入约定，但详情页 DOM 与默认选择器
完全不重叠，用来验证「只改 selectors 配置即可接入新 HTML 站点」。
生成后可直接提交，脚本可重复执行。
"""

from __future__ import annotations

from pathlib import Path

CATEGORIES = ["科研", "民生", "教育", "地理", "能源"]
TAGS = ["观察", "数据", "专题", "记录", "调研"]
MONTH_DAY = [(3, d) for d in range(1, 16)]

OUT = Path(__file__).resolve().parents[1] / "mock_site" / "fixtures" / "html_alt"

LIST_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf"><title>城市观察门户 - 第 {page} 页</title></head>
<body>
  <section class="portal-list">
    <h2>城市观察门户</h2>
    <table class="items">
      <tbody>
{rows}
      </tbody>
    </table>
  </section>
</body>
</html>
"""

ROW_TEMPLATE = """        <tr>
          <td><a href="/portal/article/alt-{idx}">城市观察第 {idx} 期</a></td>
        </tr>"""

DETAIL_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head><meta charset="utf"><title>城市观察第 {idx} 期</title></head>
<body>
  <main id="article-root">
    <header class="article-head">
      <h2 class="headline">城市观察第 {idx} 期：{category}领域月度记录</h2>
      <div class="article-meta">
        <em class="channel">{category}</em>
        <time class="publish-time">2024-{month:02d}-{day:02d} 08:00</time>
      </div>
    </header>
    <section class="keyword-box">
      <span class="keyword-label">关键词</span>
      <ul class="keyword-list">
        <li>{tag_a}</li>
        <li>{tag_b}</li>
        <li>{tag_c}</li>
      </ul>
    </section>
    <article class="article-body">
      <p>这是第 {idx} 期城市观察正文，主题方向为{category}。</p>
      <p>本期记录三个关键词：{tag_a}、{tag_b}、{tag_c}，内容与默认 fixture 无重叠。</p>
      <p>发布时间为 2024 年 {month} 月 {day} 日上午八点。</p>
    </article>
  </main>
</body>
</html>
"""


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    total = 15
    per_page = 5
    pages = total // per_page
    for page in range(1, pages + 1):
        start = (page - 1) * per_page + 1
        rows = "\n".join(
            ROW_TEMPLATE.format(idx=idx) for idx in range(start, start + per_page)
        )
        (OUT / f"list_{page}.html").write_text(
            LIST_TEMPLATE.format(page=page, rows=rows), encoding="utf-8"
        )

    for idx in range(1, total + 1):
        month, day = MONTH_DAY[idx - 1]
        category = CATEGORIES[(idx - 1) % len(CATEGORIES)]
        tag_a = TAGS[(idx - 1) % len(TAGS)]
        tag_b = TAGS[idx % len(TAGS)]
        tag_c = TAGS[(idx + 1) % len(TAGS)]
        (OUT / f"item_{idx}.html").write_text(
            DETAIL_TEMPLATE.format(
                idx=idx,
                month=month,
                day=day,
                category=category,
                tag_a=tag_a,
                tag_b=tag_b,
                tag_c=tag_c,
            ),
            encoding="utf-8",
        )
    print(f"written {pages} list pages and {total} detail pages to {OUT}")


if __name__ == "__main__":
    main()
