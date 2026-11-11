# crawl-flow

配置驱动的公开数据采集管道：**列表翻页 → 详情抓取 → 解析 → 归一化 → 原始响应留档 → SQLite 入库**。
面向小团队做公开数据长期采集的底座，自带本地 mock 站点，断网也能跑通全流程。

## 定位

- 是：可配置的多源采集管道、适配器可插拔、原始响应可追溯、结构化记录幂等入库。
- 不是：Web 前端、CRUD 后台、报表看板、通用爬虫框架、分布式任务平台。

## 快速上手

要求 Python 3.11+。以下命令均在仓库根目录执行。

```bash
python -m venv .venv
# Windows PowerShell: .\.venv\Scripts\Activate.ps1
# macOS / Linux:    source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

启动 mock 站点（终端 A，保持运行）：

```bash
crawl-flow serve-mock --port 8010
```

另开终端 B 跑采集与统计：

```bash
crawl-flow run --source demo_api --limit 5
crawl-flow run --source demo_html --limit 5
crawl-flow db stats
```

同一源重复执行不会增加 `records` 行数，只会刷新 `updated_at`。

## 三条核心命令

| 用途 | 命令 |
| --- | --- |
| 起 mock 站点 | `crawl-flow serve-mock --port 8010` |
| 跑一次采集 | `crawl-flow run --source demo_api --limit 5` / `crawl-flow run --all` |
| 检索原始响应 | `crawl-flow raw search --status failed --error-keyword "HTTP 500"` |
| 导出原始响应 | `crawl-flow raw export -o raw.json --source demo_api`（加 `--with-content` 才含完整正文） |
| 跑测试 | `pytest -q` |

其他可用命令与参数：

```bash
crawl-flow run --all --dry-run          # 解析但不写 raw 与数据库
crawl-flow run --source demo_api_flaky # 详情一律 500 的演练源
crawl-flow db stats --db data/crawl.db
crawl-flow raw search --source demo_api --from 2026-11-01 --to 2026-11-12
crawl-flow raw search --body-keyword 数据库 --max-scan 1000  # 按需读 raw 文件全文匹配
crawl-flow raw export -o raw.json --with-content             # 默认仅导出元信息
```

`raw search` 支持按 `--source` / `--external-id` / `--status` / `--error-keyword` /
`--from`/`--to` 过滤，默认只查 `raw_records` 索引、最多返回 100 条；只有加
`--body-keyword` 才按需加载 raw 文件做正文检索，不做目录全量扫描。损坏、缺失或
无权限的 raw 文件只产生告警，不会中断检索。

## 目录速览

```
crawl-flow/
├── config/sources.yaml      # 源定义（demo_api / demo_html 等）
├── src/crawl_flow/          # 管道实现（CLI、配置、适配器、存储）
├── mock_site/               # 本地 mock 站点与 fixtures
├── tests/                   # pytest 测试（自带随机端口 mock 服务）
└── docs/项目说明.md         # 详细设计、数据模型与测试说明
```

完整结构、F1–F8 功能拆解、数据表字段与关键流程见 [docs/项目说明.md](docs/项目说明.md)。

## 已知限制

以下能力**当前不实现**，属于后续迭代范围：

1. **增量游标 / 断点续采**：每次从列表第 1 页全量重扫，没有游标与断点。
2. **并发抓取 / 全局限速 / 代理池**：单线程串行请求，无速率控制、无代理。
3. **内容指纹去重**：只保证 `(source, external_id)` 唯一，不做正文指纹。
4. **重试退避 / 死信队列 / 失败重放**：仅对连接级传输异常做一次立即重试，5xx 不重试，失败只留档不自动重放。
5. **清洗规则 DSL / 字段质量校验**：清洗逻辑固定在代码中，不做可配置 DSL 与质量评分。
6. **任务编排 / 定时调度 / 指标暴露**：只能手动执行 CLI，无调度器与监控指标。
7. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。
