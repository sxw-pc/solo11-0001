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
| 跑测试 | `pytest -q` |

其他可用命令与参数：

```bash
crawl-flow run --all --dry-run          # 解析但不写 raw 与数据库（预算只估算不记账）
crawl-flow run --source demo_api_flaky # 详情一律 500 的演练源
crawl-flow db stats --db data/crawl.db
crawl-flow budget status                # 查看各源当日（UTC）已用/剩余请求与字节额度
```

## 请求预算管控

在源配置里声明两个**可选**的每日上限（不配 = 该维度完全不限制，行为不变）：

```yaml
sources:
  - name: demo_api
    # ...
    daily_request_limit: 8        # 每日请求次数上限（含列表/详情/重试/重定向）
    daily_bytes_limit: 2097152    # 每日响应字节上限（按实际接收的响应体字节计）
```

行为约定：

- **实时记账**：每个 HTTP 请求发出前在 SQLite 账本（`budget_usage` 独立新表）
  原子预留一次请求额度，每个响应回来后累加实际字节；按 UTC 自然日分区、跨天自动归零。
- **熔断**：任一额度触顶，该源在**下一次请求发出之前**停止抓取，本轮已完成条目
  正常入库，命令行打印熔断原因并以退出码 `3` 结束；同轮其他源不受影响。
  当天再次运行会立即被拒。
- **演练**：`--dry-run` 只在内存估算本次会消耗多少额度、是否会超限，不打开、
  不写入账本，退出码仍为 0。
- **并发安全**：账本使用 WAL + `BEGIN IMMEDIATE` 原子短事务，多个采集进程同时
  运行时计数不重不漏、不会超额。
- **损坏防护**：启动时执行 `integrity_check`、表结构比对与逐行数据校验，
  账本损坏时宁可拒绝启动（退出码 2）也不静默丢账。

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
2. **并发抓取 / 全局限速 / 代理池**：单线程串行请求，无速率控制（仅有按源的每日请求/字节预算熔断）、无代理。
3. **内容指纹去重**：只保证 `(source, external_id)` 唯一，不做正文指纹。
4. **重试退避 / 死信队列 / 失败重放**：仅对连接级传输异常做一次立即重试，5xx 不重试，失败只留档不自动重放。
5. **清洗规则 DSL / 字段质量校验**：清洗逻辑固定在代码中，不做可配置 DSL 与质量评分。
6. **任务编排 / 定时调度 / 指标暴露**：只能手动执行 CLI，无调度器与监控指标。
7. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。
