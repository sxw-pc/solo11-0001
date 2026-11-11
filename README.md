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
crawl-flow run --all --dry-run          # 解析但不写 raw 与数据库
crawl-flow run --source demo_api_flaky # 详情一律 500 的演练源
crawl-flow db stats --db data/crawl.db
```

## 并发调度与限流

默认仍是单线程串行（行为与历史版本完全一致）；加上 `--scheduled` 即启用
单进程内的请求调度与限流模块，**不改动适配器与管道核心逻辑**（通过与
`Fetcher` 同接口的代理可插拔接入）：

```bash
crawl-flow run --all --scheduled                 # 启用并发调度
crawl-flow run --source demo_api --scheduled --max-concurrency 4 --qps 10
```

能力一览：

- **两级并发上限**：全局 `max_concurrency` 与单源 `max_concurrency` 同时生效，
  实际在途请求数不会超过任一配置值。
- **两级 QPS 限流**：全局与单源 QPS 独立配置，毫秒精度滑动窗口，阈值误差 ≤5%。
- **加权优先级调度**：按源 `weight` 做亏空轮询（DRR），高权重源优先获得配额，
  长期配额比例与权重一致；内置老化机制，低权重源不会长期饥饿。
- **域名级限流**：对命中规则的域名单独限流，触发后请求自动排队等待，不返回失败。
- **错误隔离不变**：单条请求失败仍按原规则标记 `failed` 并继续，不影响调度池。
- **自动降级**：调度模块自身异常时自动回退为串行直连，正在运行的采集任务不中断。

调度规则写在 `config/scheduler.yaml`（可用 `--scheduler-config` 覆盖路径），
字段与设计细节见 [docs/项目说明.md](docs/项目说明.md) 的 F9 章节。

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
2. **代理池 / 分布式调度**：并发调度与全局限速已支持（`--scheduled`，单进程内
   完成，无分布式依赖），但不提供代理 IP 池与多机分布式任务分发。
3. **内容指纹去重**：只保证 `(source, external_id)` 唯一，不做正文指纹。
4. **重试退避 / 死信队列 / 失败重放**：仅对连接级传输异常做一次立即重试，5xx 不重试，失败只留档不自动重放。
5. **清洗规则 DSL / 字段质量校验**：清洗逻辑固定在代码中，不做可配置 DSL 与质量评分。
6. **任务编排 / 定时调度 / 指标暴露**：只能手动执行 CLI，无调度器与监控指标。
7. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。
