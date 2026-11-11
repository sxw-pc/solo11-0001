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

## 定时调度与指标

为源配置标准 cron 表达式后，即可用内置调度器长期周期采集，无需外部调度系统：

```yaml
# config/sources.yaml
sources:
  - name: demo_api
    type: api
    base_url: http://127.0.0.1:8010
    schedule: "*/5 * * * *"   # 标准 5 段 cron（分 时 日 月 周，本地时间）
```

```bash
crawl-flow schedule start            # 前台常驻：到点自动采集 + 暴露 /metrics（仅 127.0.0.1:9108）
crawl-flow schedule list             # 查看各源定时计划与下次触发时间
crawl-flow schedule history          # 执行历史、成功率、平均耗时
crawl-flow metrics                   # 本地打印 Prometheus 文本指标
```

- 调度在单进程内运行，触发误差 < 1 秒；同一源上一次未跑完时本次自动跳过。
- 手动 `run` 与定时任务共用同一份指标（SQLite `job_runs` 表），统计互不冲突。
- `/metrics` 为 Prometheus 文本格式，可被主流监控系统直接拉取；只监听本机回环，不输出任何配置信息。
- 定时计划写在配置文件中，进程重启后自动恢复。

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
crawl-flow schedule start --metrics-port 9108  # 自定义指标端口
```

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
6. **分布式调度 / 跨进程任务编排**：调度器为单进程设计，多实例同时运行会重复采集；停机期间错过的轮次不补跑。
7. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。
