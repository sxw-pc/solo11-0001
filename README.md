# crawl-flow

配置驱动的公开数据采集管道：**列表翻页 → 详情抓取 → 解析 → 归一化 → 规则引擎清洗/质量评分 → 原始响应留档 → SQLite 入库**。
面向小团队做公开数据长期采集的底座，自带本地 mock 站点，断网也能跑通全流程。

## 定位

- 是：可配置的多源采集管道、适配器可插拔、字段清洗/校验规则可配置、原始响应可追溯、结构化记录幂等入库。
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
crawl-flow rules check                 # 启动时校验 config/rules.yaml，不跑采集
crawl-flow run --source demo_api --rules config/rules.yaml  # 指定规则文件（默认即该路径）
crawl-flow db stats --db data/crawl.db
```

## 规则引擎（字段清洗 + 质量评分）

`config/rules.yaml` 用配置定义字段清洗与质量校验规则，新增来源无需改代码：

- **15 种内置算子**：`strip` / `truncate`（截断）/ `regex_replace`（正则替换）/ `date_format`（日期格式转换）/ `case` / `default` / `type_cast` / `map_values` / `html_strip` 清洗算子，`required` / `enum`（枚举校验）/ `length` / `pattern` / `range` / `date_parseable` 校验算子。
- **质量评分**：按规则命中情况自动计算（0–100），低于 `quality_threshold` 的条目按 `on_failure` 处置：`skip`（丢弃）/ `mark`（保留并打 `low_quality` 标记）/ `alert`（保留仅告警）。
- **按字段、按来源独立配置**：`defaults` 全局生效，源级同名字段整体覆盖；`order` 控制执行顺序。
- **异常降级**：规则配置错误或算子运行异常都不影响采集主流程，自动降级为默认清洗逻辑。
- **可追溯**：每条规则的执行轨迹（字段处理前后值）写入日志与质量报告，质量分与标记落 `records.quality_score` / `quality_flags`。
- **启动时校验**：`crawl-flow rules check` 在跑采集前检查全部语法、参数与正则。

配置示例见 `config/rules.yaml`，完整说明见 [docs/项目说明.md](docs/项目说明.md)。

## 目录速览

```
crawl-flow/
├── config/sources.yaml      # 源定义（demo_api / demo_html 等）
├── config/rules.yaml        # 字段清洗与质量校验规则（规则引擎）
├── src/crawl_flow/          # 管道实现（CLI、配置、适配器、规则引擎、存储）
├── mock_site/               # 本地 mock 站点与 fixtures
├── tests/                   # pytest 测试（自带随机端口 mock 服务）
└── docs/项目说明.md         # 详细设计、数据模型与测试说明
```

完整结构、F1–F9 功能拆解、数据表字段与关键流程见 [docs/项目说明.md](docs/项目说明.md)。

## 已知限制

以下能力**当前不实现**，属于后续迭代范围：

1. **增量游标 / 断点续采**：每次从列表第 1 页全量重扫，没有游标与断点。
2. **并发抓取 / 全局限速 / 代理池**：单线程串行请求，无速率控制、无代理。
3. **内容指纹去重**：只保证 `(source, external_id)` 唯一，不做正文指纹。
4. **重试退避 / 死信队列 / 失败重放**：仅对连接级传输异常做一次立即重试，5xx 不重试，失败只留档不自动重放。
5. **任务编排 / 定时调度 / 指标暴露**：只能手动执行 CLI，无调度器与监控指标。
6. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。
