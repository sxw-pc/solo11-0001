# crawl-flow

配置驱动的公开数据采集管道：**列表翻页 → 详情抓取 → 解析 → 规则清洗/质量校验 → 原始响应留档 → SQLite 入库**。
面向小团队做公开数据长期采集的底座，自带本地 mock 站点，断网也能跑通全流程。

## 定位

- 是：可配置的多源采集管道、适配器可插拔、原始响应可追溯、结构化记录幂等入库。
- 是：配置驱动的字段规则引擎——清洗规则、质量校验、质量评分均可按源/按字段配置，无需改代码。
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
| 启动前校验规则配置 | `crawl-flow rules check --config config/rules.yaml` |
| 跑测试 | `pytest -q` |

其他可用命令与参数：

```bash
crawl-flow run --all --dry-run          # 解析但不写 raw 与数据库
crawl-flow run --source demo_api_flaky # 详情一律 500 的演练源
crawl-flow run --source demo_api --rules other-rules.yaml   # 指定规则配置
crawl-flow db stats --db data/crawl.db
```

## 规则引擎（配置驱动，无需改代码）

字段清洗与质量校验在 `config/rules.yaml` 中按「源 → 字段 → 规则链」配置，规则按列表顺序执行：

```yaml
sources:
  demo_api:
    threshold: 60                 # 低于该质量分视为低质量
    on_quality_failure: mark      # skip 跳过 / mark 标记 / warn 仅告警
    fields:
      title:
        rules:
          - name: strip
          - name: truncate
            params: {max_length: 100}
          - name: required
            weight: 50
            on_failure: skip
```

- 内置 12 种算子：`strip`、`truncate`、`regex_replace`、`date_format`、`default`、`type_cast`、`map_values`、`required`、`enum`、`length`、`pattern`、`range`。
- 质量分 = `100 - 命中规则扣分之和`；评分、低质量标记随记录落库，规则执行前后值写入 DEBUG 日志（`--verbose`）。
- 规则配置在启动时校验（YAML 语法、算子名、参数、正则、日期格式）；错误时输出明确提示并**自动降级为默认清洗逻辑**，采集任务不中断。
- 不引入任何第三方规则引擎依赖，规则处理为微秒级，远低于单条采集耗时的 10%。

## 目录速览

```
crawl-flow/
├── config/
│   ├── sources.yaml         # 源定义（demo_api / demo_html 等）
│   └── rules.yaml           # 字段清洗与质量校验规则
├── src/crawl_flow/          # 管道实现（CLI、配置、适配器、规则引擎、存储）
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
5. **任务编排 / 定时调度 / 指标暴露**：只能手动执行 CLI，无调度器与监控指标。
6. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。

规则引擎相关边界：规则只作用于适配器解析出的源字段（入库前）；未命中规则按权重扣分，不改变字段本身的入库与否，除非触发质量闸门（`skip`）。
