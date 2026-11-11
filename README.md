# crawl-flow

配置驱动的公开数据采集管道：**列表翻页 → 详情抓取 → 解析 → 归一化 → 关键词布控 → 原始响应留档 → SQLite 入库**。
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

## 关键词布控

每条内容完成归一化后、写入存储前会过一遍布控引擎：命中品牌词/敏感词就把源名、条目标识、命中规则、命中位置上下文写进独立的 `watch_hits` 表。同一条内容同一条规则重复采集只保留**首次命中**，重扫不产生重复行。

规则写在配置里，分全局规则（顶层 `watch.rules`，对所有源生效）和源级规则（`sources[].watch.rules`，只对该源生效），两者叠加：

```yaml
watch:
  enabled: true            # 总开关
  context_chars: 40        # 命中位置前后各截 40 个字符
  timeout_seconds: 1.0     # 单条正则硬超时，超时即熔断该规则
  rules:
    - name: brand_database       # 规则名全局唯一，查询时按它过滤
      type: word                 # 普通词，默认不区分大小写
      pattern: 数据库
      # fields: [title, content]  # 可选，默认扫标题+正文，另有 category/tags
    - name: metro_flow
      type: regex                # 正则，默认区分大小写
      pattern: 地铁.{0,6}客流
      ignore_case: false         # 可选，覆盖默认大小写策略
```

三条容错底线：

- **非法规则启动即拦下**：正则在配置加载阶段编译，写错直接报「配置错误」并退出码 2，不会跑到一半才暴露。
- **病态规则拖不死整轮**：正则执行有 `SIGALRM` 硬超时（CPython 的灾难性回溯持 GIL，线程超时无法打断，故用信号），超时一次该规则本轮熔断跳过。
- **布控异常不影响入库**：单条规则的任何异常只记日志并跳过，条目照常入库。

布控命中翻查命令：

```bash
crawl-flow watch hits --rule brand_database                 # 按规则
crawl-flow watch hits --source demo_api                     # 按源
crawl-flow watch hits --start 2026-11-01 --end 2026-11-12   # 按时间范围
crawl-flow watch hits --rule metro_flow --json              # JSON 输出，便于脚本消费
```

## 核心命令

| 用途 | 命令 |
| --- | --- |
| 起 mock 站点 | `crawl-flow serve-mock --port 8010` |
| 跑一次采集（自动过布控） | `crawl-flow run --source demo_api --limit 5` / `crawl-flow run --all` |
| 翻查布控命中 | `crawl-flow watch hits [--source S] [--rule R] [--start D] [--end D]` |
| 跑测试 | `pytest -q` |

其他可用命令与参数：

```bash
crawl-flow run --all --dry-run          # 解析但不写 raw 与数据库（含布控命中）
crawl-flow run --source demo_api_flaky # 详情一律 500 的演练源
crawl-flow db stats --db data/crawl.db
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
6. **任务编排 / 定时调度 / 指标暴露**：只能手动执行 CLI，无调度器与监控指标。
7. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。
