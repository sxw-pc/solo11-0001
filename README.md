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

## 内容指纹与去重

采集入库只保证 `(source, external_id)` 唯一；不同来源、不同 ID 的相同内容会重复存储。
独立的 `dedup` 子包提供**内容级**去重能力，与采集流程解耦：只读主库 `records`，
指纹与去重标记写入独立的 `data/fingerprints.db`，**只标记不删除**任何采集数据。

```bash
# 计算指纹并识别完全重复 / 近似重复（增量，已存在且未变化的自动跳过）
crawl-flow dedup compute
# 查看指纹与去重簇统计
crawl-flow dedup stats
# 列出每个重复/相似簇及全部关联来源与条目标识（含跨源）
crawl-flow dedup groups
# 反查单条记录命中的簇
crawl-flow dedup show --source partner_a --external-id pa-100
# 规则更新（递增 config/dedup.yaml 的 rule.version）后批量重算历史条目
crawl-flow dedup recompute
```

能力要点：

- **精确指纹**：归一化（NFKC、大小写、空白、标点均可配置）后做 SHA-256，
  完全相同的正文指纹完全一致，完全重复识别准确率 100%，同源/跨源均可识别。
- **近似指纹**：中文友好的字符 unigram+bigram 分词 + 64 位 SimHash + 汉明距离，
  相似度阈值可配置（默认 0.9），达到阈值才标记、低于阈值绝不误判；纯标准库、无预训练模型。
- **空/短内容跳过**：正文归一化后短于 `min_content_length`（默认 20）自动跳过，不生成无效指纹。
- **持久化与版本化**：指纹落独立 SQLite，重启无需重算；规则带版本号，更新后可批量重算。

详见 [docs/项目说明.md](docs/项目说明.md) 的「F9 内容指纹与去重」章节。

## 目录速览

```
crawl-flow/
├── config/sources.yaml      # 源定义（demo_api / demo_html 等）
├── config/dedup.yaml        # 指纹规则与相似度阈值配置（独立、可选）
├── src/crawl_flow/          # 管道实现（CLI、配置、适配器、存储、dedup 去重子包）
├── mock_site/               # 本地 mock 站点与 fixtures
├── tests/                   # pytest 测试（自带随机端口 mock 服务）
└── docs/项目说明.md         # 详细设计、数据模型与测试说明
```

完整结构、F1–F9 功能拆解、数据表字段与关键流程见 [docs/项目说明.md](docs/项目说明.md)。

## 已知限制

以下能力**当前不实现**，属于后续迭代范围：

1. **增量游标 / 断点续采**：每次从列表第 1 页全量重扫，没有游标与断点。
2. **并发抓取 / 全局限速 / 代理池**：单线程串行请求，无速率控制、无代理。
3. **重试退避 / 死信队列 / 失败重放**：仅对连接级传输异常做一次立即重试，5xx 不重试，失败只留档不自动重放。
4. **清洗规则 DSL / 字段质量校验**：清洗逻辑固定在代码中，不做可配置 DSL 与质量评分。
5. **任务编排 / 定时调度 / 指标暴露**：只能手动执行 CLI，无调度器与监控指标。
6. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。
