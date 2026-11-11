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

另开终端 B 跑采集、内容去重与统计：

```bash
crawl-flow run --source demo_api --limit 5
crawl-flow run --source demo_html --limit 5
crawl-flow dedup run          # 生成内容指纹 + 标记同源/跨源重复内容
crawl-flow dedup stats        # 指纹与重复标记统计
crawl-flow dedup groups       # 查看重复组及关联的来源与条目标识
crawl-flow db stats
```

同一源重复执行不会增加 `records` 行数，只会刷新 `updated_at`。

## 内容指纹与去重

独立模块 `crawl_flow.dedup`，**只读**采集结果、只写自有标记表，不修改
`records` / `raw_records`，也不删除任何原始数据。核心能力：

- **精确指纹**：可配置归一化（NFKC、大小写、空白、标点）后计算哈希，
  完全相同的内容必然得到相同指纹；同源、跨源的完全重复都能识别。
- **相似指纹**：SimHash + 中文按字符 n-gram（纯标准库、无预训练模型），
  相似度阈值可配置；达到阈值标记为相似，低于阈值不误判。
- **只标记不删除**：重复组与成员标记写入 `dedup_groups` /
  `dedup_group_members`，标记所有关联的来源与 `external_id`。
- **持久化 + 可重算**：指纹落库，任务重启无需重算；规则（`config/dedup.yaml`）
  任意变化会改变规则版本并触发历史条目批量重算，也可 `--force` 全量重算。
- **跳过无效内容**：正文为空或短于 `min_content_length` 的记录不生成指纹。

配置见 [config/dedup.yaml](config/dedup.yaml)，详细设计见
[docs/项目说明.md](docs/项目说明.md)。

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
crawl-flow dedup run --force           # 规则更新后强制重算全部历史指纹
crawl-flow db stats --db data/crawl.db
```

## 目录速览

```
crawl-flow/
├── config/sources.yaml      # 源定义（demo_api / demo_html 等）
├── config/dedup.yaml        # 内容指纹与去重规则（可配置）
├── src/crawl_flow/          # 管道实现（CLI、配置、适配器、存储、去重）
├── mock_site/               # 本地 mock 站点与 fixtures
├── tests/                   # pytest 测试（自带随机端口 mock 服务）
└── docs/项目说明.md         # 详细设计、数据模型与测试说明
```

完整结构、F1–F8 功能拆解、数据表字段与关键流程见 [docs/项目说明.md](docs/项目说明.md)。

## 已知限制

以下能力**当前不实现**，属于后续迭代范围：

1. **增量游标 / 断点续采**：每次从列表第 1 页全量重扫，没有游标与断点。
2. **并发抓取 / 全局限速 / 代理池**：单线程串行请求，无速率控制、无代理。
3. **跨语言/语义级相似**：相似指纹基于字符 n-gram，未引入语义向量模型；改写幅度较大但语义相同的内容可能落在阈值之外。
4. **重试退避 / 死信队列 / 失败重放**：仅对连接级传输异常做一次立即重试，5xx 不重试，失败只留档不自动重放。
5. **清洗规则 DSL / 字段质量校验**：清洗逻辑固定在代码中，不做可配置 DSL 与质量评分。
6. **任务编排 / 定时调度 / 指标暴露**：只能手动执行 CLI，无调度器与监控指标。
7. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。
