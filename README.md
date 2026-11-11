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

## 适配器自动生成与适配校验（autogen）

只需要**入口地址 + 必填字段**，即可自动爬取样本、识别分页/列表/详情结构，
生成可直接被本管道加载的声明式适配器（`auto_html` 类型，无需改核心代码），
并支持定期校验、漂移告警与可落地的调整建议。

```bash
# 起公开静态博客 mock（v1 正常结构；--variant v2 可模拟站点改版）
crawl-flow autogen serve-blog --port 8020

# 自动生成适配器（默认每 2 秒至多 1 次请求；动态渲染默认关闭）
crawl-flow autogen generate http://127.0.0.1:8020/posts \
  --field title --field content --field published_at --field category --field tags \
  --output config/generated --sample-root data/autogen_samples

# 把生成的源接入管道即可直接采集（输出与手写适配器完全一致）
crawl-flow run --source 127_0_0_1_8020 --config config/generated/127_0_0_1_8020.source.yaml

# 用最新站点样本校验适配器，结构漂移时给出新旧选择器对照建议
crawl-flow autogen validate config/generated/127_0_0_1_8020.spec.json

# 周期校验（默认 1 小时；--once 可交给外部 cron / CronJob 接管）
crawl-flow autogen validate-schedule config/generated/127_0_0_1_8020.spec.json --once
```

内置约束：请求间隔默认 ≥ 2s、样本仅保留最近 3 版且总量 ≤ 100MB（超出清理最早版本）、
登录/验证码站点明确拒绝、动态渲染需显式 `--enable-dynamic-rendering`、
恶意样本经沙箱拦截且运行期不执行任何站点脚本。设计细节见
[docs/autogen模块设计.md](docs/autogen模块设计.md)。

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
