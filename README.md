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

## 并发运行互斥

同一数据库文件同一时刻只允许一个 `run` 写入进程，避免两个进程同时写
`data/crawl.db` 触发 `database is locked` 或互相覆盖 raw 文件：

```bash
# 默认：发现已有 run 在跑，立即报错并以退出码 3 退出（区别于配置/采集/存储错误的 2）
crawl-flow run --source demo_api

# 可选：排队等锁，前一个进程结束后自动开跑
crawl-flow run --source demo_api --wait-for-lock
# 只愿意等 60 秒，超时仍拿不到锁则退出码 3
crawl-flow run --source demo_api --wait-for-lock --lock-timeout 60
```

- 互斥基于操作系统级文件锁（POSIX `flock` / Windows `msvcrt`），不是
  「锁文件存在即占用」：进程被 `kill -9` 或机器重启后，内核自动释放锁，
  残留的 `*.run.lock` 文件不会挡住后续运行，无需手工清理。
- 锁按数据库文件区分：不同 `--db` 的运行互不影响。
- `--dry-run` 不写库也不写 raw，不申请锁：既不会被其他运行挡住，也不会挡住别人。
- `db stats` 走只读连接，不需要锁，采集进行中也能随时查看。
- 退出码约定：`0` 成功（含单条详情失败）；`1` 环境类问题（如数据库文件不存在）；
  `2` 配置/采集/存储错误；`3` 数据库被其他 run 占用。

数据库写连接启用 SQLite WAL 模式并设置 `busy_timeout`：采集写入与只读统计
可以并发，`db stats` 不再被写事务阻塞。WAL 是数据库的持久化属性，已有老库
首次被新版本打开时自动从默认日志模式平滑切换，历史数据不受影响（会多出
`*.db-wal` / `*.db-shm` 侧车文件，属正常现象）。

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
