# crawl-flow

配置驱动的公开数据采集管道：**列表翻页 → 详情抓取 → 解析 → 归一化 → 原始响应留档 → SQLite 入库**，
并内置**增量游标 / 断点续采**：每个采集源可独立配置游标规则，任务中断后重启自动从上次位置继续。
面向小团队做公开数据长期采集的底座，自带本地 mock 站点，断网也能跑通全流程。

## 定位

- 是：可配置的多源采集管道、适配器可插拔、原始响应可追溯、结构化记录幂等入库、按源独立的断点续采。
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
crawl-flow run --all --dry-run          # 解析但不写 raw、数据库与游标
crawl-flow run --source demo_api_flaky # 详情一律 500 的演练源
crawl-flow db stats --db data/crawl.db
crawl-flow cursor list                 # 查看各源游标状态
crawl-flow cursor reset --source demo_api   # 清空单源游标，回到全量采集
crawl-flow cursor reset --all          # 清空配置中所有源的游标
```

## 增量游标 / 断点续采

游标模块（`src/crawl_flow/cursor/`）独立于采集主流程，纯本地 JSON 持久化，
不依赖任何外部服务，也不引入新的第三方依赖。

- **按源配置**：在源定义里加 `cursor` 段即可启用；不配置或 `enabled: false` 时行为与旧版完全一致（全量）。
  - `rule: completed_ids`（默认）：按 `external_id` 记录已完成条目，列表条目增删也不会误判，适合长期周期采集。
  - `rule: list_position`：按列表枚举位置记录断点，只依赖顺序，适合列表顺序稳定的源。
- **自动初始化 / 跳过**：首次运行自动建立游标；再次运行时已完成条目在发起详情请求前跳过（汇总里的 `cursor_skipped`）。
- **中断续采**：每条记录在**全流程处理完成后**才原子写入游标（临时文件 + `os.replace`），
  任务被网络波动或手动终止后，重启自动从终止位置继续，已完成条目不重复请求。
- **手动重置**：`crawl-flow cursor reset` 清空游标文件，下次运行回到全量采集。
- **损坏降级**：游标文件损坏、版本不支持或规则不匹配时，输出 `cursor degraded` 告警并自动回退全量采集，任务不崩溃。
- **单源隔离**：一源一个游标文件（默认 `data/cursors/{source}.json`），单个源的游标异常不影响其他源。
- **空列表不推进**：空列表不会产生或推进游标；`--dry-run` 下游标完全旁路（不读、不写、不跳过）。

```yaml
sources:
  - name: demo_api
    type: api
    base_url: http://127.0.0.1:8010
    cursor:
      enabled: true
      rule: completed_ids   # 或 list_position
```

## 目录速览

```
crawl-flow/
├── config/sources.yaml      # 源定义（demo_api / demo_html 等，含 cursor 配置示例）
├── src/crawl_flow/          # 管道实现（CLI、配置、适配器、游标、存储）
├── mock_site/               # 本地 mock 站点与 fixtures
├── tests/                   # pytest 测试（自带随机端口 mock 服务）
└── docs/项目说明.md         # 详细设计、数据模型与测试说明
```

完整结构、F1–F8 功能拆解、数据表字段与关键流程见 [docs/项目说明.md](docs/项目说明.md)。

## 已知限制

以下能力**当前不实现**，属于后续迭代范围：

1. **并发抓取 / 全局限速 / 代理池**：单线程串行请求，无速率控制、无代理。
2. **内容指纹去重**：只保证 `(source, external_id)` 唯一，不做正文指纹。
3. **重试退避 / 死信队列 / 失败重放**：仅对连接级传输异常做一次立即重试，5xx 不重试，失败只留档不自动重放。
4. **清洗规则 DSL / 字段质量校验**：清洗逻辑固定在代码中，不做可配置 DSL 与质量评分。
5. **任务编排 / 定时调度 / 指标暴露**：只能手动执行 CLI，无调度器与监控指标。
6. **浏览器渲染（Playwright）**：只抓取静态 HTTP 响应，不执行 JavaScript。

> 增量游标 / 断点续采已实现，详见上文「增量游标 / 断点续采」。
