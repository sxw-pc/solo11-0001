# 站点适配器自动生成与适配校验模块（autogen）

本文档描述 `crawl_flow.autogen` 包的设计、约束与使用方式。模块目标：**用户只需提供
目标站点入口地址与必填字段说明，即可自动生成可直接接入现有采集管道的声明式适配器，
并能定期校验适配器在站点结构变更后的可用性，给出可落地的调整建议。**

## 1. 设计原则

1. **零核心改动接入**：新增源类型 `auto_html`，通过现有 `create_adapter` 工厂注册
   `GenericHtmlAdapter`。采集主流程 `pipeline/run.py`、归一化 `pipeline/normalize.py`
   完全不改动，生成适配器的输出（`records` / `raw_records` 表结构、Record 字段、
   raw payload 键）与手写适配器完全一致。
2. **产物是声明式数据，不是代码**：适配器规格 `AdapterSpec` 只包含 CSS 选择器、
   URL 模板、提取模式等静态声明，不含任何可执行片段，可被静态安全审计。
3. **不执行站点内容**：样本只做 HTML 静态解析，不存在浏览器/模板引擎/`eval` 执行面；
   生成期沙箱扫描 + 运行期元素摘除构成纵深防御。
4. **所有失败显式化**：能力边界、结构识别、回放校验、沙箱审计各阶段失败都抛出带
   中文原因与错误码的领域异常，CLI 以差异化退出码呈现，杜绝静默失败。

## 2. 模块结构

```
src/crawl_flow/autogen/
├── constants.py        # 全部硬约束常量（2s 限速 / 3 版本 / 100MB / 1h 校验等）
├── errors.py           # UnsupportedSiteError / GenerationError / SandboxViolation ...
├── spec.py             # 声明式适配器规格 AdapterSpec（pydantic 校验）
├── limiter.py          # 进程内共享限速器：最小间隔 + 优先级排队 + 队龄老化
├── polite.py           # PoliteFetcher：限速 + 优先级 + 响应体积上限
├── detector.py         # 登录 / 验证码 / JS-only 能力边界检测
├── renderer.py         # 可选浏览器渲染器（默认关闭，显式开启，依赖缺失明确报错）
├── sandbox.py          # 恶意样本静态扫描 + 规格安全审计
├── selectors.py        # CSS 选择器生成与重复结构发现
├── list_analysis.py    # 列表条目 / 详情链接 / 分页 / external_id 模式识别
├── detail_analysis.py  # 详情容器与必填字段定位
├── sample_store.py     # 版本化样本库（3 版本 / 100MB 治理）
├── generator.py        # 端到端生成编排 + 离线回放校验
├── validator.py        # 在线适配校验：漂移检测 + 调整建议
├── scheduler.py        # 周期校验调度器（守护线程，支持外部调度器接管）
└── cli.py              # autogen 子命令
src/crawl_flow/sources/generic_html.py  # 解释 AdapterSpec 的运行时适配器
mock_site/blog/          # 公开静态博客 mock（v1/v2 结构 + 登录/验证码/JS/恶意场景）
```

## 3. 端到端生成流程

```
入口 URL + 必填字段
  │
  ├─ 1. 能力边界检测（静态 HTML 信号）
  │     密码框/登录表单 → UnsupportedSiteError(login_required)
  │     验证码输入/图片 → UnsupportedSiteError(captcha_required)
  │     JS-only 且未开渲染 → UnsupportedSiteError(dynamic_rendering_disabled)
  │
  ├─ 2. 限速爬取列表样本（最多 3 页，每页间隔 ≥ 2s）
  │     沙箱扫描 → 识别重复条目容器、详情链接、分页规则、id 模式
  │
  ├─ 3. 限速爬取详情样本（最多 5 个，至少 2 个）
  │     沙箱扫描 → 识别正文容器与每个必填字段的稳定选择器
  │
  ├─ 4. 组装 AdapterSpec → 规格静态安全审计（选择器白名单 / 引擎白名单）
  │
  ├─ 5. 离线回放校验：用 GenericHtmlAdapter 跑样本，
  │     列表必须有条目、每个必填字段在每个样本上必须有值
  │
  └─ 6. 样本版本化留档（保留最近 3 版，总量超 100MB 回收最旧版本）
```

任一步骤失败都会在错误信息里标注阶段（`[list_analysis]` / `[replay]` 等）与具体原因。

## 4. 硬性约束与实现位置

| 需求约束 | 实现 |
| --- | --- |
| 请求频率默认每 2 秒至多 1 次 | `constants.DEFAULT_MIN_INTERVAL_SECONDS` + `limiter.RateLimiter` |
| 样本保留最近 3 个版本 | `constants.DEFAULT_SAMPLE_VERSIONS` + `SampleStore._apply_retention` |
| 样本总量 ≤ 100MB，超出清理最早样本 | `constants.DEFAULT_SAMPLE_MAX_BYTES` + `SampleStore._enforce_global_cap` |
| 登录 / 验证码站点明确不支持 | `detector.ensure_site_supported`，退出码 3 |
| 动态渲染默认关闭、显式开启 | `constants.DYNAMIC_RENDER_ENABLED_BY_DEFAULT=False`，CLI `--enable-dynamic-rendering` |
| 恶意样本沙箱拦截、不触发本地执行 | `sandbox.scan_html`（生成期）+ `generic_html.sanitize_soup`（运行期），退出码 4 |
| 校验任务不抢占正常采集 | 共享同一限速器，校验固定 `PRIORITY_VALIDATION=10`，采集 `PRIORITY_CRAWL=0` |
| 校验周期默认 1 小时 | `constants.DEFAULT_VALIDATION_INTERVAL_SECONDS` + `scheduler.ValidationScheduler` |
| 生成失败不静默 | 全链路领域异常 + CLI 退出码 3/4/5/6 |
| 5 分钟内生成可用适配器 | 样本请求数有界（3 列表页 + 5 详情页），测试断言耗时 < 300s |
| 单页响应体积保护 | `constants.MAX_HTML_BYTES`（5MB），`PoliteFetcher` 超限拒绝 |

## 5. 安全模型

- **生成期沙箱**：`scan_html` 识别 `<script>`、内联事件（`on*` 属性）、
  `javascript:`/`vbscript:` 危险协议、`iframe/object/embed/applet` 外联载体、
  data-URI HTML，命中即抛 `SandboxViolation` 并附带逐条威胁清单，不产出适配器。
- **规格审计**：`audit_spec` 校验所有选择器只含静态 CSS 语法字符，拒绝
  `expression(`、`url(`、反引号、反斜杠等；引擎只允许 `generic_html`。
- **运行期摘除**：`GenericHtmlAdapter._soup` 每次解析都摘除脚本节点、内联事件、
  危险协议属性。即使样本绕过生成期检查，运行时也不存在代码执行路径。
- **无动态执行原语**：通用适配器源码经 `assert_no_execution_primitives` 检查，
  不包含 `eval` / `exec` / `subprocess` / `os.system`。

## 6. 适配校验与漂移建议

`AdapterValidator.validate(spec)` 以低优先级重新取样（2 个列表页 + 最多 3 个详情页），
逐项核对规格中的选择器是否仍命中：

- `healthy`：全部命中；
- `drifted`：有选择器失效，但页面仍可解析；同时用生成期同一套分析器重新识别结构，
  输出 `AdjustmentSuggestion`（旧选择器 → 新选择器 + 置信度 + 理由），建议可直接
  用于再生成；
- `broken`：样本无法获取或结构彻底无法识别，报告携带具体 HTTP / 解析原因。

周期调度由 `ValidationScheduler` 提供：守护线程按间隔执行，单任务异常隔离，
也支持 `run_once()` 交给 cron / k8s CronJob 等外部调度器接管。

## 7. CLI 用法与退出码

```bash
# 生成（动态渲染默认关闭；--min-interval 默认 2.0）
crawl-flow autogen generate http://127.0.0.1:8020/posts \
  --field title --field content --field published_at --field category --field tags \
  --output config/generated --sample-root data/autogen_samples

# 在线校验
crawl-flow autogen validate config/generated/<site>.spec.json

# 周期校验（默认 1 小时；--once 供外部调度器）
crawl-flow autogen validate-schedule config/generated/<site>.spec.json --once

# 样本治理查看
crawl-flow autogen samples http://127.0.0.1:8020/posts

# 博客 mock（v1 正常结构 / v2 模拟改版）
crawl-flow autogen serve-blog --port 8020 --variant v1
```

| 退出码 | 含义 |
| --- | --- |
| 0 | 成功 / 校验健康 |
| 3 | 站点不在能力边界（登录、验证码、未开启的动态渲染、渲染依赖缺失） |
| 4 | 沙箱拦截到恶意样本 |
| 5 | 结构识别 / 回放校验等生成失败 |
| 6 | 适配校验发现 drifted / broken |

## 8. 能力边界

当前自动生成仅支持**静态 HTML 列表 + 静态 HTML 详情**站点。以下场景明确拒绝并
告知原因，不会生成无效适配器：

- 需要登录或任何形式人机验证（验证码、reCAPTCHA/hCaptcha）的站点；
- 内容完全由 JavaScript 渲染的站点（显式 `--enable-dynamic-rendering` 后进入
  浏览器渲染分支，且环境必须自行安装 Playwright 与 Chromium）；
- 非 HTML 响应（文件下载、纯 API 接口等）；
- 列表无重复条目结构、详情样本不足 2 个、必填字段无法在全部样本稳定命中的站点。
