"""autogen 模块的默认策略常量。

所有数字都对应需求中的硬性约束，集中在此便于审计与测试。
"""

from __future__ import annotations

# 自动爬取样本的默认最小请求间隔：每 2 秒至多 1 次请求。
DEFAULT_MIN_INTERVAL_SECONDS = 2.0

# 样本默认保留最近 3 个版本。
DEFAULT_SAMPLE_VERSIONS = 3

# 样本总存储空间上限 100MB，超出后按最旧版本优先清理。
DEFAULT_SAMPLE_MAX_BYTES = 100 * 1024 * 1024

# 生成阶段最多爬取的列表页数 / 详情样本数（控制访问压力与 5 分钟预算）。
GENERATION_MAX_LIST_PAGES = 3
GENERATION_MAX_DETAIL_SAMPLES = 5

# 适配校验阶段最多取样的详情页数（校验是低优先级任务，尽量轻量）。
VALIDATION_MAX_DETAIL_SAMPLES = 3

# 默认定期适配校验周期：1 小时。
DEFAULT_VALIDATION_INTERVAL_SECONDS = 3600

# 校验任务相对正常采集任务的优先级权重（仅用于共享限速器排队）。
PRIORITY_CRAWL = 0
PRIORITY_VALIDATION = 10

# 单份 HTML 样本的最大解析体积，防御解压炸弹 / 超大页面。
MAX_HTML_BYTES = 5 * 1024 * 1024

# 动态渲染站点的自动生成能力默认关闭。
DYNAMIC_RENDER_ENABLED_BY_DEFAULT = False

# 适配器规格版本，便于后续演进与灰度。
ADAPTER_SPEC_VERSION = 1
