"""站点采集适配器自动生成与适配校验模块。

公共入口：

- :class:`AdapterGenerator`：入口 URL + 必填字段 → 声明式 AdapterSpec；
- :class:`AdapterValidator` / :class:`ValidationScheduler`：定期适配校验与漂移告警；
- :class:`SampleStore`：版本化样本留档（3 版 / 100MB 治理）；
- :class:`RateLimiter`：生成与校验共享的全局限速器（默认每 2 秒 1 次）。
"""

from __future__ import annotations

from crawl_flow.autogen.constants import (
    DEFAULT_MIN_INTERVAL_SECONDS,
    DEFAULT_SAMPLE_MAX_BYTES,
    DEFAULT_SAMPLE_VERSIONS,
    DEFAULT_VALIDATION_INTERVAL_SECONDS,
)
from crawl_flow.autogen.detail_analysis import RequiredField
from crawl_flow.autogen.errors import (
    AdapterSpecError,
    AutoGenError,
    GenerationError,
    SandboxViolation,
    UnsupportedSiteError,
)
from crawl_flow.autogen.generator import AdapterGenerator
from crawl_flow.autogen.limiter import RateLimiter, get_global_limiter, reset_global_limiter
from crawl_flow.autogen.sample_store import SampleStore
from crawl_flow.autogen.scheduler import ValidationScheduler
from crawl_flow.autogen.spec import AdapterSpec
from crawl_flow.autogen.validator import (
    AdapterValidator,
    AdjustmentSuggestion,
    DriftFinding,
    ValidationReport,
)

__all__ = [
    "AdapterGenerator",
    "AdapterSpec",
    "AdapterSpecError",
    "AdapterValidator",
    "AdjustmentSuggestion",
    "AutoGenError",
    "DEFAULT_MIN_INTERVAL_SECONDS",
    "DEFAULT_SAMPLE_MAX_BYTES",
    "DEFAULT_SAMPLE_VERSIONS",
    "DEFAULT_VALIDATION_INTERVAL_SECONDS",
    "DriftFinding",
    "GenerationError",
    "RateLimiter",
    "RequiredField",
    "SampleStore",
    "SandboxViolation",
    "UnsupportedSiteError",
    "ValidationReport",
    "ValidationScheduler",
    "get_global_limiter",
    "reset_global_limiter",
]
