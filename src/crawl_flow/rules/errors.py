"""规则引擎异常体系。

- :class:`RuleConfigError`：配置/语法校验失败（启动期严格校验、``validate-rules``）。
- :class:`RuleExecutionError`：单条规则运行期失败，引擎内部捕获后按 ``on_fail`` 降级，
  一般不会向上抛到采集主流程。
"""

from __future__ import annotations

from crawl_flow.errors import CrawlFlowError


class RuleConfigError(CrawlFlowError):
    """规则配置缺失、语法错误或语义校验失败。"""

    def __init__(self, message: str, *, errors: list[str] | None = None) -> None:
        super().__init__(message)
        self.errors: list[str] = list(errors or [])


class RuleExecutionError(CrawlFlowError):
    """单条清洗/校验规则执行失败（可恢复，引擎按 on_fail 策略处理）。"""

    def __init__(self, rule_type: str, message: str) -> None:
        full = f"rule={rule_type} error={message}"
        super().__init__(full)
        self.rule_type = rule_type
        self.message = message


class ValidationFailure(Exception):
    """校验规则未通过的内部信号，携带面向用户的失败原因。"""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message
