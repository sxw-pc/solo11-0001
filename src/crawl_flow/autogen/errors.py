"""自动生成 / 适配校验模块的异常体系。

所有自动生成失败都通过具体异常携带**明确的失败原因**向上抛出，
CLI 层统一渲染为非零退出码，杜绝静默失败。
"""

from __future__ import annotations


class AutoGenError(Exception):
    """autogen 模块所有可预期异常的基类。"""


class UnsupportedSiteError(AutoGenError):
    """站点落在能力边界外（需要登录 / 验证码 / 未开启的动态渲染等）。

    ``reason_code`` 供程序化消费（监控、测试断言），``reason`` 是面向用户的中文说明。
    """

    def __init__(self, reason: str, *, reason_code: str) -> None:
        super().__init__(reason)
        self.reason = reason
        self.reason_code = reason_code


class GenerationError(AutoGenError):
    """样本爬取或结构识别失败，无法生成可用适配器。"""

    def __init__(self, reason: str, *, stage: str, detail: str | None = None) -> None:
        message = f"[{stage}] {reason}"
        if detail:
            message = f"{message}: {detail}"
        super().__init__(message)
        self.reason = reason
        self.stage = stage
        self.detail = detail


class SandboxViolation(AutoGenError):
    """沙箱校验在样本中发现可触发代码执行的危险内容。"""

    def __init__(self, reason: str, *, threats: list[str]) -> None:
        super().__init__(reason)
        self.reason = reason
        self.threats = threats


class AdapterSpecError(AutoGenError):
    """适配器规格文件本身不合法（字段缺失、选择器不安全等）。"""
