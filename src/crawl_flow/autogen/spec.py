"""声明式适配器规格（AdapterSpec）。

规格是自动生成模块的唯一产物：**只包含 CSS 选择器与 URL 模板等静态声明，
不包含任何可执行代码**。运行时由 :class:`crawl_flow.sources.generic_html.GenericHtmlAdapter`
解释执行，因此同一份规格可以被沙箱静态审计，也能直接被现有管道加载。
"""

from __future__ import annotations

import re
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from crawl_flow.autogen.constants import ADAPTER_SPEC_VERSION
from crawl_flow.autogen.errors import AdapterSpecError

# 只允许白名单内的提取方式，从数据结构上杜绝代码注入面。
ExtractMode = Literal["text", "html", "attr", "tags_text"]
PaginationMode = Literal["query_param", "next_link"]


class FieldRule(BaseModel):
    """单个标准字段的提取规则。"""

    model_config = ConfigDict(extra="forbid")

    selector: str = Field(min_length=1, description="CSS 选择器，相对于 detail_scope")
    mode: ExtractMode = "text"
    attr: str | None = Field(default=None, description="mode=attr 时读取的属性名")
    optional: bool = False

    @field_validator("selector")
    @classmethod
    def _reject_unsafe_selector(cls, value: str) -> str:
        # 选择器只允许静态 CSS 语法；尤其禁止 expression/url( 等在老旧引擎里
        # 可能被当作脚本执行的写法，以及尖括号、反引号、反斜杠。
        forbidden = ("expression(", "url(", "javascript:", "`", "\\")
        lowered = value.lower()
        for token in forbidden:
            if token in lowered:
                raise ValueError(f"选择器包含不安全片段: {token}")
        if "<" in value:
            raise ValueError("选择器不允许包含尖括号 <")
        return value.strip()

    @field_validator("attr")
    @classmethod
    def _check_attr(cls, value: str | None, info: dict[str, object]) -> str | None:
        # pydantic v2 的 field_validator 无法直接拿同级字段，mode 约束放在 model 层补。
        if value is not None:
            value = value.strip()
            if any(ch in value for ch in (" ", '"', "'", "<", ">", "`")):
                raise ValueError("attr 只能是简单属性名")
        return value


class DetailSpec(BaseModel):
    """详情页提取规格。"""

    model_config = ConfigDict(extra="forbid")

    detail_path: str = Field(description="详情 URL 模板，必须包含 {id} 占位符")
    scope_selector: str = Field(min_length=1, description="详情正文的容器选择器")
    fields: dict[str, FieldRule]

    @field_validator("detail_path")
    @classmethod
    def _check_detail_path(cls, value: str) -> str:
        if "{id}" not in value:
            raise ValueError("detail_path 必须包含 {id} 占位符")
        return value

    def model_post_init(self, __context: object) -> None:
        for rule in self.fields.values():
            if rule.mode == "attr" and not rule.attr:
                raise AdapterSpecError("mode=attr 的字段规则必须给出 attr")


class ListSpec(BaseModel):
    """列表页提取规格。"""

    model_config = ConfigDict(extra="forbid")

    list_path: str = Field(min_length=1)
    item_selector: str = Field(min_length=1, description="每条列表条目的容器选择器")
    link_selector: str = Field(min_length=1, description="条目内详情链接选择器（相对 item）")
    link_attr: str = "href"
    id_pattern: str = Field(
        min_length=1,
        description="从详情 URL 提取 external_id 的正则，必须含且仅含一个捕获组",
    )
    pagination: PaginationMode
    page_param: str | None = Field(default=None, description="query_param 模式的页码参数名")
    next_selector: str | None = Field(default=None, description="next_link 模式的下一页链接选择器")

    def model_post_init(self, __context: object) -> None:
        if self.pagination == "query_param" and not self.page_param:
            raise AdapterSpecError("query_param 分页必须给出 page_param")
        if self.pagination == "next_link" and not self.next_selector:
            raise AdapterSpecError("next_link 分页必须给出 next_selector")
        # 只统计真正的捕获组，排除 (?:...) 非捕获组
        groups = re.findall(r"\((?!\?)", self.id_pattern)
        if len(groups) != 1:
            raise AdapterSpecError("id_pattern 必须恰好包含一个捕获组")


class AdapterSpec(BaseModel):
    """自动生成模块产出的完整适配器规格。"""

    model_config = ConfigDict(extra="forbid")

    spec_version: int = ADAPTER_SPEC_VERSION
    engine: Literal["generic_html"] = "generic_html"
    base_url: str
    list: ListSpec
    detail: DetailSpec
    # 生成时识别到的能力标记，供运维审计（如是否经过动态渲染）。
    capabilities: dict[str, str] = Field(default_factory=dict)

    @field_validator("base_url")
    @classmethod
    def _check_base_url(cls, value: str) -> str:
        if not value.startswith(("http://", "https://")):
            raise ValueError("base_url 必须以 http:// 或 https:// 开头")
        return value.rstrip("/")

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> "AdapterSpec":
        try:
            return cls.model_validate(payload)
        except Exception as exc:  # noqa: BLE001 - 统一转成领域异常
            raise AdapterSpecError(f"适配器规格校验失败: {exc}") from exc

    def to_source_config_dict(self, name: str) -> dict[str, object]:
        """转换为现有 ``config/sources.yaml`` 中一个源的配置片段。"""

        return {
            "name": name,
            "type": "auto_html",
            "base_url": self.base_url,
            "enabled": True,
            "page_size": 5,
            "timeout_seconds": 10.0,
            "adapter_spec": self.model_dump(mode="json"),
        }
