"""沙箱校验：在生成期拦截恶意样本，并对适配器规格做静态安全审计。

本模块的安全模型是「**不执行任何站点内容**」：

1. 样本只做 HTML 静态解析，绝不交给浏览器 / 模板引擎 / ``eval`` 执行；
2. :func:`scan_html` 扫描一切可能在渲染侧触发代码执行的结构——
   ``<script>``、内联事件属性（onerror/onclick...）、``javascript:`` 等危险协议、
   ``iframe/object/embed`` 外联载体、data-URI 脚本，发现即判定为威胁；
3. :func:`audit_spec` 审计生成的声明式规格，确保选择器与属性名落在白名单内，
   规格中不存在任何可执行载荷。

运行时 :class:`~crawl_flow.sources.generic_html.GenericHtmlAdapter` 还会再做一次
元素摘除（纵深防御），但恶意样本在生成阶段就会被这里拦截，不会产出适配器。
"""

from __future__ import annotations

import re
from typing import Any

from bs4 import BeautifulSoup, Tag

from crawl_flow.autogen.errors import AdapterSpecError, SandboxViolation
from crawl_flow.autogen.spec import AdapterSpec

_INLINE_EVENT = re.compile(r"^on[a-z]+$", re.IGNORECASE)
_DANGEROUS_PROTOCOL = re.compile(r"^\s*(javascript|vbscript)\s*:", re.IGNORECASE)
_DATA_SCRIPT = re.compile(r"^\s*data:\s*text/html", re.IGNORECASE)
_EMBED_NODES = ("iframe", "object", "embed", "applet")
# 静态审计：选择器 / 属性名只允许这些字符出现。
_SELECTOR_ALLOWED = re.compile(r"^[A-Za-z0-9_\-\s.,#:>*=\[\]\"'()~^$|]+$")
_SAFE_ATTR = re.compile(r"^[a-zA-Z_:][-a-zA-Z0-9_:.]*$")


def scan_html(html: str, *, source: str = "") -> list[str]:
    """扫描一份 HTML 样本，返回命中的威胁描述列表（空列表表示安全）。"""

    threats: list[str] = []
    soup = BeautifulSoup(html, "lxml")

    for element in soup.find_all("script"):
        threats.append(f"{source} 包含 <script> 元素".strip())
    for node in _EMBED_NODES:
        for element in soup.find_all(node):
            threats.append(f"{source} 包含外联载体 <{node}>".strip())

    for element in soup.find_all(True):
        if not isinstance(element, Tag):
            continue
        for attr_name, attr_value in list(element.attrs.items()):
            if _INLINE_EVENT.match(attr_name):
                threats.append(
                    f"{source} 元素 <{element.name}> 带内联事件属性 {attr_name}".strip()
                )
                continue
            # BeautifulSoup 对 class 等属性返回列表，对 href/src 返回字符串，统一展开
            values = attr_value if isinstance(attr_value, list) else [attr_value]
            for piece in values:
                if not isinstance(piece, str):
                    continue
                if _DANGEROUS_PROTOCOL.match(piece):
                    threats.append(
                        f"{source} 元素 <{element.name}> 的 {attr_name} 使用危险协议".strip()
                    )
                elif _DATA_SCRIPT.match(piece):
                    threats.append(
                        f"{source} 元素 <{element.name}> 的 {attr_name} 内嵌 data-URI HTML".strip()
                    )

    # 去重，保持顺序稳定，方便测试断言与告警展示。
    return list(dict.fromkeys(threats))


def assert_sample_safe(html: str, *, source: str = "") -> None:
    """样本存在威胁时抛 :class:`SandboxViolation`，否则静默通过。"""

    threats = scan_html(html, source=source)
    if threats:
        raise SandboxViolation(
            f"沙箱校验拦截：样本包含 {len(threats)} 处可触发代码执行的危险内容（{source}）",
            threats=threats,
        )


def audit_spec(spec: AdapterSpec) -> None:
    """对生成的适配器规格做静态安全审计，不合法直接拒绝落盘。"""

    problems: list[str] = []
    list_spec, detail_spec = spec.list, spec.detail

    selectors = [
        list_spec.item_selector,
        list_spec.link_selector,
        detail_spec.scope_selector,
    ]
    selectors += [rule.selector for rule in detail_spec.fields.values()]
    if list_spec.next_selector:
        selectors.append(list_spec.next_selector)

    for selector in selectors:
        if not _SELECTOR_ALLOWED.match(selector):
            problems.append(f"选择器包含白名单外字符: {selector}")
        lowered = selector.lower()
        for token in ("expression(", "javascript:", "url(", "`", "\\"):
            if token in lowered:
                problems.append(f"选择器包含不安全片段 {token}: {selector}")

    attrs = [list_spec.link_attr] + [
        rule.attr for rule in detail_spec.fields.values() if rule.attr is not None
    ]
    for attr in attrs:
        if not _SAFE_ATTR.match(attr):
            problems.append(f"非法属性名: {attr}")

    if spec.engine != "generic_html":
        problems.append(f"未知引擎（拒绝任何非声明式引擎）: {spec.engine}")

    if problems:
        raise AdapterSpecError("适配器规格沙箱审计失败: " + "; ".join(problems))


def assert_no_execution_primitives(runtime_source: str) -> None:
    """额外保险：审计运行时代码文本，确认不存在动态执行原语。

    对通用适配器自身源码做检查，作为「声明式规格永不落地为代码执行」的佐证。
    只检查代码行（跳过注释/文档字符串行），避免把说明文字误判为调用；
    ``re.compile`` 是静态正则编译，不属于动态执行原语。
    """

    forbidden = ("eval(", "exec(", "__import__(", "subprocess", "os.system")
    code_lines = [
        line
        for line in runtime_source.splitlines()
        if line.strip() and not line.lstrip().startswith(("#", '"', "'"))
    ]
    code_text = "\n".join(code_lines)
    hits = [token for token in forbidden if token in code_text]
    if hits:
        raise AdapterSpecError(f"运行时包含动态执行原语: {', '.join(hits)}")


def scan_payload(value: Any, *, source: str = "") -> list[str]:
    """供校验阶段复用：非 HTML 样本直接放行，HTML 样本走同一套扫描。"""

    if not isinstance(value, str):
        return []
    if "<" not in value or ">" not in value:
        return []
    return scan_html(value, source=source)
