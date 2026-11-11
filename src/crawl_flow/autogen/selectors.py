"""CSS 选择器生成与页面结构分析工具。

选择器生成策略：优先使用 ``tag.class`` 链（稳定、可读、可被人工调整），
无 class 时退化为 ``tag:nth-of-type(n)`` 路径。所有输出都在沙箱白名单字符集内。
"""

from __future__ import annotations

from typing import Iterator

from bs4 import BeautifulSoup, Tag

# 噪音 class：纯布局/组件框架命名，不代表业务结构
_NOISE_CLASSES = {
    "container",
    "wrapper",
    "inner",
    "row",
    "col",
    "clearfix",
    "active",
    "hidden",
}


def _classes_of(element: Tag) -> list[str]:
    classes = element.get("class")
    if not isinstance(classes, list):
        return []
    return [c for c in classes if c and c not in _NOISE_CLASSES]


def build_selector(element: Tag, *, root: Tag | None = None) -> str:
    """生成从 ``root``（默认 body）到 ``element`` 的 CSS 选择器。

    输出不含 body/document 前缀：body 无 class 时只会产生脆弱的
    ``body:nth-of-type(1)`` 噪声，业务页面里 body 也天然唯一。
    """

    chain: list[str] = []
    current: Tag | None = element
    while current is not None and isinstance(current, Tag) and current.name not in (
        "[document]",
        "html",
        "body",
    ):
        if root is not None and current is root:
            break
        classes = _classes_of(current)
        if classes:
            chain.append(f"{current.name}.{classes[0]}")
        else:
            nth = _position_among_type(current)
            chain.append(f"{current.name}:nth-of-type({nth})")
        current = current.parent
    chain.reverse()
    return " > ".join(chain)


def _position_among_type(element: Tag) -> int:
    parent = element.parent
    if parent is None:
        return 1
    same_type = [sib for sib in parent.find_all(element.name, recursive=False)]
    return same_type.index(element) + 1


def relative_selector(ancestor: Tag, descendant: Tag) -> str:
    """生成相对 ``ancestor`` 的选择器（不含祖先自身），用于条目内 / 容器内提取。"""

    chain: list[str] = []
    current: Tag | None = descendant
    while current is not None and isinstance(current, Tag) and current is not ancestor:
        classes = _classes_of(current)
        if classes:
            chain.append(f"{current.name}.{classes[0]}")
        else:
            chain.append(f"{current.name}:nth-of-type({_position_among_type(current)})")
        current = current.parent
    chain.reverse()
    return " > ".join(chain) if chain else ancestor.name or "body"


def walk_elements(soup: BeautifulSoup, *, skip: tuple[str, ...] = ("script", "style", "nav", "header", "footer")) -> Iterator[Tag]:
    """按文档顺序遍历可作为结构候选的元素。"""

    for element in soup.find_all(True):
        if element.name in skip:
            continue
        yield element


def repeated_children(soup: BeautifulSoup) -> dict[str, list[Tag]]:
    """按「父元素 + 子元素签名」分组，找出重复出现的条目容器候选。

    签名 = (父元素 id, 子 tag, 首个有意义 class)，同签名出现 >=2 次即视为重复结构。
    """

    groups: dict[str, list[Tag]] = {}
    for parent in soup.find_all(True):
        if parent.name in ("script", "style", "nav", "header", "footer"):
            continue
        buckets: dict[str, list[Tag]] = {}
        for child in parent.find_all(True, recursive=False):
            classes = _classes_of(child)
            signature = f"{child.name}.{classes[0]}" if classes else child.name
            buckets.setdefault(signature, []).append(child)
        for signature, members in buckets.items():
            if len(members) >= 2:
                key = f"{id(parent)}::{signature}"
                groups[key] = members
    return groups
