"""详情页结构分析：定位必填字段并生成提取规则。

用户只需要给出必填字段的**语义说明**（标准字段名 + 可选关键词提示），
分析器在多个详情样本上做一致性匹配，输出 :class:`FieldRule`。
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup, Tag

from crawl_flow.autogen.errors import GenerationError
from crawl_flow.autogen.selectors import _classes_of, relative_selector
from crawl_flow.autogen.spec import FieldRule

# 标准字段 → 中英文关键词（class/id/标签名/邻近文本匹配用）
FIELD_KEYWORDS: dict[str, tuple[str, ...]] = {
    "title": ("title", "entry-title", "headline", "heading", "标题", "文章标题"),
    "content": ("content", "entry-content", "article-body", "body", "正文", "内容"),
    "published_at": ("date", "time", "published", "pub", "publish", "日期", "发布", "时间"),
    "category": ("category", "cat", "分类", "栏目"),
    "tags": ("tag", "tags", "标签"),
}

# 标题候选标签
_TITLE_TAGS = ("h1", "h2", "h3")
# 日期文本识别
_DATE_TEXT = re.compile(
    r"(\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}日?)"
    r"|(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2})"
)
# 列表型标签的容器
_TAG_CONTAINER_TAGS = ("ul", "ol")


@dataclass(frozen=True)
class RequiredField:
    """用户声明的必填字段。"""

    name: str
    keywords: tuple[str, ...] = ()


def analyze_detail(
    samples: list[tuple[str, BeautifulSoup]],
    required: list[RequiredField],
) -> tuple[str, dict[str, FieldRule]]:
    """返回 (详情容器选择器, 字段规则字典)。"""

    if not samples:
        raise GenerationError("没有可用的详情页样本", stage="detail_analysis")

    scope = _pick_scope(samples)
    rules: dict[str, FieldRule] = {}
    for field in required:
        rule = _locate_field(scope, samples, field)
        rules[field.name] = rule
    return relative_selector(scope.parent or scope, scope), rules


# ---------- 详情容器 ----------


def _pick_scope(samples: list[tuple[str, BeautifulSoup]]) -> Tag:
    """选择在所有样本中都存在、且同时包住标题与大段正文的最小公共容器。"""

    common: list[Tag] = []
    first_scope: Tag | None = None
    for index, (_, soup) in enumerate(samples):
        candidates = [
            el
            for el in soup.find_all(True)
            if el.find(_TITLE_TAGS) is not None
            and (len(el.get_text(strip=True)) > 40 or el.find("p"))
        ]
        if not candidates:
            raise GenerationError(
                "详情页样本中找不到包含标题与正文的容器",
                stage="detail_analysis",
            )
        scope = _best_scope_candidate(candidates)
        if index == 0:
            first_scope = scope
            common = [scope]
        else:
            common = [el for el in common if _same_shape(el, scope)]
    if first_scope is None:
        raise GenerationError("详情容器识别失败", stage="detail_analysis")
    return common[0] if common else first_scope


# 语义上最像正文容器的标签，优先级从高到低
_SCOPE_TAG_PRIORITY = ("article", "main", "section", "div")


def _best_scope_candidate(candidates: list[Tag]) -> Tag:
    """优先 article/main 等语义容器；同标签内取文本最小（最贴近正文）的一个。"""

    for tag_name in _SCOPE_TAG_PRIORITY:
        same_tag = [el for el in candidates if el.name == tag_name]
        if same_tag:
            return min(same_tag, key=lambda el: len(el.get_text(strip=True)))
    return min(candidates, key=lambda el: len(el.get_text(strip=True)))


def _same_shape(a: Tag, b: Tag) -> bool:
    return a.name == b.name and bool(_classes_of(a)) == bool(_classes_of(b))


# ---------- 字段定位 ----------


def _locate_field(scope: Tag, samples: list[tuple[str, BeautifulSoup]], field: RequiredField) -> FieldRule:
    keywords = field.keywords or FIELD_KEYWORDS.get(field.name, (field.name,))
    locator = _LOCATORS.get(field.name)
    if locator is None:
        raise GenerationError(f"不支持的标准字段: {field.name}", stage="detail_analysis")

    rule: FieldRule | None = None
    for _, soup in samples:
        page_scope = _find_matching_scope(soup, scope)
        if page_scope is None:
            continue
        candidate = locator(page_scope, keywords)
        if candidate is None:
            continue
        candidate_rule = _build_rule(field.name, page_scope, candidate)
        if rule is None:
            rule = candidate_rule
        elif candidate_rule.selector != rule.selector:
            # 不同样本选择器不一致时保留第一个可用规则，但要求至少首个样本命中
            pass
    if rule is None:
        raise GenerationError(
            f"必填字段 {field.name} 在详情样本中定位失败，关键词: {', '.join(keywords)}",
            stage="detail_analysis",
        )
    # 用全部样本复核：选择器必须在每个样本都能命中
    for _, soup in samples:
        page_scope = _find_matching_scope(soup, scope)
        if page_scope is None or not page_scope.select(rule.selector):
            raise GenerationError(
                f"字段 {field.name} 的提取规则无法在全部详情样本上稳定命中: {rule.selector}",
                stage="detail_analysis",
            )
    return rule


def _find_matching_scope(soup: BeautifulSoup, reference: Tag) -> Tag | None:
    classes = _classes_of(reference)
    if classes:
        return soup.select_one(f"{reference.name}.{classes[0]}")
    return soup.find(reference.name)


def _build_rule(field_name: str, scope: Tag, element: Tag) -> FieldRule:
    selector = relative_selector(scope, element)
    if field_name == "content":
        return FieldRule(selector=selector, mode="html")
    if field_name == "tags":
        # 列表型标签：选择器指向重复的 li
        if element.name in _TAG_CONTAINER_TAGS:
            children = element.find_all("li")
            if children:
                return FieldRule(
                    selector=f"{selector} > li",
                    mode="tags_text",
                )
        return FieldRule(selector=selector, mode="text")
    return FieldRule(selector=selector, mode="text")


# ---------- 各字段定位器 ----------


def _locate_title(scope: Tag, keywords: tuple[str, ...]) -> Tag | None:
    for tag in _TITLE_TAGS:
        for element in scope.find_all(tag):
            if _matches_keywords(element, keywords) or len(scope.find_all(tag)) == 1:
                return element
    # 兜底：class 带 title/headline 的任意元素
    return _find_by_class(scope, keywords)


def _locate_content(scope: Tag, keywords: tuple[str, ...]) -> Tag | None:
    # 优先 class 命中
    by_class = _find_by_class(scope, keywords)
    if by_class is not None:
        return by_class
    # 兜底：段落最多的容器
    containers = [el for el in scope.find_all(True) if el.find_all("p")]
    if not containers:
        return None
    return max(containers, key=lambda el: len(el.find_all("p")))


def _locate_date(scope: Tag, keywords: tuple[str, ...]) -> Tag | None:
    # 1) time 标签
    time_el = scope.find("time")
    if time_el is not None:
        return time_el
    # 2) class/id 命中
    by_class = _find_by_class(scope, keywords)
    if by_class is not None:
        return by_class
    # 3) 文本像日期的最浅元素
    for element in scope.find_all(True):
        if _DATE_TEXT.search(element.get_text(" ", strip=True)) and not element.find(True):
            return element
    return None


def _locate_category(scope: Tag, keywords: tuple[str, ...]) -> Tag | None:
    return _find_by_class(scope, keywords)


def _locate_tags(scope: Tag, keywords: tuple[str, ...]) -> Tag | None:
    by_class = _find_by_class(scope, keywords)
    if by_class is not None:
        return by_class
    # 兜底：带多个 li 的 ul/ol
    for container in scope.find_all(_TAG_CONTAINER_TAGS):
        if len(container.find_all("li")) >= 2:
            return container
    return None


_LOCATORS = {
    "title": _locate_title,
    "content": _locate_content,
    "tags": _locate_tags,
    "published_at": _locate_date,
    "category": _locate_category,
}


# ---------- 关键词匹配 ----------


def _matches_keywords(element: Tag, keywords: tuple[str, ...]) -> bool:
    haystack = " ".join(
        [element.name, " ".join(_classes_of(element)), str(element.get("id", ""))]
    ).lower()
    return any(keyword.lower() in haystack for keyword in keywords)


def _find_by_class(scope: Tag, keywords: tuple[str, ...]) -> Tag | None:
    best: Tag | None = None
    for element in scope.find_all(True):
        if _matches_keywords(element, keywords):
            # 取文档顺序中最浅（层级最高）的命中，避免选到嵌套的同名子元素
            if best is None or _depth(element) < _depth(best):
                best = element
    return best


def _depth(element: Tag) -> int:
    depth = 0
    current: Tag | None = element
    while current is not None and current.parent is not None:
        depth += 1
        current = current.parent
    return depth
