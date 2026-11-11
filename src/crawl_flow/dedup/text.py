"""指纹前的文本归一化与中文友好分词。

设计目标：

* 归一化规则可配置（大小写、NFKC、空白折叠、标点剥离），同一份内容在
  不同来源（HTML 空白差异、全角/半角差异等）下归一化结果一致。
* 分词不依赖任何预训练模型或第三方词表：中文（及其他 CJK 文字）按字符
  unigram + bigram 切分，拉丁字母/数字按词切分，纯标准库实现。
"""

from __future__ import annotations

import unicodedata


def normalize_text(text: str | None, *, strip_punctuation: bool = True) -> str:
    """把任意来源的文本归一化成稳定的指纹输入。

    步骤固定且确定性：

    1. ``None`` 或非字符串转字符串后做 Unicode NFKC（全角转半角、兼容字符合并）。
    2. 大小写折叠（``casefold``，对德语 ß 等也成立）。
    3. 按需剥离 Unicode 标点（``P*``）与符号（``S*``）：标点两侧都是 CJK
       字符时直接删除（中文标点不承担分词作用）；否则替换为空格，保证
       ``hello,world`` 不会粘连成 ``helloworld``。
    4. 规范化文字边界：CJK 与拉丁/数字交界补空格；两个 CJK 字符之间的
       空白删除（中文逐字切分，词间空格无语义），使 ``压测 包含`` 与
       ``压测包含``、``压测，包含`` 归一到同一结果。
    5. 剩余连续空白折叠为单个空格并裁剪首尾。

    标点剥离是可配置项：关闭后标点原样保留（文字边界仍按第 4 步规范化）。
    """

    if text is None:
        return ""
    normalized = unicodedata.normalize("NFKC", str(text)).casefold()
    if strip_punctuation:
        normalized = _strip_punctuation(normalized)
    normalized = _normalize_boundaries(normalized)
    return " ".join(normalized.split())


def _strip_punctuation(text: str) -> str:
    chars = list(text)
    kept: list[str] = []
    for index, char in enumerate(chars):
        category = unicodedata.category(char)
        if not (category.startswith("P") or category.startswith("S")):
            kept.append(char)
            continue
        previous = _nearest_visible(chars, index, -1)
        following = _nearest_visible(chars, index, 1)
        if (
            previous is not None
            and following is not None
            and _is_cjk(previous)
            and _is_cjk(following)
        ):
            continue  # CJK 之间的标点：直接删除，不引入空格
        kept.append(" ")
    return "".join(kept)


def _normalize_boundaries(text: str) -> str:
    """规范化文字与空白边界，消除 CJK 文本中无语义的空白差异。"""

    chars = list(text)
    result: list[str] = []
    for index, char in enumerate(chars):
        if char.isspace():
            previous = _nearest_visible(chars, index, -1)
            following = _nearest_visible(chars, index, 1)
            # 两侧都是 CJK：空白无语义，直接删除。
            if (
                previous is not None
                and following is not None
                and _is_cjk(previous)
                and _is_cjk(following)
            ):
                continue
            result.append(" ")
            continue
        if index > 0:
            previous = chars[index - 1]
            if (
                not previous.isspace()
                and _is_word_char_or_cjk(previous)
                and _is_word_char_or_cjk(char)
                and _is_cjk(previous) != _is_cjk(char)
            ):
                result.append(" ")  # CJK ↔ 拉丁/数字交界补空格
        result.append(char)
    return "".join(result)


def _is_word_char_or_cjk(char: str) -> bool:
    return char.isalnum()


def _nearest_visible(chars: list[str], index: int, step: int) -> str | None:
    """跳过空白找相邻的可见字符，用于判断标点/空白两侧文字类型。"""

    cursor = index + step
    while 0 <= cursor < len(chars):
        char = chars[cursor]
        if not char.isspace():
            return char
        cursor += step
    return None


def _is_cjk(char: str) -> bool:
    """判断字符是否属于按字切分的 CJK / 东亚表意文字区间。"""

    code = ord(char)
    return (
        0x4E00 <= code <= 0x9FFF  # CJK 统一表意文字
        or 0x3400 <= code <= 0x4DBF  # 扩展 A
        or 0x20000 <= code <= 0x2A6DF  # 扩展 B
        or 0x2A700 <= code <= 0x2EBEF  # 扩展 C–F
        or 0x3040 <= code <= 0x30FF  # 平假名 / 片假名
        or 0xAC00 <= code <= 0xD7AF  # 韩文音节
        or 0xF900 <= code <= 0xFAFF  # 兼容表意文字
    )


def _is_word_char(char: str) -> bool:
    """拉丁字母、数字等可组成连续 token 的字符。"""

    return char.isalnum() and not _is_cjk(char)


def tokenize(text: str, *, cjk_bigram: bool = True) -> list[str]:
    """把归一化后的文本切成 token 列表。

    - 连续拉丁字母/数字作为一个词（如 ``python3``、``2024``）。
    - 每个 CJK 字符是一个 unigram；``cjk_bigram=True`` 时相邻 CJK 字符
      额外生成 bigram（``天气`` → ``["天", "气", "天气"]``），bigram 对
      中文 SimHash 的语义稳定性至关重要：单字改一个只会影响少量特征。
    - 空白与标点（归一化阶段已转为空格）作为天然分隔符。
    """

    tokens: list[str] = []
    latin_buffer: list[str] = []
    cjk_run: list[str] = []

    def flush_latin() -> None:
        if latin_buffer:
            tokens.append("".join(latin_buffer))
            latin_buffer.clear()

    def flush_cjk() -> None:
        if not cjk_run:
            return
        tokens.extend(cjk_run)
        if cjk_bigram and len(cjk_run) >= 2:
            tokens.extend("".join(pair) for pair in zip(cjk_run, cjk_run[1:]))
        cjk_run.clear()

    for char in text:
        if char.isspace():
            flush_latin()
            flush_cjk()
        elif _is_cjk(char):
            flush_latin()
            cjk_run.append(char)
        elif _is_word_char(char):
            flush_cjk()
            latin_buffer.append(char)
        else:
            # 理论上归一化后不会出现标点，防御性兜底当作分隔符。
            flush_latin()
            flush_cjk()

    flush_latin()
    flush_cjk()
    return tokens
