"""布控引擎：对归一化后的记录逐条匹配规则，产出命中结果。

设计要点：

- 普通词（``type=word``）转义后按字面匹配，默认大小写不敏感；
  正则（``type=regex``）独立编译，默认区分大小写，可用 ``ignore_case`` 覆盖。
- 正则执行有**硬超时**：CPython 的正则回溯在 C 层持 GIL，线程 join
  无法中断灾难性回溯（实测会被阻塞到匹配自然结束），因此用
  ``signal.SIGALRM`` + ``setitimer`` 在指定秒数内打断。信号方案只在
  主线程、且平台提供 SIGALRM 时生效（当前管道串行跑在主线程）。
- 每条规则超时一次即**熔断**：本轮后续条目直接跳过该规则，避免病态
  规则反复拖慢采集；熔断事件只记日志。
- 单条规则的任何异常都被捕获并跳过，布控引擎自身不会让条目入库失败。
"""

from __future__ import annotations

import logging
import re
import signal
import threading
from dataclasses import dataclass

from crawl_flow.config import DEFAULT_WATCH_FIELDS, WatchRule, WatchSettings
from crawl_flow.errors import WatchRegexTimeout
from crawl_flow.models import Record

LOGGER_NAME = "crawl_flow.watch"
WHITESPACE_RUN = re.compile(r"\s+")

# SIGALRM 是进程级的，这里用模块级状态保存「当前正在计时的规则」，
# 保证引擎串行调用（同一时刻只有一条规则在匹配）时异常能带上规则名与超时值。
_active_rule_name: str | None = None
_active_timeout: float = 0.0


def _timeout_handler(signum: int, frame: object) -> None:
    raise WatchRegexTimeout(_active_rule_name or "<unknown>", _active_timeout)


@dataclass(frozen=True)
class RuleHit:
    """一次规则命中：命中位置与上下文片段。"""

    rule_name: str
    rule_type: str
    field: str
    start: int
    end: int
    matched: str
    context: str


class _CompiledRule:
    """编译后的规则，持有正则对象与作用字段。"""

    def __init__(self, rule: WatchRule, default_fields: tuple[str, ...]) -> None:
        self.name = rule.name
        self.rule_type = rule.type
        ignore_case = rule.effective_ignore_case()
        flags = re.IGNORECASE if ignore_case else 0
        raw_pattern = rule.pattern if rule.type == "regex" else re.escape(rule.pattern)
        self.regex = re.compile(raw_pattern, flags)
        self.ignore_case = ignore_case
        self.fields: tuple[str, ...] = tuple(rule.fields) if rule.fields else default_fields


def _supports_hard_timeout() -> bool:
    """SIGALRM 只在类 Unix 平台、且只能在主线程注册。"""

    return hasattr(signal, "SIGALRM") and threading.current_thread() is threading.main_thread()


class WatchEngine:
    """按源持有规则集的布控引擎。

    由 :func:`build_watch_engine` 构建；``scan_record`` 对一条归一化记录
    跑完该源的全部规则，返回命中列表，且保证不向外抛异常。
    """

    def __init__(
        self,
        settings: WatchSettings,
        rules_by_source: dict[str, list[_CompiledRule]],
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.settings = settings
        self._rules_by_source = rules_by_source
        self._tripped: set[str] = set()
        self._hard_timeout = _supports_hard_timeout()
        self.log = logger or logging.getLogger(LOGGER_NAME)
        if not self._hard_timeout:
            self.log.warning(
                "当前运行环境不支持 SIGALRM 硬超时（非主线程或非类 Unix 平台），"
                "正则超时保护退化为仅依赖正则自身终止，请避免使用高回溯风险规则"
            )

    def rules_for(self, source_name: str) -> tuple[_CompiledRule, ...]:
        return tuple(self._rules_by_source.get(source_name, ()))

    def scan_record(self, record: Record) -> list[RuleHit]:
        """扫描一条记录，返回所有命中；任何规则异常都只记日志并跳过。"""

        hits: list[RuleHit] = []
        for rule in self._rules_by_source.get(record.source, ()):
            if rule.name in self._tripped:
                continue
            for field_name in rule.fields:
                text = self._field_text(record, field_name)
                if not text:
                    continue
                try:
                    match = self._search(rule, text)
                except WatchRegexTimeout:
                    # 病态规则：熔断，本轮不再执行，避免拖死整轮采集。
                    self._tripped.add(rule.name)
                    self.log.warning(
                        "布控规则超时已熔断 source=%s rule=%s timeout_seconds=%.3f",
                        record.source,
                        rule.name,
                        self.settings.timeout_seconds,
                    )
                    break
                except Exception as exc:  # noqa: BLE001 - 布控异常绝不能影响条目入库
                    self.log.warning(
                        "布控规则执行异常，跳过该规则 source=%s rule=%s error=%s",
                        record.source,
                        rule.name,
                        exc,
                    )
                    break
                if match is None:
                    continue
                hits.append(
                    RuleHit(
                        rule_name=rule.name,
                        rule_type=rule.rule_type,
                        field=field_name,
                        start=match.start(),
                        end=match.end(),
                        matched=_compact(match.group(0)),
                        context=_build_context(
                            text, match.start(), match.end(), self.settings.context_chars
                        ),
                    )
                )
        return hits

    def _search(self, rule: _CompiledRule, text: str) -> re.Match[str] | None:
        # 普通词已经转义成字面正则，不存在回溯风险，无需计时；
        # 只有用户写的 regex 类型需要硬超时保护。
        if rule.rule_type != "regex" or not self._hard_timeout:
            return rule.regex.search(text)

        global _active_rule_name, _active_timeout
        previous_handler = signal.getsignal(signal.SIGALRM)
        _active_rule_name = rule.name
        _active_timeout = float(self.settings.timeout_seconds)
        signal.signal(signal.SIGALRM, _timeout_handler)
        # setitimer 支持小数秒；计时到点由 _timeout_handler 抛 WatchRegexTimeout。
        signal.setitimer(signal.ITIMER_REAL, _active_timeout)
        try:
            return rule.regex.search(text)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous_handler)
            _active_rule_name = None
            _active_timeout = 0.0

    @staticmethod
    def _field_text(record: Record, field_name: str) -> str:
        if field_name == "tags":
            return " ".join(record.tags)
        value = getattr(record, field_name, None)
        return value if isinstance(value, str) else ""


def _compact(text: str) -> str:
    """把命中文本里的换行/连续空白压成空格，便于表格与日志展示。"""

    return WHITESPACE_RUN.sub(" ", text).strip()


def _build_context(text: str, start: int, end: int, context_chars: int) -> str:
    """截取命中位置前后各 ``context_chars`` 个字符的上下文。"""

    left = max(0, start - context_chars)
    right = min(len(text), end + context_chars)
    snippet = text[left:right]
    prefix = "…" if left > 0 else ""
    suffix = "…" if right < len(text) else ""
    return prefix + _compact(snippet) + suffix


def build_watch_engine(
    settings: WatchSettings,
    sources_rules: dict[str, list[WatchRule]],
    *,
    logger: logging.Logger | None = None,
) -> WatchEngine | None:
    """根据全局设置与各源规则构建引擎；布控关闭或没有任何规则时返回 ``None``。"""

    if not settings.enabled:
        return None

    default_fields = tuple(DEFAULT_WATCH_FIELDS)
    compiled_by_source: dict[str, list[_CompiledRule]] = {}
    has_any_rule = False
    for source_name, rules in sources_rules.items():
        compiled = [_CompiledRule(rule, default_fields) for rule in rules]
        if compiled:
            has_any_rule = True
        compiled_by_source[source_name] = compiled
    if not has_any_rule:
        return None
    return WatchEngine(settings, compiled_by_source, logger=logger)
