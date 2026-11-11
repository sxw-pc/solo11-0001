"""布控配置与引擎单元测试：启动校验、词/正则匹配、超时熔断、异常隔离。"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import pytest

from crawl_flow.config import WatchRule, WatchSettings, load_app_config
from crawl_flow.errors import ConfigError
from crawl_flow.models import Record
from crawl_flow.watch.engine import WatchEngine, build_watch_engine


def _rule(**overrides: Any) -> WatchRule:
    payload: dict[str, Any] = {"name": "r1", "type": "word", "pattern": "acme"}
    payload.update(overrides)
    return WatchRule.model_validate(payload)


def _record(
    *,
    source: str = "demo_api",
    external_id: str = "api-1",
    title: str = "Acme 发布新品",
    content: str = "正文提到 acme 品牌与其他内容。",
    tags: list[str] | None = None,
    category: str | None = "技术",
) -> Record:
    return Record(
        source=source,
        external_id=external_id,
        title=title,
        content=content,
        tags=tags or [],
        category=category,
        url="http://example.com/i",
        fetched_at=None,
    )


def _engine(rules: list[WatchRule], **settings: Any) -> WatchEngine:
    config = WatchSettings.model_validate({"rules": [], **settings})
    engine = build_watch_engine(config, {"demo_api": rules})
    assert engine is not None
    return engine


# ---------- 配置：启动即校验 ----------


def test_invalid_regex_is_rejected_at_load(write_config: Any, source_payload: Any) -> None:
    config_path = write_config([source_payload("demo_api")], filename="sources.yaml")
    text = config_path.read_text(encoding="utf-8")
    text += (
        "\nwatch:\n  rules:\n"
        '    - name: bad_rule\n      type: regex\n      pattern: "("  # 未闭合分组\n'
    )
    config_path.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError) as exc_info:
        load_app_config(config_path)
    assert "bad_rule" in str(exc_info.value)


def test_invalid_regex_in_source_watch_is_rejected_at_load(
    write_config: Any, source_payload: Any
) -> None:
    payload = source_payload("demo_api")
    payload["watch"] = {"rules": [{"name": "bad", "type": "regex", "pattern": "[a-"}]}
    config_path = write_config([payload])

    with pytest.raises(ConfigError):
        load_app_config(config_path)


def test_unknown_watch_field_fails(write_config: Any, source_payload: Any) -> None:
    config_path = write_config([source_payload("demo_api")])
    text = config_path.read_text(encoding="utf-8")
    text += (
        "\nwatch:\n  rules:\n"
        '    - name: r1\n      type: word\n      pattern: acme\n      bogus: 1\n'
    )
    config_path.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError):
        load_app_config(config_path)


def test_duplicate_rule_name_across_global_and_source_fails(
    write_config: Any, source_payload: Any
) -> None:
    payload = source_payload("demo_api")
    payload["watch"] = {"rules": [{"name": "shared", "type": "word", "pattern": "x"}]}
    config_path = write_config([payload])
    text = config_path.read_text(encoding="utf-8")
    text += (
        "\nwatch:\n  rules:\n"
        '    - name: shared\n      type: word\n      pattern: y\n'
    )
    config_path.write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError, match="规则名称重复"):
        load_app_config(config_path)


def test_rule_case_defaults() -> None:
    assert _rule(type="word").effective_ignore_case() is True
    assert _rule(type="regex", pattern="A.+").effective_ignore_case() is False
    regex_cs = WatchRule.model_validate(
        {"name": "r", "type": "regex", "pattern": "a", "ignore_case": False}
    )
    assert regex_cs.effective_ignore_case() is False
    regex_ci = WatchRule.model_validate(
        {"name": "r", "type": "regex", "pattern": "a", "ignore_case": True}
    )
    assert regex_ci.effective_ignore_case() is True


def test_shipped_config_loads() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    config = load_app_config(repo_root / "config" / "sources.yaml")
    assert {rule.name for rule in config.watch.rules} == {
        "brand_database",
        "metro_flow_regex",
    }


# ---------- 引擎：普通词 / 正则匹配 ----------


def test_word_rule_is_case_insensitive_by_default() -> None:
    engine = _engine([_rule()])
    hits = engine.scan_record(_record())
    assert {hit.field for hit in hits} == {"title", "content"}
    assert all(hit.rule_name == "r1" and hit.matched.lower() == "acme" for hit in hits)


def test_word_rule_respects_ignore_case_false() -> None:
    # 大小写敏感时，小写规则只能命中小写文本，标题里的大写 ACME 不命中
    engine = _engine([_rule(ignore_case=False)])
    hits = engine.scan_record(
        _record(title="大写 ACME 标题", content="这里有小写 acme 品牌词")
    )
    assert [hit.field for hit in hits] == ["content"]
    assert hits[0].matched == "acme"


def test_regex_rule_matches_pattern() -> None:
    rule = WatchRule.model_validate(
        {"name": "metro", "type": "regex", "pattern": r"地铁.{0,6}客流"}
    )
    engine = _engine([rule])
    hits = engine.scan_record(_record(title="地铁早高峰客流变化观察", content="无"))
    assert len(hits) == 1
    assert hits[0].matched == "地铁早高峰客流"
    assert hits[0].field == "title"


def test_fields_option_limits_scan_scope() -> None:
    engine = _engine([_rule(fields=["content"])])
    hits = engine.scan_record(_record(title="Acme 在标题", content="正文没有品牌词"))
    assert hits == []


def test_tags_field_is_scannable() -> None:
    rule = WatchRule.model_validate(
        {"name": "tag", "type": "word", "pattern": "图书馆", "fields": ["tags"]}
    )
    engine = _engine([rule])
    hits = engine.scan_record(_record(title="无", content="无", tags=["社区图书馆", "借阅"]))
    assert len(hits) == 1
    assert hits[0].field == "tags"
    assert hits[0].matched == "图书馆"


def test_context_is_clipped_around_match() -> None:
    engine = _engine([_rule(pattern="中心词")], context_chars=5)
    record = _record(
        title="一二三四五六七八九十" + "中心词" + "子丑寅卯辰巳午未申酉",
        content="",
    )
    hits = engine.scan_record(record)
    assert len(hits) == 1
    hit = hits[0]
    assert "中心词" in hit.context
    assert hit.context.startswith("…") and hit.context.endswith("…")
    # 前后各 5 个字符 + 命中词本身（3 个字符），再加两个省略号
    assert len(hit.context) == 5 + 3 + 5 + 2


def test_no_hit_returns_empty() -> None:
    engine = _engine([_rule(pattern="不存在的品牌词")])
    assert engine.scan_record(_record()) == []


def test_disabled_or_empty_watch_returns_none() -> None:
    assert build_watch_engine(WatchSettings(enabled=False), {"demo_api": [_rule()]}) is None
    assert build_watch_engine(WatchSettings(), {"demo_api": []}) is None


def test_source_without_rules_is_unaffected() -> None:
    engine = _engine([_rule()])
    assert engine.scan_record(_record(source="other_source")) == []


# ---------- 引擎：正则硬超时与熔断 ----------


def test_catastrophic_regex_times_out_and_trips() -> None:
    rule = WatchRule.model_validate(
        {"name": "evil", "type": "regex", "pattern": r"(a+)+$"}
    )
    engine = _engine([rule], timeout_seconds=0.2)
    evil_text = "a" * 30 + "!"

    started = time.perf_counter()
    first = engine.scan_record(_record(content=evil_text))
    elapsed = time.perf_counter() - started

    # 无保护的回溯在该输入上需要数秒；硬超时必须在亚秒级打断。
    assert first == []
    assert elapsed < 2.0
    assert "evil" in engine._tripped

    # 熔断后同轮后续条目直接跳过，不再付出回溯成本。
    started = time.perf_counter()
    second = engine.scan_record(_record(external_id="api-2", content=evil_text))
    assert second == []
    assert time.perf_counter() - started < 0.1


def test_word_rule_never_hangs_on_repetitive_input() -> None:
    engine = _engine([_rule(pattern="a" * 20)], timeout_seconds=0.2)
    hits = engine.scan_record(_record(content="a" * 25 + "!"))
    assert len(hits) == 1


# ---------- 引擎：单规则异常隔离 ----------


def test_rule_exception_is_logged_and_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _BoomPattern:
        def search(self, text: str) -> Any:
            raise RuntimeError("模拟编译规则对象故障")

    good = _rule(name="good", pattern="acme")
    broken = _rule(name="broken")
    engine = _engine([broken, good])
    engine._rules_by_source["demo_api"][0].regex = _BoomPattern()  # type: ignore[assignment]

    with caplog.at_level(logging.WARNING, logger="crawl_flow.watch"):
        hits = engine.scan_record(_record())

    assert [hit.rule_name for hit in hits] == ["good", "good"]
    assert any("布控规则执行异常" in record.message for record in caplog.records)
    # 非超时异常不熔断，下一条仍会尝试（但同样失败并跳过）
    assert "broken" not in engine._tripped


def test_scan_record_never_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _engine([_rule()])

    def boom(record: Record) -> Any:
        raise RuntimeError("引擎内部意外")

    monkeypatch.setattr(engine, "scan_record", boom)
    # 管道层的兜底函数也要保证不抛
    from crawl_flow.pipeline.run import _scan_watch

    assert _scan_watch(engine, _record(), logger=logging.getLogger("test")) == []
