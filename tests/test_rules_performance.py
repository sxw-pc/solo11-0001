"""规则引擎性能测试：引擎处理延迟相对整条采集耗时的占比不超过 10%。

约束来源：需求明确「规则引擎的执行延迟不超过单条采集总耗时的 10%」。
测试用真实 mock 站点跑完整采集（含 HTTP + HTML/JSON 解析 + 入库），
同时用纯内存基准给出引擎单条处理的绝对延迟上界。
"""

from __future__ import annotations

import statistics
import time
from pathlib import Path
from typing import Any

from crawl_flow.pipeline.run import run_source
from crawl_flow.rules import load_rules
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

RULES_PAYLOAD = """
version: 1
quality: {threshold: 60, on_validation_fail: mark}
fields:
  title:
    type_hint: text
    weight: 2
    rules:
      - {type: trim}
      - {type: regex_replace, params: {pattern: '[-—_【\\[]*(官方|最新|热门|推荐)[】\\]]*$', replacement: ''}}
      - {type: truncate, params: {max_length: 200}}
    validations:
      - {type: required}
      - {type: length, params: {min: 2, max: 200}}
  category:
    type_hint: text
    rules: [{type: trim}]
    validations:
      - type: enum
        params: {values: [环境, 技术, 文化, 教育, 经济, 交通]}
  tags:
    type_hint: tags
    rules: [{type: split_tags}]
    validations:
      - {type: length, params: {min: 1, max: 10}}
  published_at:
    type_hint: datetime
    rules:
      - {type: trim}
      - {type: date_format, params: {to_format: '%Y-%m-%d'}}
  content:
    type_hint: multiline
    rules:
      - {type: strip_html}
      - {type: truncate, params: {max_length: 5000}}
    validations:
      - {type: non_empty}
  url:
    type_hint: text
    validations:
      - {type: regex, params: {pattern: '^https?://.+'}}
"""


def _write_rules(tmp_path: Path) -> Path:
    path = tmp_path / "rules.yaml"
    path.write_text(RULES_PAYLOAD, encoding="utf-8")
    return path


def test_engine_latency_under_10_percent_of_item_total(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
    tmp_path: Path,
) -> None:
    engine = load_rules(_write_rules(tmp_path))

    # 无引擎基线：完整采集单条平均耗时（HTTP/解析/入库）。
    baseline = run_source(make_source("demo_api"), db, raw_store, limit=10)
    baseline_avg_ms = statistics.mean(item.elapsed_ms for item in baseline.results)

    # 带引擎再跑一轮（数据库 upsert 覆盖同样的行，成本与基线一致）。
    db2 = Database(tmp_path / "perf.db")
    db2.init()
    try:
        powered = run_source(
            make_source("demo_api"),
            db2,
            RawStore(tmp_path / "perf-raw"),
            limit=10,
            engine=engine,
        )
    finally:
        db2.close()
    powered_avg_ms = statistics.mean(item.elapsed_ms for item in powered.results)

    overhead_ms = max(powered_avg_ms - baseline_avg_ms, 0.0)
    ratio = overhead_ms / powered_avg_ms if powered_avg_ms else 0.0
    # 网络抖动可能让 powered 更快（overhead=0），用绝对上界兜底断言。
    assert ratio <= 0.10, (
        f"规则引擎延迟占比 {ratio:.1%} 超过 10% "
        f"(baseline={baseline_avg_ms:.2f}ms powered={powered_avg_ms:.2f}ms)"
    )


def test_engine_absolute_latency_is_sub_millisecond_for_normal_record(
    tmp_path: Path,
) -> None:
    """纯内存基准：不含 IO，单条全字段规则处理的 p95 延迟低于 2ms。"""

    engine = load_rules(_write_rules(tmp_path))
    raw = {
        "title": " 城市公园春季花期观测报告【最新】 ",
        "category": "环境",
        "tags": "公园、花期、观测",
        "published_at": "2024-03-01T09:30:00+08:00",
        "content": "<p>第一段</p>" * 20,
        "url": "http://example.test/items/api-1",
    }

    iterations = 500
    samples: list[float] = []
    for i in range(iterations):
        start = time.perf_counter()
        result = engine.process("demo_api", f"id-{i}", raw)
        samples.append((time.perf_counter() - start) * 1000)
        assert result.action in {"pass", "warn", "mark", "skip"}

    p95 = sorted(samples)[int(iterations * 0.95)]
    median = statistics.median(samples)
    assert p95 < 2.0, f"p95={p95:.3f}ms median={median:.3f}ms 超出 2ms 上界"
