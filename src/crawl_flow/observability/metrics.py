"""轻量指标采集与 Prometheus 文本格式暴露。

设计原则：

- 计数器与直方图持久化在 SQLite 中，调度器长驻进程与手动 CLI 进程共享
  同一套指标，任一进程累加都不会丢失或互相覆盖（upsert 原子自增）。
- Gauge 是进程内瞬时值（当前运行任务数、调度器存活状态），不落库。
- 渲染遵循 Prometheus 文本 exposition 格式（0.0.4 兼容写法），主流监控
  系统（Prometheus / VictoriaMetrics 等）可直接拉取。
- 标签只允许出现源名称、触发方式、状态等运维字段，严禁写入 URL、
  token、请求头等敏感配置。
"""

from __future__ import annotations

import json
import threading
from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, sessionmaker

from crawl_flow.scheduler.models import (
    MetricCounterRow,
    MetricHistogramBucketRow,
    MetricHistogramSumRow,
)

# 耗时直方图桶边界（毫秒），覆盖亚秒级到 30 秒的采集任务
DURATION_BUCKETS_MS: tuple[int, ...] = (100, 300, 1000, 3000, 10000, 30000)

# ---- 指标名称与 HELP 文案集中管理，避免各调用方拼写漂移 ----
METRIC_RUNS_TOTAL = "crawlflow_collection_runs_total"
METRIC_ITEMS_TOTAL = "crawlflow_collection_items_total"
METRIC_TRIGGERS_TOTAL = "crawlflow_scheduler_triggers_total"
METRIC_DURATION_MS = "crawlflow_collection_duration_milliseconds"
METRIC_RUNNING_GAUGE = "crawlflow_scheduler_running_jobs"
METRIC_UP_GAUGE = "crawlflow_scheduler_up"

HELP_TEXT: dict[str, str] = {
    METRIC_RUNS_TOTAL: "采集任务执行总次数，按来源/触发方式/结果区分",
    METRIC_ITEMS_TOTAL: "采集处理的记录总条数，按来源/触发方式/结果区分",
    METRIC_TRIGGERS_TOTAL: "调度器触发总次数，fired=实际执行 skipped=因上一轮未完成跳过",
    METRIC_DURATION_MS: "采集任务执行耗时分布（毫秒）",
    METRIC_RUNNING_GAUGE: "当前正在执行的采集任务数量（进程内瞬时值）",
    METRIC_UP_GAUGE: "调度器进程存活标识，1=存活（进程内瞬时值）",
}

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def canonical_labels(labels: dict[str, str] | None) -> str:
    """把标签字典按 key 排序后 JSON 序列化，作为数据库里的稳定键。"""

    if not labels:
        return "{}"
    return json.dumps(dict(sorted(labels.items())), ensure_ascii=False, separators=(",", ":"))


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _render_labels(labels_json: str) -> str:
    labels = json.loads(labels_json)
    if not labels:
        return ""
    parts = [f'{key}="{_escape_label_value(str(value))}"' for key, value in sorted(labels.items())]
    return "{" + ",".join(parts) + "}"


class MetricsRegistry:
    """线程安全的指标注册表，支持多进程共享同一 SQLite 文件。"""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory
        self._lock = threading.RLock()
        # Gauge 仅保存在当前进程内存：(指标名, 标签 JSON) -> (值, HELP)
        self._gauges: dict[tuple[str, str], tuple[float, str]] = {}

    # ---- 写入 ----
    def inc_counter(
        self,
        metric: str,
        labels: dict[str, str] | None = None,
        amount: int = 1,
    ) -> None:
        """计数器原子自增；新序列自动创建。"""

        labels_json = canonical_labels(labels)
        with self._lock, self._session_factory() as session:
            statement = sqlite_insert(MetricCounterRow).values(
                metric=metric,
                labels=labels_json,
                value=amount,
                help_text=HELP_TEXT.get(metric, ""),
            )
            statement = statement.on_conflict_do_update(
                index_elements=["metric", "labels"],
                set_={
                    "value": MetricCounterRow.value + amount,
                    "help_text": HELP_TEXT.get(metric, ""),
                },
            )
            session.execute(statement)
            session.commit()

    def observe_histogram(
        self,
        metric: str,
        value_ms: int,
        labels: dict[str, str] | None = None,
    ) -> None:
        """记录一次耗时观测，累计分桶、计数与总和。"""

        labels_json = canonical_labels(labels)
        with self._lock, self._session_factory() as session:
            for boundary in (*DURATION_BUCKETS_MS, None):
                # None 表示 +Inf 桶，所有观测都落入
                if boundary is not None and value_ms > boundary:
                    continue
                statement = sqlite_insert(MetricHistogramBucketRow).values(
                    metric=metric,
                    labels=labels_json,
                    bucket=boundary,
                    cumulative_count=1,
                )
                statement = statement.on_conflict_do_update(
                    index_elements=["metric", "labels", "bucket"],
                    set_={"cumulative_count": MetricHistogramBucketRow.cumulative_count + 1},
                )
                session.execute(statement)

            sum_statement = sqlite_insert(MetricHistogramSumRow).values(
                metric=metric,
                labels=labels_json,
                observation_count=1,
                sum_ms=value_ms,
                help_text=HELP_TEXT.get(metric, ""),
            )
            sum_statement = sum_statement.on_conflict_do_update(
                index_elements=["metric", "labels"],
                set_={
                    "observation_count": MetricHistogramSumRow.observation_count + 1,
                    "sum_ms": MetricHistogramSumRow.sum_ms + value_ms,
                    "help_text": HELP_TEXT.get(metric, ""),
                },
            )
            session.execute(sum_statement)
            session.commit()

    def set_gauge(
        self,
        metric: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        """设置进程内 Gauge 瞬时值。"""

        with self._lock:
            self._gauges[(metric, canonical_labels(labels))] = (value, HELP_TEXT.get(metric, ""))

    # ---- 读取 / 渲染 ----
    def snapshot_gauges(self) -> dict[tuple[str, str], tuple[float, str]]:
        with self._lock:
            return dict(self._gauges)

    def render_prometheus(self) -> str:
        """渲染完整 Prometheus 文本 exposition（含 HELP/TYPE，序列稳定排序）。"""

        with self._lock, self._session_factory() as session:
            counter_rows = list(
                session.execute(
                    select(MetricCounterRow).order_by(
                        MetricCounterRow.metric, MetricCounterRow.labels
                    )
                ).scalars()
            )
            bucket_rows = list(
                session.execute(
                    select(MetricHistogramBucketRow).order_by(
                        MetricHistogramBucketRow.metric,
                        MetricHistogramBucketRow.labels,
                    )
                ).scalars()
            )
            sum_rows = list(
                session.execute(select(MetricHistogramSumRow)).scalars()
            )

        lines: list[str] = []
        self._render_counters(lines, counter_rows)
        self._render_histograms(lines, bucket_rows, sum_rows)
        self._render_gauges(lines)
        return "".join(lines)

    def _render_counters(
        self, lines: list[str], rows: list[MetricCounterRow]
    ) -> None:
        grouped: dict[str, list[MetricCounterRow]] = defaultdict(list)
        for row in rows:
            grouped[row.metric].append(row)
        for metric in sorted(grouped):
            help_text = grouped[metric][0].help_text or HELP_TEXT.get(metric, "")
            lines.append(f"# HELP {metric} {help_text}\n")
            lines.append(f"# TYPE {metric} counter\n")
            for row in sorted(grouped[metric], key=lambda item: item.labels):
                lines.append(
                    f"{metric}{_render_labels(row.labels)} {int(row.value)}\n"
                )

    def _render_histograms(
        self,
        lines: list[str],
        bucket_rows: list[MetricHistogramBucketRow],
        sum_rows: list[MetricHistogramSumRow],
    ) -> None:
        # (metric, labels) -> {bucket_boundary: cumulative}，None 即 +Inf
        buckets: dict[tuple[str, str], dict[int | None, int]] = defaultdict(dict)
        for row in bucket_rows:
            buckets[(row.metric, row.labels)][row.bucket] = row.cumulative_count

        sums = {(row.metric, row.labels): row for row in sum_rows}
        for metric, labels_json in sorted(buckets):
            help_text = sums.get((metric, labels_json)).help_text if (metric, labels_json) in sums else ""
            lines.append(f"# HELP {metric} {help_text or HELP_TEXT.get(metric, '')}\n")
            lines.append(f"# TYPE {metric} histogram\n")
            series_buckets = buckets[(metric, labels_json)]
            rendered_labels = _render_labels(labels_json)
            prefix_labels = _strip_outer_braces(rendered_labels)
            for boundary in (*DURATION_BUCKETS_MS, None):
                le_value = "+Inf" if boundary is None else str(boundary)
                count = series_buckets.get(boundary, 0)
                lines.append(
                    f"{metric}_bucket{_with_le(prefix_labels, le_value)} {count}\n"
                )
            sum_row = sums.get((metric, labels_json))
            total_count = sum_row.observation_count if sum_row else 0
            total_sum = sum_row.sum_ms if sum_row else 0
            lines.append(f"{metric}_sum{rendered_labels} {int(total_sum)}\n")
            lines.append(f"{metric}_count{rendered_labels} {int(total_count)}\n")

    def _render_gauges(self, lines: list[str]) -> None:
        snapshot = self.snapshot_gauges()
        grouped: dict[str, list[tuple[str, float, str]]] = defaultdict(list)
        for (metric, labels_json), (value, help_text) in snapshot.items():
            grouped[metric].append((labels_json, value, help_text))
        for metric in sorted(grouped):
            help_text = grouped[metric][0][2] or HELP_TEXT.get(metric, "")
            lines.append(f"# HELP {metric} {help_text}\n")
            lines.append(f"# TYPE {metric} gauge\n")
            for labels_json, value, _ in sorted(grouped[metric], key=lambda item: item[0]):
                lines.append(f"{metric}{_render_labels(labels_json)} {_format_number(value)}\n")


def _strip_outer_braces(rendered: str) -> str:
    """``{a="b"}`` -> ``a="b"``；空标签返回空串，便于追加 le 标签。"""

    if rendered == "":
        return ""
    return rendered[1:-1]


def _with_le(inner_labels: str, le_value: str) -> str:
    le_part = f'le="{le_value}"'
    if not inner_labels:
        return "{" + le_part + "}"
    return "{" + inner_labels + "," + le_part + "}"


def _format_number(value: float) -> str:
    if float(value).is_integer():
        return str(int(value))
    return repr(float(value))


def make_registry(session_factory: sessionmaker[Session]) -> MetricsRegistry:
    """工厂函数，便于测试与调用方按 session 工厂构造。"""

    return MetricsRegistry(session_factory)
