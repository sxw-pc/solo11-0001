"""指标汇总与 Prometheus 文本格式渲染。

指标数据全部来自 SQLite 的 ``job_runs`` 表（手动与定时触发共用同一份存储，
因此两种触发方式的统计天然累加、互不冲突）；调度器内存状态（任务数、
下次触发时间等）由调度进程在渲染时额外注入。

渲染结果是 Prometheus 文本 exposition 格式（``text/plain; version=0.0.4``），
可被 Prometheus / VictoriaMetrics 等主流监控系统直接拉取。输出只包含
源名称与数值指标，不含 base_url、文件路径等任何配置信息。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def render_metrics(
    aggregates: Mapping[str, Any],
    *,
    scheduler_state: Iterable[Mapping[str, Any]] | None = None,
) -> str:
    """把 :meth:`Database.job_run_aggregates` 的结果渲染为 Prometheus 文本。

    ``scheduler_state`` 为调度器每个任务的快照（可选），键：``source``、
    ``running``、``next_fire``。
    """

    lines: list[str] = []

    def metric(
        name: str,
        help_text: str,
        metric_type: str,
        samples: Iterable[tuple[dict[str, str], float | int]],
    ) -> None:
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")
        for labels, value in samples:
            lines.append(f"{name}{_format_labels(labels)} {value}")

    metric(
        "crawl_flow_runs_total",
        "Total number of collection runs by source, trigger and status.",
        "counter",
        (
            ({"source": row["source"], "trigger": row["trigger"], "status": row["status"]}, row["count"])
            for row in aggregates["runs"]
        ),
    )
    metric(
        "crawl_flow_items_total",
        "Total number of items processed by source and item status.",
        "counter",
        (
            ({"source": row["source"], "status": status}, row[status])
            for row in aggregates["items"]
            for status in ("ok", "failed", "skipped")
        ),
    )
    metric(
        "crawl_flow_run_duration_seconds_avg",
        "Average duration of executed runs (excludes skipped runs).",
        "gauge",
        (
            ({"source": row["source"]}, round(row["avg_ms"] / 1000.0, 6))
            for row in aggregates["durations"]
        ),
    )
    metric(
        "crawl_flow_run_last_success",
        "Whether the most recent executed run succeeded (1) or failed (0).",
        "gauge",
        (
            ({"source": row["source"]}, 1 if row["status"] == "succeeded" else 0)
            for row in aggregates["last_executed"]
        ),
    )
    metric(
        "crawl_flow_run_last_timestamp_seconds",
        "Unix timestamp when the most recent executed run finished.",
        "gauge",
        (
            ({"source": row["source"]}, _epoch_seconds(row["finished_at"]))
            for row in aggregates["last_executed"]
            if row["finished_at"] is not None
        ),
    )

    if scheduler_state is not None:
        jobs = list(scheduler_state)
        metric(
            "crawl_flow_scheduler_jobs",
            "Number of active scheduled jobs in this process.",
            "gauge",
            [({}, len(jobs))],
        )
        metric(
            "crawl_flow_scheduler_job_running",
            "Whether a scheduled run of the source is currently executing.",
            "gauge",
            (
                ({"source": job["source"]}, 1 if job["running"] else 0)
                for job in jobs
            ),
        )
        metric(
            "crawl_flow_scheduler_next_fire_timestamp_seconds",
            "Unix timestamp of the next scheduled fire time per source.",
            "gauge",
            (
                ({"source": job["source"]}, _epoch_seconds(job["next_fire"]))
                for job in jobs
                if job["next_fire"] is not None
            ),
        )

    return "\n".join(lines) + "\n"


def summarize_sources(aggregates: Mapping[str, Any]) -> list[dict[str, Any]]:
    """按源汇总核心指标：执行次数、成功率、平均耗时、最近一次状态。

    成功率只统计真正执行了的轮次（succeeded / failed），调度跳过不计入。
    """

    per_source: dict[str, dict[str, Any]] = {}

    def bucket(source: str) -> dict[str, Any]:
        return per_source.setdefault(
            source,
            {
                "source": source,
                "runs": 0,
                "succeeded": 0,
                "failed": 0,
                "skipped": 0,
                "items_ok": 0,
                "items_failed": 0,
                "items_skipped": 0,
                "avg_ms": None,
                "last_started_at": None,
                "last_status": None,
            },
        )

    for row in aggregates["runs"]:
        entry = bucket(row["source"])
        count = int(row["count"])
        entry["runs"] += count
        if row["status"] in entry:
            entry[row["status"]] += count
    for row in aggregates["items"]:
        entry = bucket(row["source"])
        entry["items_ok"] = row["ok"]
        entry["items_failed"] = row["failed"]
        entry["items_skipped"] = row["skipped"]
    for row in aggregates["durations"]:
        bucket(row["source"])["avg_ms"] = row["avg_ms"]
    for row in aggregates["last"]:
        entry = bucket(row["source"])
        entry["last_started_at"] = row["started_at"]
        entry["last_status"] = row["status"]

    for entry in per_source.values():
        executed = entry["succeeded"] + entry["failed"]
        entry["success_rate"] = (entry["succeeded"] / executed) if executed else None
    return sorted(per_source.values(), key=lambda item: item["source"])


def _format_labels(labels: Mapping[str, str]) -> str:
    if not labels:
        return ""
    pairs = ",".join(f'{key}="{_escape_label_value(value)}"' for key, value in labels.items())
    return "{" + pairs + "}"


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _epoch_seconds(moment: datetime) -> int:
    if moment.tzinfo is None:
        # SQLite 读回的时间不带时区；本项目的写入统一为 UTC。
        moment = moment.replace(tzinfo=timezone.utc)
    return int(moment.timestamp())
