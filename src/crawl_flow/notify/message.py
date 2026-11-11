"""通知消息体构建：把 :class:`RunSummary` 汇总成一条 JSON 可序列化的字典。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlsplit

from crawl_flow.config import NotifySettings
from crawl_flow.pipeline.run import RunSummary

LEVEL_INFO = "info"
LEVEL_ALERT = "alert"
SCOPE_ROUND = "round"
SCOPE_SOURCE = "source"
_ROUND_FAILURE_RATE_PRECISION = 4


def failure_rate(ok: int, failed: int) -> float:
    """失败率 = failed / (ok + failed)；没有任何处理结果时按 0 处理。

    skipped 不计入分母：跳过的记录没有被尝试，不代表失败。
    """

    attempted = ok + failed
    if attempted == 0:
        return 0.0
    return round(failed / attempted, _ROUND_FAILURE_RATE_PRECISION)


def redact_webhook_url(url: str) -> str:
    """脱敏 webhook 地址：隐藏 userinfo 与路径/查询中的密钥，只留协议与主机端口。

    例如 ``https://robot:token@im.example.com/hooks/secret/abc?key=x``
    变为 ``https://im.example.com/***``。
    """

    try:
        parts = urlsplit(url)
    except ValueError:
        return "***"
    host = parts.hostname or ""
    if parts.port:
        host = f"{host}:{parts.port}"
    if not parts.scheme or not host:
        return "***"
    return f"{parts.scheme}://{host}/***"


def _source_stats(
    summary: RunSummary, threshold: float
) -> dict[str, Any]:
    rate = failure_rate(summary.ok, summary.failed)
    return {
        "source": summary.source,
        "ok": summary.ok,
        "failed": summary.failed,
        "skipped": summary.skipped,
        "elapsed_ms": summary.elapsed_ms,
        "failure_rate": rate,
        "level": LEVEL_ALERT if rate > threshold else LEVEL_INFO,
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_round_payload(
    summaries: list[RunSummary],
    settings: NotifySettings,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """构建本轮全量汇总消息（推送给全局 webhook）。"""

    sources = [
        _source_stats(summary, settings.alert_failure_rate)
        for summary in summaries
    ]
    total_ok = sum(summary.ok for summary in summaries)
    total_failed = sum(summary.failed for summary in summaries)
    total_skipped = sum(summary.skipped for summary in summaries)
    total_elapsed_ms = sum(summary.elapsed_ms for summary in summaries)
    total_rate = failure_rate(total_ok, total_failed)
    any_source_alert = any(item["level"] == LEVEL_ALERT for item in sources)
    return {
        "scope": SCOPE_ROUND,
        "generated_at": _now_iso(),
        "dry_run": dry_run,
        "level": LEVEL_ALERT
        if any_source_alert or total_rate > settings.alert_failure_rate
        else LEVEL_INFO,
        "alert_failure_rate": settings.alert_failure_rate,
        "total": {
            "ok": total_ok,
            "failed": total_failed,
            "skipped": total_skipped,
            "elapsed_ms": total_elapsed_ms,
            "failure_rate": total_rate,
        },
        "sources": sources,
    }


def build_source_payload(
    summary: RunSummary,
    settings: NotifySettings,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """构建单源汇总消息（推送给该源自己覆盖的 webhook）。"""

    stats = _source_stats(summary, settings.alert_failure_rate)
    return {
        "scope": SCOPE_SOURCE,
        "generated_at": _now_iso(),
        "dry_run": dry_run,
        "level": stats["level"],
        "alert_failure_rate": settings.alert_failure_rate,
        "total": {
            "ok": stats["ok"],
            "failed": stats["failed"],
            "skipped": stats["skipped"],
            "elapsed_ms": stats["elapsed_ms"],
            "failure_rate": stats["failure_rate"],
        },
        "sources": [stats],
    }
