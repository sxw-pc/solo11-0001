"""通知模块：消息体、脱敏、重试、CLI 端到端与「不影响退出码」保证。"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.config import NotifySettings
from crawl_flow.fetch.types import FetchResult
from crawl_flow.notify.message import (
    LEVEL_ALERT,
    LEVEL_INFO,
    build_round_payload,
    build_source_payload,
    failure_rate,
    redact_webhook_url,
)
from crawl_flow.notify.notifier import Notifier
from crawl_flow.pipeline.run import RunSummary

runner = CliRunner()


def _summary(source: str, ok: int, failed: int, skipped: int, elapsed_ms: int = 12) -> RunSummary:
    return RunSummary(
        source=source, ok=ok, failed=failed, skipped=skipped, elapsed_ms=elapsed_ms
    )


def _settings(**overrides: Any) -> NotifySettings:
    return NotifySettings.model_validate(overrides)


# ---------- 消息体构建 ----------


def test_failure_rate_excludes_skipped_and_handles_zero() -> None:
    assert failure_rate(8, 2) == 0.2
    assert failure_rate(0, 0) == 0.0
    # 全部跳过（没有任何尝试）不算失败
    assert failure_rate(0, 0) == 0.0


def test_round_payload_fields_complete_and_info_level() -> None:
    settings = _settings(webhooks=["http://127.0.0.1/hook/a"], alert_failure_rate=0.5)
    payload = build_round_payload(
        [_summary("demo_api", ok=9, failed=1, skipped=2, elapsed_ms=100)],
        settings,
    )

    assert payload["scope"] == "round"
    assert payload["level"] == LEVEL_INFO
    assert payload["alert_failure_rate"] == 0.5
    assert payload["dry_run"] is False
    assert isinstance(payload["generated_at"], str) and payload["generated_at"]
    assert payload["total"] == {
        "ok": 9,
        "failed": 1,
        "skipped": 2,
        "elapsed_ms": 100,
        "failure_rate": 0.1,
    }
    source = payload["sources"][0]
    assert source == {
        "source": "demo_api",
        "ok": 9,
        "failed": 1,
        "skipped": 2,
        "elapsed_ms": 100,
        "failure_rate": 0.1,
        "level": LEVEL_INFO,
    }


def test_round_payload_alert_when_rate_exceeds_threshold() -> None:
    settings = _settings(webhooks=["http://127.0.0.1/hook/a"], alert_failure_rate=0.1)
    # 失败率 0.2 > 0.1 → alert；边界值等于阈值时保持 info
    payload_alert = build_round_payload(
        [_summary("demo_api", ok=8, failed=2, skipped=0)], settings
    )
    assert payload_alert["level"] == LEVEL_ALERT
    assert payload_alert["sources"][0]["level"] == LEVEL_ALERT

    payload_edge = build_round_payload(
        [_summary("demo_api", ok=9, failed=1, skipped=0)], settings
    )
    assert payload_edge["level"] == LEVEL_INFO


def test_round_payload_alert_when_any_source_exceeds_threshold() -> None:
    settings = _settings(webhooks=["http://127.0.0.1/hook/a"], alert_failure_rate=0.3)
    payload = build_round_payload(
        [
            _summary("healthy", ok=10, failed=0, skipped=0),
            _summary("flaky", ok=2, failed=8, skipped=0),
        ],
        settings,
    )
    # 总失败率 0.4 同样超阈值；即使某一个源超阈值，顶层也必须是 alert
    assert payload["level"] == LEVEL_ALERT
    assert payload["sources"][0]["level"] == LEVEL_INFO
    assert payload["sources"][1]["level"] == LEVEL_ALERT


def test_source_payload_is_scoped_to_single_source() -> None:
    settings = _settings(webhooks=["http://127.0.0.1/hook/a"], alert_failure_rate=0.05)
    payload = build_source_payload(
        _summary("demo_api", ok=1, failed=1, skipped=3, elapsed_ms=50), settings
    )
    assert payload["scope"] == "source"
    assert payload["level"] == LEVEL_ALERT
    assert len(payload["sources"]) == 1
    assert payload["sources"][0]["source"] == "demo_api"
    assert payload["total"]["failure_rate"] == 0.5


# ---------- 脱敏 ----------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            "https://robot:secret@im.example.com/hooks/abc?token=zzz",
            "https://im.example.com/***",
        ),
        ("http://127.0.0.1:8099/hook/x", "http://127.0.0.1:8099/***"),
        ("https://im.example.com/very/secret/path", "https://im.example.com/***"),
    ],
)
def test_redact_webhook_url_hides_secrets(raw: str, expected: str) -> None:
    assert redact_webhook_url(raw) == expected


# ---------- 推送器：本地真实接收服务 ----------


def test_notifier_posts_full_payload_to_each_global_webhook(
    webhook_server: Any,
) -> None:
    settings = _settings(
        webhooks=[
            f"{webhook_server.url}/hook/one",
            f"{webhook_server.url}/hook/two",
        ],
        retries=0,
    )
    with Notifier(settings, sleep=lambda _: None) as notifier:
        notifier.notify_round(
            [_summary("demo_api", ok=3, failed=0, skipped=1)],
            sources=[],
        )

    assert len(webhook_server.requests) == 2
    paths = {item["path"] for item in webhook_server.requests}
    assert paths == {"/hook/one", "/hook/two"}
    for payload in webhook_server.payloads:
        assert payload["scope"] == "round"
        assert payload["sources"][0]["source"] == "demo_api"
        assert payload["total"]["ok"] == 3


def test_notifier_retries_on_500_then_gives_up_without_raising(
    webhook_server: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    webhook_server.status_code = 500
    settings = _settings(
        webhooks=[f"{webhook_server.url}/hook/a"], retries=2
    )
    with caplog.at_level(logging.WARNING, logger="crawl_flow.notify"):
        with Notifier(settings, sleep=lambda _: None) as notifier:
            # 不应抛异常
            notifier.notify_round(
                [_summary("demo_api", ok=1, failed=1, skipped=0)], sources=[]
            )

    # 首次 + 2 次重试 = 3 次请求
    assert len(webhook_server.requests) == 3
    warning_text = caplog.text
    assert "WARNING" in warning_text
    assert "已放弃" in warning_text
    # 日志里不能出现完整路径（密钥随路径脱敏）
    assert "/hook/a" not in warning_text
    assert "127.0.0.1" in warning_text


def test_notifier_succeeds_after_transport_retry() -> None:
    """传输层异常重试后成功：用假 Fetcher 精确控制每次结果。"""

    class FlakyFetcher:
        def __init__(self) -> None:
            self.calls = 0

        def post(self, url: str, json: dict[str, Any]) -> FetchResult:
            self.calls += 1
            if self.calls == 1:
                from crawl_flow.errors import FetchError

                raise FetchError("连接被重置", url=url)
            return FetchResult(url=url, status_code=200, body="ok")

        def close(self) -> None:
            pass

    settings = _settings(webhooks=["http://im.example.com/hook/x"], retries=2)
    fetcher = FlakyFetcher()
    with Notifier(settings, fetcher=fetcher, sleep=lambda _: None) as notifier:
        notifier.notify_round(
            [_summary("demo_api", ok=1, failed=0, skipped=0)], sources=[]
        )
    assert fetcher.calls == 2


def test_notifier_transport_failure_exhausts_retries_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class DeadFetcher:
        def __init__(self) -> None:
            self.calls = 0

        def post(self, url: str, json: dict[str, Any]) -> FetchResult:
            self.calls += 1
            from crawl_flow.errors import FetchError

            raise FetchError("连接超时", url=url)

        def close(self) -> None:
            pass

    settings = _settings(webhooks=["http://im.example.com/hook/x"], retries=1)
    with caplog.at_level(logging.WARNING, logger="crawl_flow.notify"):
        with Notifier(
            settings, fetcher=DeadFetcher(), sleep=lambda _: None
        ) as notifier:
            notifier.notify_round(
                [_summary("demo_api", ok=1, failed=0, skipped=0)], sources=[]
            )
    assert caplog.records and all(
        record.levelno == logging.WARNING for record in caplog.records
    )
    assert "最终失败" in caplog.text
    # 原始 URL 路径不得进入日志
    assert "/hook/x" not in caplog.text


def test_notifier_does_not_retry_4xx(webhook_server: Any) -> None:
    webhook_server.status_code = 400
    settings = _settings(
        webhooks=[f"{webhook_server.url}/hook/a"], retries=3
    )
    with Notifier(settings, sleep=lambda _: None) as notifier:
        notifier.notify_round(
            [_summary("demo_api", ok=1, failed=0, skipped=0)], sources=[]
        )
    assert len(webhook_server.requests) == 1


def test_notifier_dry_run_and_disabled_send_nothing(webhook_server: Any) -> None:
    settings = _settings(webhooks=[f"{webhook_server.url}/hook/a"], retries=0)
    with Notifier(settings, sleep=lambda _: None) as notifier:
        notifier.notify_round(
            [_summary("demo_api", ok=1, failed=0, skipped=0)],
            sources=[],
            dry_run=True,
        )
        notifier.notify_round(
            [_summary("demo_api", ok=1, failed=0, skipped=0)],
            sources=[],
            disabled=True,
        )
    assert webhook_server.requests == []


def test_notifier_timeout_is_retried_and_then_abandoned(
    webhook_server: Any,
) -> None:
    webhook_server.slow_seconds = 1.0
    settings = _settings(
        webhooks=[f"{webhook_server.url}/slow"],
        timeout_seconds=0.2,
        retries=1,
        retry_backoff_seconds=0,
    )
    with Notifier(settings) as notifier:
        # 不抛异常即视为通过；超时由 Fetcher 的 httpx 超时触发
        notifier.notify_round(
            [_summary("demo_api", ok=1, failed=0, skipped=0)], sources=[]
        )


def test_notifier_unexpected_exception_is_swallowed(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class BrokenFetcher:
        def post(self, url: str, json: dict[str, Any]) -> FetchResult:
            raise RuntimeError("意料之外的错误")

        def close(self) -> None:
            pass

    settings = _settings(webhooks=["http://im.example.com/hook/x"])
    with caplog.at_level(logging.WARNING, logger="crawl_flow.notify"):
        with Notifier(settings, fetcher=BrokenFetcher()) as notifier:
            notifier.notify_round(
                [_summary("demo_api", ok=1, failed=0, skipped=0)], sources=[]
            )
    assert "未预期异常" in caplog.text


# ---------- 单源覆盖 ----------


def test_source_override_webhook_gets_scoped_payload(
    webhook_server: Any,
    make_source: Any,
) -> None:
    settings = _settings(webhooks=[f"{webhook_server.url}/hook/global"])
    sources = [
        make_source("demo_api"),
        make_source(
            "demo_api_flaky",
            detail_path="/api/items/{id}?fail=1",
            notify={"webhooks": [f"{webhook_server.url}/hook/oncall"]},
        ),
    ]
    with Notifier(settings, sleep=lambda _: None) as notifier:
        notifier.notify_round(
            [
                _summary("demo_api", ok=2, failed=0, skipped=0),
                _summary("demo_api_flaky", ok=0, failed=2, skipped=0),
            ],
            sources=sources,
        )

    by_path = {item["path"]: item["payload"] for item in webhook_server.requests}
    assert set(by_path) == {"/hook/global", "/hook/oncall"}
    global_payload = by_path["/hook/global"]
    assert global_payload["scope"] == "round"
    assert {s["source"] for s in global_payload["sources"]} == {
        "demo_api",
        "demo_api_flaky",
    }
    oncall_payload = by_path["/hook/oncall"]
    assert oncall_payload["scope"] == "source"
    assert oncall_payload["sources"][0]["source"] == "demo_api_flaky"
    assert oncall_payload["level"] == LEVEL_ALERT


# ---------- CLI 端到端 ----------


def _write_full_config(
    tmp_path: Path,
    sources: list[dict[str, Any]],
    notify: dict[str, Any],
) -> Path:
    config_path = tmp_path / "full_notify.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"notify": notify, "sources": sources},
            allow_unicode=True,
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return config_path


def _notify_config(
    tmp_path: Path,
    source_payload: Any,
    webhook_url: str,
    *,
    alert_failure_rate: float = 0.1,
    extra_notify: dict[str, Any] | None = None,
    source_overrides: dict[str, dict[str, Any]] | None = None,
) -> Path:
    payload = source_payload("demo_api")
    if source_overrides and "demo_api" in source_overrides:
        payload["notify"] = source_overrides["demo_api"]
    notify: dict[str, Any] = {
        "webhooks": [webhook_url],
        "alert_failure_rate": alert_failure_rate,
        "retries": 0,
    }
    if extra_notify:
        notify.update(extra_notify)
    return _write_full_config(tmp_path, [payload], notify)


def _run(config_path: Path, tmp_path: Path, *extra: str) -> Any:
    return runner.invoke(
        app,
        [
            "run",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "cli.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
            "--source",
            "demo_api",
            "--limit",
            "2",
            *extra,
        ],
    )


def test_cli_webhook_500_process_exits_zero(
    source_payload: Any,
    tmp_path: Path,
    webhook_server: Any,
) -> None:
    webhook_server.status_code = 500
    config_path = _notify_config(
        tmp_path,
        source_payload,
        f"{webhook_server.url}/hook/a",
        extra_notify={"retries": 1},
    )

    result = _run(config_path, tmp_path)
    assert result.exit_code == 0, result.output
    # 至少收到一次推送（500 后重试），采集本身仍然成功
    assert len(webhook_server.requests) >= 1
    assert "ok=2" in result.output


def test_cli_payload_alert_level_above_threshold(
    source_payload: Any,
    tmp_path: Path,
    webhook_server: Any,
) -> None:
    flaky_payload = source_payload(
        "demo_api", detail_path="/api/items/{id}?fail=1"
    )
    config_path = _write_full_config(
        tmp_path,
        [flaky_payload],
        {
            "webhooks": [f"{webhook_server.url}/hook/a"],
            "alert_failure_rate": 0.1,
            "retries": 0,
        },
    )

    result = _run(config_path, tmp_path)
    assert result.exit_code == 0, result.output
    payload = webhook_server.payloads[-1]
    assert payload["level"] == LEVEL_ALERT
    assert payload["total"]["failed"] == 2
    assert payload["total"]["failure_rate"] == 1.0


def test_cli_dry_run_sends_no_notification(
    source_payload: Any,
    tmp_path: Path,
    webhook_server: Any,
) -> None:
    config_path = _notify_config(
        tmp_path, source_payload, f"{webhook_server.url}/hook/a"
    )
    result = _run(config_path, tmp_path, "--dry-run")
    assert result.exit_code == 0, result.output
    assert webhook_server.requests == []


def test_cli_no_notify_flag_sends_nothing(
    source_payload: Any,
    tmp_path: Path,
    webhook_server: Any,
) -> None:
    config_path = _notify_config(
        tmp_path, source_payload, f"{webhook_server.url}/hook/a"
    )
    result = _run(config_path, tmp_path, "--no-notify")
    assert result.exit_code == 0, result.output
    assert webhook_server.requests == []


def test_cli_without_notify_config_makes_no_extra_requests(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
    webhook_server: Any,
) -> None:
    # 未配置 notify 时行为与现状一致：不创建任何通知 HTTP 流量
    config_path = write_config([source_payload("demo_api")])
    result = _run(config_path, tmp_path)
    assert result.exit_code == 0, result.output
    assert webhook_server.requests == []


def test_cli_secret_url_is_redacted_in_logs(
    source_payload: Any,
    tmp_path: Path,
    webhook_server: Any,
    caplog: pytest.LogCaptureFixture,
) -> None:
    port = webhook_server.url.rsplit(":", 1)[1]
    secret_url = (
        f"http://robot:tokensecret@127.0.0.1:{port}/hook/secret-path"
    )
    config_path = _notify_config(tmp_path, source_payload, secret_url)
    with caplog.at_level(logging.INFO, logger="crawl_flow.notify"):
        result = runner.invoke(
            app,
            [
                "run",
                "--config",
                str(config_path),
                "--db",
                str(tmp_path / "cli.db"),
                "--raw-dir",
                str(tmp_path / "raw"),
                "--source",
                "demo_api",
                "--limit",
                "1",
            ],
        )
    assert result.exit_code == 0, result.output
    assert len(webhook_server.requests) == 1
    # 密钥与完整路径不得出现在任何日志里
    assert "tokensecret" not in caplog.text
    assert "secret-path" not in caplog.text
    # 脱敏后仍保留主机信息，便于值班同学定位是哪个接收端
    assert "127.0.0.1" in caplog.text
    assert "通知推送成功" in caplog.text
