"""通知推送器：把汇总消息 POST 到 IM 机器人 webhook。

设计约束：

- 复用 :class:`crawl_flow.fetch.client.Fetcher`（底层仍是 httpx），不引入新依赖；
- 推送有独立超时与有限次重试，传输异常/超时与接收端 5xx 触发重试，4xx 立即放弃；
- 任何推送失败只打 WARNING，不向上抛异常，绝不影响采集进程的退出码；
- webhook 地址可能带密钥，所有日志一律走 :func:`redact_webhook_url` 脱敏。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from crawl_flow.config import NotifySettings, SourceConfig
from crawl_flow.errors import FetchError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.notify.message import (
    build_round_payload,
    build_source_payload,
    redact_webhook_url,
)
from crawl_flow.pipeline.run import RunSummary

LOGGER_NAME = "crawl_flow.notify"


class Notifier:
    """采集结束后的 webhook 推送器。"""

    def __init__(
        self,
        settings: NotifySettings,
        *,
        fetcher: Fetcher | None = None,
        logger: logging.Logger | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._settings = settings
        self._log = logger or logging.getLogger(LOGGER_NAME)
        self._sleep = sleep
        self._owns_fetcher = fetcher is None
        # 通知使用独立的 Fetcher：超时与重试语义由本类控制，
        # Fetcher 自身的传输重试关闭，避免两层重试叠加放大请求次数。
        self._fetcher = fetcher or Fetcher(
            timeout=settings.timeout_seconds, retries=0
        )

    def notify_round(
        self,
        summaries: list[RunSummary],
        sources: list[SourceConfig],
        *,
        dry_run: bool = False,
        disabled: bool = False,
    ) -> None:
        """推送一轮采集的汇总。

        ``dry_run``（演练模式）或 ``disabled``（命令行临时禁用）时直接返回，
        不发出任何 HTTP 请求。本方法吞掉所有异常，只记录 WARNING 日志。
        """

        if disabled:
            self._log.info("通知已被命令行开关临时禁用，跳过推送")
            return
        if dry_run:
            self._log.info("dry-run 演练模式不发送任何通知请求")
            return
        try:
            self._dispatch(summaries, sources)
        except Exception as exc:  # noqa: BLE001 - 通知失败不得影响采集退出码
            # 只记录异常类型，避免异常消息里夹带完整 webhook（含密钥）。
            self._log.warning(
                "通知推送发生未预期异常，已忽略: %s", exc.__class__.__name__
            )

    def _dispatch(
        self, summaries: list[RunSummary], sources: list[SourceConfig]
    ) -> None:
        summary_by_name = {summary.source: summary for summary in summaries}
        overridden = {
            source.name: source.notify.webhooks
            for source in sources
            if source.notify is not None
        }

        # 全局地址：收到本轮所有源的全量汇总。
        if self._settings.webhooks:
            round_payload = build_round_payload(
                summaries, self._settings, dry_run=False
            )
            for webhook in dict.fromkeys(self._settings.webhooks):
                self._deliver(webhook, round_payload)

        # 单源覆盖地址：只收到该源自己的汇总，不再接收全局消息。
        for source_name, webhooks in overridden.items():
            summary = summary_by_name.get(source_name)
            if summary is None:
                continue
            source_payload = build_source_payload(
                summary, self._settings, dry_run=False
            )
            for webhook in dict.fromkeys(webhooks):
                self._deliver(webhook, source_payload)

    def _deliver(self, webhook: str, payload: dict[str, Any]) -> None:
        """向单个 webhook 推送，带有限次重试；失败只打 WARNING。"""

        safe_url = redact_webhook_url(webhook)
        attempts = self._settings.retries + 1
        for attempt in range(1, attempts + 1):
            try:
                result = self._fetcher.post(webhook, json=payload)
            except FetchError as exc:
                # 连接失败 / 超时等传输层异常。
                # 注意：FetchError 的消息里带原始 URL（可能含密钥），
                # 这里只记录异常类名，完整 URL 绝不进入日志。
                if attempt < attempts:
                    self._log.warning(
                        "通知推送传输失败，准备重试 url=%s attempt=%d/%d error=%s",
                        safe_url,
                        attempt,
                        attempts,
                        exc.__class__.__name__,
                    )
                    self._sleep(self._settings.retry_backoff_seconds)
                    continue
                self._log.warning(
                    "通知推送最终失败（传输异常），已放弃 url=%s attempts=%d error=%s",
                    safe_url,
                    attempts,
                    exc.__class__.__name__,
                )
                return

            if 200 <= result.status_code < 300:
                self._log.info(
                    "通知推送成功 url=%s status=%d attempt=%d",
                    safe_url,
                    result.status_code,
                    attempt,
                )
                return

            if result.status_code >= 500 and attempt < attempts:
                self._log.warning(
                    "通知推送收到 %d，准备重试 url=%s attempt=%d/%d",
                    result.status_code,
                    safe_url,
                    attempt,
                    attempts,
                )
                self._sleep(self._settings.retry_backoff_seconds)
                continue

            # 4xx（客户端问题，重试无意义）或重试耗尽后的 5xx。
            self._log.warning(
                "通知推送失败，已放弃 url=%s status=%d attempts=%d",
                safe_url,
                result.status_code,
                attempt,
            )
            return

    def close(self) -> None:
        if self._owns_fetcher:
            self._fetcher.close()

    def __enter__(self) -> "Notifier":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()
