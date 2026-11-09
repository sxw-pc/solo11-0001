"""单条详情的指数退避重试。

只负责「重试几次、每次等多久、日志怎么写」，不关心具体的抓取/解析细节：
调用方把一次详情尝试封装成无参可调用对象，失败时抛 :class:`DetailAttemptError`，
重试耗尽后返回 :class:`RetryExhausted`，由管道决定如何落死信。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar

from crawl_flow.config import RetryConfig

T = TypeVar("T")


class DetailAttemptError(Exception):
    """单次详情尝试失败的统一包装（传输异常 / HTTP 4xx-5xx / 解析异常）。"""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True)
class RetryExhausted:
    """所有尝试均失败后的最终错误信息。"""

    error: str
    attempts: int


def backoff_delay(config: RetryConfig, failed_attempt: int) -> float:
    """第 ``failed_attempt`` 次尝试失败后、下一次重试前的等待秒数。

    ``failed_attempt`` 从 1 开始：默认参数下依次为 1s、2s、4s。
    """

    return config.base_delay_seconds * config.backoff_factor ** (failed_attempt - 1)


def run_with_retries(
    attempt: Callable[[], T],
    config: RetryConfig,
    *,
    logger: logging.Logger,
    source: str,
    external_id: str,
    sleep: Callable[[float], None] = time.sleep,
) -> tuple[T | None, RetryExhausted | None, int]:
    """按指数退避执行 ``attempt``，成功返回 ``(value, None, attempts)``。

    单条最多尝试 ``max_retries + 1`` 次；耗尽后返回
    ``(None, RetryExhausted, attempts)``。每次失败与每次退避等待都会写日志。
    """

    total_attempts = config.max_retries + 1
    attempts = 0
    while True:
        attempts += 1
        try:
            return attempt(), None, attempts
        except DetailAttemptError as exc:
            if attempts <= config.max_retries:
                delay = backoff_delay(config, attempts)
                logger.warning(
                    "detail attempt failed source=%s external_id=%s attempt=%d/%d "
                    "error=%s next_delay_s=%.3f",
                    source,
                    external_id,
                    attempts,
                    total_attempts,
                    exc.reason,
                    delay,
                )
                sleep(delay)
                continue
            logger.error(
                "detail failed permanently source=%s external_id=%s attempts=%d error=%s",
                source,
                external_id,
                attempts,
                exc.reason,
            )
            return None, RetryExhausted(error=exc.reason, attempts=attempts), attempts
