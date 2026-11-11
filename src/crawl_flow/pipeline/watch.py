"""``--watch`` 模式：周期性采集，并在轮次之间安全热重载配置。

热重载只替换下一轮使用的 :class:`~crawl_flow.config.SourceConfig`，不打断
正在执行的 :func:`~crawl_flow.pipeline.run.run_source`；HTTP 客户端、数据库
连接与原始响应目录由调用方在进入循环前创建并全程复用，避免资源泄漏。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable

from crawl_flow.config import SourceConfig, select_sources
from crawl_flow.config_watch import ConfigWatcher
from crawl_flow.errors import ConfigError, FetchError, ParseError, StoreError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.pipeline.run import RunSummary, run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

LOGGER_NAME = "crawl_flow.pipeline.watch"

RoundCallback = Callable[[int, list[RunSummary]], None]
SleepCallback = Callable[[float], None]


def run_watch_loop(
    watcher: ConfigWatcher,
    db: Database,
    raw_store: RawStore,
    fetcher: Fetcher,
    *,
    name: str | None,
    all_sources: bool,
    limit: int | None = None,
    dry_run: bool = False,
    poll_interval: float = 2.0,
    max_rounds: int | None = None,
    logger: logging.Logger | None = None,
    sleep: SleepCallback = time.sleep,
    on_round: RoundCallback | None = None,
) -> list[RunSummary]:
    """持续执行采集轮次，并在轮次开始前处理配置热重载。

    ``max_rounds`` 主要用于测试；生产 CLI 传 ``None`` 表示直到
    :class:`KeyboardInterrupt` 才退出。单个源失败只记录 WARNING，不影响
    其他源和后续轮次。
    """

    log = logger or logging.getLogger(LOGGER_NAME)
    last_selected: list[SourceConfig] | None = None
    all_summaries: list[RunSummary] = []
    rounds = 0

    while max_rounds is None or rounds < max_rounds:
        if rounds > 0:
            watcher.poll()

        try:
            selected = select_sources(
                watcher.sources,
                name=name,
                all_sources=all_sources,
            )
        except ConfigError as exc:
            # 首次选择必须由 CLI 按退出码 2 处理；后续轮次则保留旧选择继续运行。
            if last_selected is None:
                raise
            log.warning(
                "config reload rejected by source selector error=%s keep_previous=true",
                exc,
            )
            selected = last_selected
        else:
            last_selected = selected

        summaries: list[RunSummary] = []
        for source in selected:
            try:
                summary = run_source(
                    source,
                    db,
                    raw_store,
                    fetcher=fetcher,
                    limit=limit,
                    dry_run=dry_run,
                    logger=log,
                )
            except (FetchError, ParseError, StoreError) as exc:
                # watch 模式是长驻进程：单源失败不能终止整个监听任务。
                log.warning(
                    "watch source run failed source=%s error=%s continue=true",
                    source.name,
                    exc,
                )
                continue
            summaries.append(summary)

        rounds += 1
        all_summaries.extend(summaries)
        if on_round is not None:
            on_round(rounds, summaries)

        if max_rounds is not None and rounds >= max_rounds:
            break

        try:
            sleep(poll_interval)
        except KeyboardInterrupt:
            log.info("watch stopped reason=keyboard_interrupt rounds=%d", rounds)
            break

    return all_summaries


__all__ = ["run_watch_loop"]
