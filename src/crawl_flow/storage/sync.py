"""多目标同步：采集完成后把记录与原始响应异步投递到外部存储。

设计原则：

- **本地优先**：同步只在本地 SQLite/raw 落盘成功后触发，且任何同步异常
  都不会传播回采集主流程。
- **异步并发**：:class:`SyncManager` 用有界线程池投递，``max_workers``
  控制并发数，避免占用过多资源拖慢主流程。
- **有限重试**：每个任务最多尝试 ``max_attempts`` 次（默认 3，含首次），
  仍失败则以 ``pending`` 状态持久化到独立的 ``sync_tasks`` 表等待后续
  处理，不会无限重试。
"""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx

from crawl_flow.config import SyncSettings, SyncTargetConfig
from crawl_flow.errors import SyncError
from crawl_flow.models import RawRecord, Record
from crawl_flow.storage.db import Database

LOGGER_NAME = "crawl_flow.sync"
MAX_ERROR_LENGTH = 500
SYNC_USER_AGENT = "crawl-flow-sync/0.1 (+https://example.invalid/crawl-flow)"


@dataclass(frozen=True)
class SyncItem:
    """一条待投递的同步任务。"""

    target: str
    kind: str  # "record" | "raw"
    source: str
    external_id: str
    payload: dict[str, Any]


def record_payload(record: Record) -> dict[str, Any]:
    """结构化记录的可 JSON 序列化载荷。"""

    return record.model_dump(mode="json")


def raw_payload(raw: RawRecord, payload: dict[str, Any]) -> dict[str, Any]:
    """原始响应载荷：元信息 + 与本地 raw 文件一致的完整内容。"""

    return {
        "source": raw.source,
        "external_id": raw.external_id,
        "url": raw.url,
        "http_status": raw.http_status,
        "fetched_at": raw.fetched_at.isoformat(),
        "status": raw.status,
        "error": raw.error,
        "raw_path": raw.raw_path,
        "payload": payload,
    }


class SyncTarget(Protocol):
    """外部存储目标抽象：deliver 抛 :class:`SyncError` 表示可重试的失败。"""

    name: str

    def deliver(self, item: SyncItem) -> None: ...

    def close(self) -> None: ...


class WebhookTarget:
    """把同步任务 POST 到 HTTP 端点；4xx/5xx 与传输异常都视为失败。"""

    def __init__(self, config: SyncTargetConfig) -> None:
        self.name = config.name
        self._url = config.url or ""
        self._client = httpx.Client(
            timeout=config.timeout_seconds,
            follow_redirects=True,
            headers={
                "User-Agent": SYNC_USER_AGENT,
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )

    def deliver(self, item: SyncItem) -> None:
        envelope = {
            "target": item.target,
            "kind": item.kind,
            "source": item.source,
            "external_id": item.external_id,
            "payload": item.payload,
        }
        try:
            response = self._client.post(self._url, json=envelope)
        except httpx.HTTPError as exc:
            # 连接失败、读写超时、网络中断等传输层异常
            raise SyncError(
                f"webhook 投递失败 target={self.name} "
                f"error={exc.__class__.__name__}: {exc}"
            ) from exc
        if response.status_code >= 400:
            # 含 401/403 权限不足与 5xx 服务端故障
            raise SyncError(
                f"webhook 返回 HTTP {response.status_code} target={self.name} url={self._url}"
            )

    def close(self) -> None:
        self._client.close()


class FileTarget:
    """以 JSONL 追加写入交接目录，每 (source, kind) 一个文件。"""

    def __init__(self, config: SyncTargetConfig) -> None:
        self.name = config.name
        self._root = Path(config.path or "")
        self._lock = threading.Lock()

    def deliver(self, item: SyncItem) -> None:
        envelope = {
            "target": item.target,
            "kind": item.kind,
            "source": item.source,
            "external_id": item.external_id,
            "payload": item.payload,
        }
        directory = self._root / item.source
        path = directory / f"{item.kind}.jsonl"
        try:
            with self._lock:
                directory.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(envelope, ensure_ascii=False) + "\n")
        except OSError as exc:
            # 磁盘满、权限不足、挂载断开等
            raise SyncError(f"file 目标写入失败 target={self.name} path={path} error={exc}") from exc

    def close(self) -> None:
        return None


_TARGET_TYPES = {
    "webhook": WebhookTarget,
    "file": FileTarget,
}


def build_targets(configs: list[SyncTargetConfig]) -> list[SyncTarget]:
    """按配置实例化 enabled 的同步目标。"""

    targets: list[SyncTarget] = []
    for config in configs:
        if not config.enabled:
            continue
        factory = _TARGET_TYPES[config.type]
        targets.append(factory(config))
    return targets


class SyncManager:
    """同步调度器：有界线程池异步投递 + 有限重试 + 失败落 sync_tasks。

    所有公共方法都不会向调用方抛异常——同步层的任何故障（含死信写库
    失败）只记录日志，保证采集主流程稳定。
    """

    def __init__(
        self,
        targets: list[SyncTarget],
        db: Database,
        *,
        settings: SyncSettings | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._targets = list(targets)
        self._db = db
        self._settings = settings or SyncSettings()
        self._log = logger or logging.getLogger(LOGGER_NAME)
        self._executor = ThreadPoolExecutor(
            max_workers=self._settings.max_workers,
            thread_name_prefix="crawl-flow-sync",
        )
        self._futures: list[Future[None]] = []
        self._lock = threading.Lock()
        self._closed = False
        self._delivered = 0
        self._dead = 0

    @classmethod
    def from_source(
        cls,
        source_targets: list[SyncTargetConfig],
        db: Database,
        *,
        settings: SyncSettings | None = None,
        logger: logging.Logger | None = None,
    ) -> "SyncManager | None":
        """按源配置构建管理器；没有 enabled 目标时返回 None（流程完全不变）。"""

        targets = build_targets(source_targets)
        if not targets:
            return None
        return cls(targets, db, settings=settings, logger=logger)

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            return {"delivered": self._delivered, "pending": self._dead}

    def submit(self, kind: str, source: str, external_id: str, payload: dict[str, Any]) -> None:
        """向所有目标各投递一条任务；本身绝不抛异常。"""

        try:
            with self._lock:
                if self._closed:
                    return
                for target in self._targets:
                    item = SyncItem(
                        target=target.name,
                        kind=kind,
                        source=source,
                        external_id=external_id,
                        payload=payload,
                    )
                    future = self._executor.submit(self._deliver_with_retry, target, item)
                    self._futures.append(future)
        except Exception as exc:  # noqa: BLE001 —— 同步层故障不得影响主流程
            self._log.warning(
                "sync submit failed source=%s external_id=%s kind=%s error=%s",
                source,
                external_id,
                kind,
                exc,
            )

    def flush(self) -> None:
        """等待已提交的任务全部结束（任务内部已吞掉所有异常）。"""

        while True:
            with self._lock:
                futures = self._futures
                self._futures = []
            if not futures:
                return
            for future in futures:
                try:
                    future.result()
                except Exception as exc:  # noqa: BLE001 —— 防御性兜底
                    self._log.warning("sync task raised unexpectedly: %s", exc)

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
        self.flush()
        self._executor.shutdown(wait=True)
        for target in self._targets:
            try:
                target.close()
            except Exception as exc:  # noqa: BLE001
                self._log.warning("sync target close failed target=%s error=%s", target.name, exc)

    def __enter__(self) -> "SyncManager":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    def _deliver_with_retry(self, target: SyncTarget, item: SyncItem) -> None:
        """单任务投递：最多 max_attempts 次，耗尽后落 sync_tasks 待重试。"""

        settings = self._settings
        last_error: str | None = None
        for attempt in range(1, settings.max_attempts + 1):
            try:
                target.deliver(item)
            except Exception as exc:  # noqa: BLE001 —— 目标实现的任何异常都按失败重试
                last_error = str(exc).strip().replace("\n", " ")[:MAX_ERROR_LENGTH] or repr(exc)
                self._log.warning(
                    "sync attempt failed target=%s kind=%s source=%s external_id=%s "
                    "attempt=%d/%d error=%s",
                    item.target,
                    item.kind,
                    item.source,
                    item.external_id,
                    attempt,
                    settings.max_attempts,
                    last_error,
                )
                if attempt < settings.max_attempts and settings.backoff_seconds > 0:
                    time.sleep(settings.backoff_seconds * attempt)
                continue
            with self._lock:
                self._delivered += 1
            self._log.info(
                "sync delivered target=%s kind=%s source=%s external_id=%s attempt=%d",
                item.target,
                item.kind,
                item.source,
                item.external_id,
                attempt,
            )
            return

        # 重试耗尽：持久化为待重试，不再占用资源重试。
        try:
            self._db.mark_sync_pending(
                target=item.target,
                kind=item.kind,
                source=item.source,
                external_id=item.external_id,
                payload=item.payload,
                attempts=settings.max_attempts,
                max_attempts=settings.max_attempts,
                last_error=last_error,
            )
        except Exception as exc:  # noqa: BLE001 —— 死信写库失败也只记日志
            self._log.error(
                "sync dead-letter persist failed target=%s source=%s external_id=%s error=%s",
                item.target,
                item.source,
                item.external_id,
                exc,
            )
        with self._lock:
            self._dead += 1
        self._log.warning(
            "sync pending target=%s kind=%s source=%s external_id=%s attempts=%d",
            item.target,
            item.kind,
            item.source,
            item.external_id,
            settings.max_attempts,
        )
