"""多目标同步：成功投递、失败重试落库、故障降级、并发上限与配置校验。"""

from __future__ import annotations

import json
import socket
import threading
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from flask import Flask, request
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner
from werkzeug.serving import make_server

from crawl_flow.cli import app
from crawl_flow.config import SyncSettings, load_sources, load_sync_settings
from crawl_flow.errors import ConfigError, SyncError
from crawl_flow.models import SyncTaskRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore
from crawl_flow.storage.sync import SyncManager

FAST = SyncSettings(max_attempts=3, backoff_seconds=0)


@pytest.fixture
def make_receiver() -> Iterator[Any]:
    """测试用 webhook 接收端工厂：记录请求体，按指定状态码应答。"""

    servers: list[tuple[Any, threading.Thread]] = []

    def _make(status_code: int = 200) -> tuple[str, list[dict[str, Any]]]:
        received: list[dict[str, Any]] = []
        receiver = Flask(f"sync-receiver-{status_code}-{len(servers)}")

        @receiver.post("/ingest")
        def ingest() -> tuple[str, int]:
            received.append(request.get_json(force=True))
            return ("", status_code)

        server = make_server("127.0.0.1", 0, receiver, threaded=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        servers.append((server, thread))
        return f"http://127.0.0.1:{server.server_port}/ingest", received

    yield _make
    for server, thread in servers:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture
def unused_port() -> int:
    """一个保证无人监听的本地端口（绑定后立刻关闭）。"""

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _sync_rows(db: Database) -> list[SyncTaskRow]:
    with Session(db.engine) as session:
        return list(session.execute(select(SyncTaskRow)).scalars())


# ---------------------------------------------------------------- 同步成功


def test_sync_success_file_target(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "sync-out"
    source = make_source(
        "demo_api",
        sync_targets=[{"name": "dump", "type": "file", "path": str(out_dir)}],
    )
    summary = run_source(source, db, raw_store, limit=2, sync_settings=FAST)

    assert (summary.ok, summary.failed) == (2, 0)
    assert summary.sync == {"delivered": 4, "pending": 0}
    assert db.count_sync_tasks() == 0

    records = _read_jsonl(out_dir / "demo_api" / "record.jsonl")
    raws = _read_jsonl(out_dir / "demo_api" / "raw.jsonl")
    assert sorted(item["external_id"] for item in records) == ["api-1", "api-2"]
    assert sorted(item["external_id"] for item in raws) == ["api-1", "api-2"]
    assert {item["kind"] for item in records} == {"record"}
    assert {item["kind"] for item in raws} == {"raw"}
    assert {item["target"] for item in records + raws} == {"dump"}

    record = next(item for item in records if item["external_id"] == "api-2")
    assert record["payload"]["source"] == "demo_api"
    assert record["payload"]["title"] == "开源数据库性能基准测试汇总"
    assert record["payload"]["tags"] == ["数据库", "性能", "基线"]

    raw = next(item for item in raws if item["external_id"] == "api-2")
    assert raw["payload"]["http_status"] == 200
    assert raw["payload"]["status"] == "ok"
    assert "三款开源数据库" in raw["payload"]["payload"]["body"]


def test_sync_success_webhook_target(
    make_source: Any,
    make_receiver: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    url, received = make_receiver(200)
    source = make_source(
        "demo_api",
        sync_targets=[{"name": "hook", "type": "webhook", "url": url}],
    )
    summary = run_source(source, db, raw_store, limit=2, sync_settings=FAST)

    assert summary.sync == {"delivered": 4, "pending": 0}
    assert db.count_sync_tasks() == 0
    kinds = sorted((item["kind"], item["external_id"]) for item in received)
    assert kinds == [
        ("raw", "api-1"),
        ("raw", "api-2"),
        ("record", "api-1"),
        ("record", "api-2"),
    ]
    assert all(item["target"] == "hook" for item in received)


def test_failed_detail_raw_payload_synced(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "sync-out"
    source = make_source(
        "demo_api_flaky",
        detail_path="/api/items/{id}?fail=1",
        sync_targets=[{"name": "dump", "type": "file", "path": str(out_dir)}],
    )
    summary = run_source(source, db, raw_store, limit=2, sync_settings=FAST)

    assert summary.failed == 2
    assert summary.sync == {"delivered": 2, "pending": 0}
    raws = _read_jsonl(out_dir / "demo_api_flaky" / "raw.jsonl")
    assert len(raws) == 2
    assert all(item["payload"]["status"] == "failed" for item in raws)
    assert all(item["payload"]["http_status"] == 500 for item in raws)
    assert not (out_dir / "demo_api_flaky" / "record.jsonl").exists()


# ---------------------------------------------------------------- 失败重试


def test_sync_retry_exhausted_marks_pending(
    make_source: Any,
    make_receiver: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    url, received = make_receiver(500)
    source = make_source(
        "demo_api",
        sync_targets=[{"name": "hook", "type": "webhook", "url": url, "timeout_seconds": 2}],
    )
    summary = run_source(source, db, raw_store, limit=1, sync_settings=FAST)

    # 本地采集不受同步失败影响
    assert (summary.ok, summary.failed) == (1, 0)
    assert db.count_records("demo_api") == 1
    assert summary.sync == {"delivered": 0, "pending": 2}

    # record + raw 两个任务，每个恰好尝试 3 次，不会无限重试
    assert len(received) == 2 * 3

    rows = _sync_rows(db)
    assert len(rows) == 2
    assert {row.kind for row in rows} == {"record", "raw"}
    for row in rows:
        assert row.target == "hook"
        assert row.status == "pending"
        assert row.attempts == 3
        assert row.max_attempts == 3
        assert row.last_error is not None and "HTTP 500" in row.last_error
        assert row.payload["external_id"] == "api-1"


def test_retry_succeeds_before_exhaustion(db: Database) -> None:
    class _FlakyTarget:
        name = "flaky"

        def __init__(self) -> None:
            self.calls = 0
            self._lock = threading.Lock()

        def deliver(self, item: Any) -> None:
            with self._lock:
                self.calls += 1
                if self.calls <= 2:
                    raise SyncError("boom")

        def close(self) -> None:
            return None

    target = _FlakyTarget()
    manager = SyncManager([target], db, settings=FAST)
    manager.submit("record", "demo_api", "api-1", {"x": 1})
    manager.close()

    assert target.calls == 3
    assert manager.stats == {"delivered": 1, "pending": 0}
    assert db.count_sync_tasks() == 0


def test_sync_concurrency_is_bounded(db: Database) -> None:
    peak = 0
    current = 0
    lock = threading.Lock()

    class _SlowTarget:
        name = "slow"

        def deliver(self, item: Any) -> None:
            nonlocal peak, current
            with lock:
                current += 1
                peak = max(peak, current)
            time.sleep(0.05)
            with lock:
                current -= 1

        def close(self) -> None:
            return None

    manager = SyncManager(
        [_SlowTarget()],
        db,
        settings=SyncSettings(max_workers=2, backoff_seconds=0),
    )
    for index in range(6):
        manager.submit("record", "demo_api", f"api-{index}", {})
    manager.close()

    assert peak <= 2
    assert manager.stats == {"delivered": 6, "pending": 0}


# ---------------------------------------------------------------- 故障降级


def test_external_storage_down_does_not_break_collection(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    tmp_path: Path,
    unused_port: int,
) -> None:
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    source = make_source(
        "demo_api",
        sync_targets=[
            {
                "name": "dead_hook",
                "type": "webhook",
                "url": f"http://127.0.0.1:{unused_port}/ingest",
                "timeout_seconds": 1,
            },
            # file 目标落在普通文件下面，mkdir 必失败（权限/磁盘类故障）
            {"name": "dead_file", "type": "file", "path": str(blocker / "sub")},
        ],
    )
    summary = run_source(source, db, raw_store, limit=2, sync_settings=FAST)

    # 外部存储完全不可用，本地采集照常完成
    assert (summary.ok, summary.failed) == (2, 0)
    assert db.count_records("demo_api") == 2
    assert db.count_raw("demo_api") == 2
    assert len(list(raw_store.root.rglob("*.json"))) == 2

    # 2 目标 × 2 条 × (record + raw) = 8 条待重试，每个目标只试 3 次
    assert summary.sync == {"delivered": 0, "pending": 8}
    assert db.count_sync_tasks("pending") == 8
    assert {row.target for row in _sync_rows(db)} == {"dead_hook", "dead_file"}

    # db stats 统计逻辑不受同步功能影响
    stats = db.stats()
    assert stats["totals"] == {"records": 2, "raw_ok": 2, "raw_failed": 0}
    assert [row["source"] for row in stats["sources"]] == ["demo_api"]


def test_cli_run_completes_when_sync_target_down(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
    unused_port: int,
) -> None:
    config_path = write_config(
        [
            source_payload(
                "demo_api",
                sync_targets=[
                    {
                        "name": "dead",
                        "type": "webhook",
                        "url": f"http://127.0.0.1:{unused_port}/ingest",
                        "timeout_seconds": 1,
                    }
                ],
            )
        ]
    )
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["sync"] = {"backoff_seconds": 0}
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")

    db_path = tmp_path / "cli.db"
    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "2",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "source=demo_api ok=2" in result.output
    assert "sync_pending=4" in result.output

    database = Database(db_path)
    database.init()
    try:
        assert database.count_records("demo_api") == 2
        assert database.count_sync_tasks("pending") == 4
        stats = database.stats()
        assert stats["totals"] == {"records": 2, "raw_ok": 2, "raw_failed": 0}
    finally:
        database.close()


# ------------------------------------------------------- 未配置目标流程不变


def test_source_without_sync_targets_runs_as_before(
    api_source: Any,
    db: Database,
    raw_store: RawStore,
) -> None:
    summary = run_source(api_source, db, raw_store, limit=2)

    assert (summary.ok, summary.failed) == (2, 0)
    assert summary.sync is None
    assert db.count_sync_tasks() == 0
    assert db.count_records("demo_api") == 2


def test_dry_run_skips_sync(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "sync-out"
    source = make_source(
        "demo_api",
        sync_targets=[{"name": "dump", "type": "file", "path": str(out_dir)}],
    )
    summary = run_source(source, db, raw_store, limit=1, dry_run=True)

    assert summary.ok == 1
    assert summary.sync is None
    assert not out_dir.exists()
    assert db.count_sync_tasks() == 0


def test_disabled_sync_target_is_skipped(
    make_source: Any,
    db: Database,
    raw_store: RawStore,
    tmp_path: Path,
) -> None:
    out_dir = tmp_path / "sync-out"
    source = make_source(
        "demo_api",
        sync_targets=[
            {"name": "dump", "type": "file", "path": str(out_dir), "enabled": False}
        ],
    )
    summary = run_source(source, db, raw_store, limit=1)

    assert summary.ok == 1
    assert summary.sync is None
    assert not out_dir.exists()
    assert db.count_sync_tasks() == 0


# ---------------------------------------------------------------- 配置校验


def test_sync_targets_parse_from_yaml(write_config: Any, source_payload: Any) -> None:
    config_path = write_config(
        [
            source_payload(
                "demo_api",
                sync_targets=[
                    {"name": "hook", "type": "webhook", "url": "https://example.com/ingest"},
                    {"name": "dump", "type": "file", "path": "data/sync"},
                ],
            )
        ]
    )
    sources = load_sources(config_path)

    targets = sources[0].sync_targets
    assert [target.name for target in targets] == ["hook", "dump"]
    assert targets[0].timeout_seconds == 5.0
    assert targets[1].enabled is True


def test_source_without_sync_targets_defaults_empty(
    write_config: Any, source_payload: Any
) -> None:
    config_path = write_config([source_payload("demo_api")])
    sources = load_sources(config_path)
    assert sources[0].sync_targets == []


@pytest.mark.parametrize(
    "target",
    [
        {"name": "hook", "type": "webhook"},  # 缺 url
        {"name": "hook", "type": "webhook", "url": "ftp://example.com"},  # 非法 url
        {"name": "dump", "type": "file"},  # 缺 path
        {"name": "x", "type": "ftp"},  # 未知类型
    ],
)
def test_invalid_sync_target_rejected(
    write_config: Any, source_payload: Any, target: dict[str, Any]
) -> None:
    config_path = write_config([source_payload("demo_api", sync_targets=[target])])
    with pytest.raises(ConfigError):
        load_sources(config_path)


def test_duplicate_sync_target_names_rejected(
    write_config: Any, source_payload: Any
) -> None:
    config_path = write_config(
        [
            source_payload(
                "demo_api",
                sync_targets=[
                    {"name": "dump", "type": "file", "path": "a"},
                    {"name": "dump", "type": "file", "path": "b"},
                ],
            )
        ]
    )
    with pytest.raises(ConfigError):
        load_sources(config_path)


def test_sync_settings_defaults_when_section_missing(
    write_config: Any, source_payload: Any
) -> None:
    config_path = write_config([source_payload("demo_api")])
    settings = load_sync_settings(config_path)
    assert settings.max_workers == 2
    assert settings.max_attempts == 3
    assert settings.backoff_seconds == 0.2


def test_sync_settings_custom_values(write_config: Any, source_payload: Any) -> None:
    config_path = write_config([source_payload("demo_api")])
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["sync"] = {"max_workers": 4, "max_attempts": 2, "backoff_seconds": 0}
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")

    settings = load_sync_settings(config_path)
    assert (settings.max_workers, settings.max_attempts, settings.backoff_seconds) == (4, 2, 0)


def test_sync_settings_invalid_values(write_config: Any, source_payload: Any) -> None:
    config_path = write_config([source_payload("demo_api")])
    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    payload["sync"] = {"max_workers": 0}
    config_path.write_text(yaml.safe_dump(payload, allow_unicode=True), encoding="utf-8")

    with pytest.raises(ConfigError):
        load_sync_settings(config_path)
