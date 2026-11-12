"""健康探测：成功/各类失败、评级阈值、持久化与请求约束。"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator

import pytest
from sqlalchemy import inspect, text
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.config import SourceConfig
from crawl_flow.errors import FetchError
from crawl_flow.fetch.types import FetchResult
from crawl_flow.models import ProbeRecord, RawRecord, SourceHealthStats
from crawl_flow.pipeline.health import (
    FAILURE_RATE_DEGRADED,
    FAILURE_RATE_UNHEALTHY,
    SLOW_AVG_MS,
    SLOW_PROBE_MS,
    probe_source,
    probe_sources,
    rate_health,
)
from crawl_flow.sources.base import build_fetcher
from crawl_flow.storage.db import Database

REPO_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# 请求计数服务：验证探测只请求 1 页、不抓详情、不额外重试
# ---------------------------------------------------------------------------


class _CountingWSGI:
    """包住 Flask 应用，记录每个请求的路径与查询串。"""

    def __init__(self, app: Any) -> None:
        self.app = app
        self.requests: list[tuple[str, str]] = []

    def __call__(self, environ: dict[str, Any], start_response: Any) -> Any:
        self.requests.append((environ.get("PATH_INFO", ""), environ.get("QUERY_STRING", "")))
        return self.app(environ, start_response)


@pytest.fixture
def counting_api(mock_base_url: str) -> Iterator[tuple[str, _CountingWSGI]]:
    """mock 站点的计数包装；返回 (base_url, counter)。"""

    from werkzeug.serving import make_server

    from mock_site.app import create_app

    wrapper = _CountingWSGI(create_app(REPO_ROOT / "mock_site" / "fixtures"))
    server = make_server("127.0.0.1", 0, wrapper)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", wrapper
    finally:
        server.shutdown()
        thread.join(timeout=5)


class _SlowHandler(BaseHTTPRequestHandler):
    """可配置延迟与状态码的最小 HTTP handler，并统计收到的请求数。"""

    attempts = 0
    delay = 0.0
    status = 200
    payload = '{"total": 0, "items": []}'

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler 约定
        type(self).attempts += 1
        time.sleep(type(self).delay)
        body = type(self).payload.encode("utf-8")
        self.send_response(type(self).status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def slow_server() -> Iterator[Any]:
    """返回 (base_url, handler_class)；handler 延迟 1s 后返回 200。"""

    handler = type("SlowHandler", (_SlowHandler,), {"attempts": 0, "delay": 1.0})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", handler
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.fixture
def error_server() -> Iterator[tuple[str, Any]]:
    """返回 (base_url, handler_class)；handler 立即返回 503。"""

    handler = type("ErrorHandler", (_SlowHandler,), {"attempts": 0, "delay": 0.0, "status": 503})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", handler
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


# ---------------------------------------------------------------------------
# 成功与各类失败场景
# ---------------------------------------------------------------------------


def test_probe_api_success_is_healthy(api_source: SourceConfig) -> None:
    probe = probe_source(api_source)
    assert probe.outcome == "ok"
    assert probe.rating == "healthy"
    assert probe.http_status == 200
    assert probe.error is None


def test_probe_html_success_is_healthy(html_source: SourceConfig) -> None:
    probe = probe_source(html_source)
    assert probe.outcome == "ok"
    assert probe.rating == "healthy"


def test_probe_only_requests_first_list_page(make_source: Any, counting_api: Any) -> None:
    base_url, counter = counting_api
    source = make_source("probe_count", base_url=base_url)

    probe = probe_source(source)

    assert probe.rating == "healthy"
    assert len(counter.requests) == 1
    path, query = counter.requests[0]
    assert path == "/api/items"
    assert "page=1" in query
    # 不抓详情：没有任何 /api/items/{id} 请求。
    assert all("/api/items/" not in path for path, _ in counter.requests)


def test_probe_http_4xx_is_unhealthy(make_source: Any) -> None:
    source = make_source("probe_404", list_path="/api/missing")

    probe = probe_source(source)

    assert probe.outcome == "http_4xx"
    assert probe.http_status == 404
    assert probe.rating == "unhealthy"


def test_probe_http_5xx_is_degraded(make_source: Any, counting_api: Any) -> None:
    base_url, counter = counting_api
    source = make_source("probe_503", base_url=base_url, list_path="/api/items?fail=1")

    probe = probe_source(source)

    assert probe.outcome == "http_5xx"
    assert probe.http_status == 503
    assert probe.rating == "degraded"
    # 5xx 不在 Fetcher 的重试范围内：只发一次请求，不额外增加次数。
    assert len(counter.requests) == 1


def test_probe_parse_error_is_degraded(make_source: Any) -> None:
    # API 适配器拿到 HTML 索引页 -> JSON 解析失败。
    source = make_source("probe_parse", list_path="/")

    probe = probe_source(source)

    assert probe.outcome == "parse_error"
    assert probe.http_status == 200
    assert probe.rating == "degraded"
    assert probe.error is not None


def test_probe_html_parse_error_is_degraded(make_source: Any) -> None:
    # HTML 适配器拿到 JSON -> 找不到 table.items。
    source = make_source("probe_html_parse", source_type="html", list_path="/api/items")

    probe = probe_source(source)

    assert probe.outcome == "parse_error"
    assert probe.rating == "degraded"


def test_probe_timeout_is_degraded_and_uses_half_timeout(
    slow_server: Any,
) -> None:
    base_url, handler = slow_server
    source = SourceConfig(
        name="probe_slow",
        type="api",
        base_url=base_url,
        timeout_seconds=10,
        probe_timeout_seconds=0.2,
    )

    probe = probe_source(source)

    assert probe.outcome == "timeout"
    assert probe.http_status is None
    assert probe.rating == "degraded"
    assert source.effective_probe_timeout() == 0.2
    # 继承 Fetcher 的「一次固定重试」：= retries(1) + 首次，共 2 次请求。
    deadline = time.time() + 2.0
    while handler.attempts < 2 and time.time() < deadline:
        time.sleep(0.02)
    assert handler.attempts == 2


def test_probe_transport_error_is_degraded() -> None:
    # 指向未监听端口，连接被拒绝。
    source = SourceConfig(
        name="probe_refused",
        type="api",
        base_url="http://127.0.0.1:1",
        timeout_seconds=10,
    )

    probe = probe_source(source)

    assert probe.outcome == "transport_error"
    assert probe.rating == "degraded"
    assert probe.error is not None


# ---------------------------------------------------------------------------
# 评级计算阈值
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("outcome", "elapsed_ms", "history", "expected"),
    [
        ("ok", 10, SourceHealthStats(), "healthy"),
        ("ok", SLOW_PROBE_MS - 1, SourceHealthStats(), "healthy"),
        ("ok", SLOW_PROBE_MS, SourceHealthStats(), "degraded"),
        ("ok", 10, SourceHealthStats(failure_rate=FAILURE_RATE_DEGRADED), "degraded"),
        ("ok", 10, SourceHealthStats(failure_rate=FAILURE_RATE_UNHEALTHY), "unhealthy"),
        ("ok", 10, SourceHealthStats(failure_rate=0.0), "healthy"),
        (
            "ok",
            10,
            SourceHealthStats(avg_response_ms=SLOW_AVG_MS),
            "degraded",
        ),
        (
            "ok",
            10,
            SourceHealthStats(avg_response_ms=SLOW_AVG_MS - 1),
            "healthy",
        ),
        ("http_4xx", 10, SourceHealthStats(), "unhealthy"),
        ("http_4xx", 10, SourceHealthStats(failure_rate=0.0), "unhealthy"),
        ("http_5xx", 10, SourceHealthStats(), "degraded"),
        ("http_5xx", 10, SourceHealthStats(failure_rate=FAILURE_RATE_UNHEALTHY), "unhealthy"),
        ("timeout", 10, SourceHealthStats(), "degraded"),
        ("transport_error", 10, SourceHealthStats(), "degraded"),
        ("parse_error", 10, SourceHealthStats(), "degraded"),
        ("timeout", SLOW_PROBE_MS, SourceHealthStats(), "degraded"),
    ],
)
def test_rate_health_thresholds(
    outcome: Any,
    elapsed_ms: int,
    history: SourceHealthStats,
    expected: str,
) -> None:
    assert rate_health(outcome, elapsed_ms=elapsed_ms, history=history) == expected


def test_probe_history_failure_rate_lifts_rating(api_source: SourceConfig, db: Database) -> None:
    # 预置 1 成功 + 1 失败的历史，错误率 0.5 -> 即使本次探测成功也判 unhealthy。
    for index, status in enumerate(("ok", "failed")):
        db.upsert_raw(
            RawRecord(
                source=api_source.name,
                external_id=f"raw-{index}",
                url=f"http://mock/{index}",
                http_status=200 if status == "ok" else 500,
                fetched_at=datetime.now(timezone.utc),
                status=status,
                elapsed_ms=50,
            )
        )
    history = db.health_stats(api_source.name)
    assert history.total == 2
    assert history.failure_rate == 0.5

    probe = probe_source(api_source, history=history)
    assert probe.outcome == "ok"
    assert probe.rating == "unhealthy"
    assert probe.history_failure_rate == 0.5


def test_health_stats_failure_rate_and_avg(api_source: SourceConfig, db: Database) -> None:
    assert db.health_stats(api_source.name).failure_rate is None
    fetched_at = datetime.now(timezone.utc)
    for index, (status, elapsed) in enumerate((("ok", 100), ("failed", 300), ("failed", 500))):
        db.upsert_raw(
            RawRecord(
                source=api_source.name,
                external_id=f"raw-{index}",
                url=f"http://mock/{index}",
                http_status=200 if status == "ok" else 500,
                fetched_at=fetched_at,
                status=status,
                elapsed_ms=elapsed,
            )
        )
    stats = db.health_stats(api_source.name)
    assert stats.total == 3
    assert stats.failed == 2
    assert stats.failure_rate == pytest.approx(2 / 3)
    assert stats.avg_response_ms == pytest.approx(300.0)


# ---------------------------------------------------------------------------
# 请求复用：超时取一半、UA/请求头规则一致、不新增特殊请求
# ---------------------------------------------------------------------------


class _RecordingFetcher:
    """记录 get 调用的假 Fetcher，用于验证探测的请求形状。"""

    def __init__(self, result: FetchResult | None = None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls: list[dict[str, Any]] = []

    def get(
        self,
        url: str,
        params: dict[str, object] | None = None,
        *,
        timeout: float | None = None,
    ) -> FetchResult:
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        if self.error is not None:
            raise self.error
        assert self.result is not None
        return self.result

    def close(self) -> None:  # pragma: no cover - 共享 fetcher 不由探测关闭
        pass


def test_probe_uses_shared_fetcher_with_half_timeout(api_source: SourceConfig) -> None:
    fake = _RecordingFetcher(
        result=FetchResult(
            url="http://mock/api/items",
            status_code=200,
            body='{"items": [], "total": 0}',
        )
    )
    probe = probe_source(api_source, fetcher=fake)  # type: ignore[arg-type]

    assert probe.rating == "healthy"
    assert len(fake.calls) == 1
    assert fake.calls[0]["params"] == {"page": 1, "page_size": 5}
    assert fake.calls[0]["timeout"] == api_source.timeout_seconds / 2


def test_probe_shared_fetcher_maps_timeout(api_source: SourceConfig) -> None:
    fake = _RecordingFetcher(error=FetchError("timeout", timed_out=True))
    probe = probe_source(api_source, fetcher=fake)  # type: ignore[arg-type]

    assert probe.outcome == "timeout"
    assert probe.rating == "degraded"
    assert len(fake.calls) == 1


def test_probe_shared_fetcher_maps_parse_error(api_source: SourceConfig) -> None:
    fake = _RecordingFetcher(
        result=FetchResult(url="http://mock/api/items", status_code=200, body="<html>oops</html>")
    )
    probe = probe_source(api_source, fetcher=fake)  # type: ignore[arg-type]

    assert probe.outcome == "parse_error"
    assert probe.rating == "degraded"


def test_build_fetcher_reuses_source_timeout_ua_and_headers() -> None:
    source = SourceConfig(
        name="probe_headers",
        type="api",
        base_url="http://127.0.0.1:1",
        timeout_seconds=7,
        user_agent="custom-agent/1.0",
        headers={"X-Token": "abc", "Accept": "application/json"},
    )
    fetcher = build_fetcher(source)
    try:
        headers = fetcher._client.headers  # 验证统一客户端头，不自定义探测请求
        assert headers["User-Agent"] == "custom-agent/1.0"
        assert headers["X-Token"] == "abc"
        assert headers["Accept"] == "application/json"
        assert fetcher._client.timeout.read == 7  # type: ignore[attr-defined]
    finally:
        fetcher.close()


def test_effective_probe_timeout_defaults_to_half_and_can_override() -> None:
    default_source = SourceConfig(name="a", type="api", base_url="http://x.test", timeout_seconds=10)
    assert default_source.effective_probe_timeout() == 5.0

    overridden = SourceConfig(
        name="b",
        type="api",
        base_url="http://x.test",
        timeout_seconds=10,
        probe_timeout_seconds=2,
    )
    assert overridden.effective_probe_timeout() == 2.0


# ---------------------------------------------------------------------------
# 持久化：默认不写库，--persist 才写 probe_records
# ---------------------------------------------------------------------------


def test_probe_defaults_produce_no_raw_and_no_db_rows(
    api_source: SourceConfig, db: Database
) -> None:
    probes = probe_sources([api_source], db=db, persist=False)
    assert probes[0].rating == "healthy"
    assert db.count_records() == 0
    assert db.count_raw() == 0
    assert db.probe_history() == []


def test_probe_persist_writes_probe_records(api_source: SourceConfig, db: Database) -> None:
    probes = probe_sources([api_source], db=db, persist=True)
    rows = db.probe_history(api_source.name)
    assert len(rows) == 1
    assert rows[0]["rating"] == "healthy"
    assert rows[0]["outcome"] == "ok"
    assert rows[0]["http_status"] == 200
    assert rows[0]["source"] == api_source.name
    # 探测记录独立：不产生业务 raw/records。
    assert db.count_raw() == 0
    assert db.count_records() == 0


def test_probe_records_append_and_keep_history(api_source: SourceConfig, db: Database) -> None:
    for _ in range(2):
        db.insert_probe(
            ProbeRecord(
                source=api_source.name,
                probed_at=datetime.now(timezone.utc),
                outcome="ok",
                rating="healthy",
                http_status=200,
                elapsed_ms=3,
            )
        )
    assert len(db.probe_history(api_source.name)) == 2


def test_probe_records_table_created_by_init(db: Database) -> None:
    assert "probe_records" in inspect(db.engine).get_table_names()


def test_init_adds_elapsed_ms_to_legacy_raw_records(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.db"
    database = Database(db_path)
    try:
        with database.engine.begin() as connection:
            connection.execute(
                text(
                    "CREATE TABLE raw_records ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "source VARCHAR(128) NOT NULL, external_id VARCHAR(256) NOT NULL, "
                    "url VARCHAR(2048) NOT NULL, http_status INTEGER, "
                    "fetched_at DATETIME NOT NULL, status VARCHAR(16) NOT NULL, "
                    "error TEXT, raw_path VARCHAR(1024), "
                    "UNIQUE(source, external_id))"
                )
            )
            connection.execute(
                text(
                    "INSERT INTO raw_records "
                    "(source, external_id, url, fetched_at, status) "
                    "VALUES ('old', 'x', 'http://mock/x', '2024-01-01', 'ok')"
                )
            )
        database.init()
        columns = {column["name"] for column in inspect(database.engine).get_columns("raw_records")}
        assert "elapsed_ms" in columns
    finally:
        database.close()


# ---------------------------------------------------------------------------
# CLI：退出码与配置触发
# ---------------------------------------------------------------------------


def _probe_args(config: Path, *extra: str) -> list[str]:
    return ["probe", "--config", str(config), *extra]


def test_cli_probe_success_exit_zero(write_config: Any, source_payload: Any, tmp_path: Path) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = CliRunner().invoke(app, _probe_args(config_path, "--source", "demo_api"))

    assert result.exit_code == 0, result.output
    assert "healthy" in result.output
    assert "demo_api" in result.output
    # 默认不持久化：不创建数据库文件。
    assert not (tmp_path / "unused.db").exists()


def test_cli_probe_failures_do_not_abort_and_exit_zero(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config(
        [
            source_payload("broken", list_path="/api/items?fail=1"),
            source_payload("missing", list_path="/api/missing"),
        ]
    )
    result = CliRunner().invoke(app, _probe_args(config_path, "--all"))

    assert result.exit_code == 0, result.output
    assert "degraded" in result.output
    assert "unhealthy" in result.output
    assert "broken" in result.output
    assert "missing" in result.output


def test_cli_probe_persist_writes_independent_table(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "health.db"

    result = CliRunner().invoke(
        app,
        _probe_args(config_path, "--source", "demo_api", "--persist", "--db", str(db_path)),
    )
    assert result.exit_code == 0, result.output
    assert db_path.exists()

    database = Database(db_path)
    database.init()
    try:
        assert database.count_records() == 0
        assert database.count_raw() == 0
        assert len(database.probe_history("demo_api")) == 1
    finally:
        database.close()

    health = CliRunner().invoke(app, ["db", "health", "--db", str(db_path)])
    assert health.exit_code == 0, health.output
    assert "demo_api" in health.output
    assert "healthy" in health.output


def test_cli_db_health_missing_db_exits_one(tmp_path: Path) -> None:
    result = CliRunner().invoke(app, ["db", "health", "--db", str(tmp_path / "missing.db")])
    assert result.exit_code == 1


def test_cli_probe_unknown_source_exits_two(
    write_config: Any, source_payload: Any
) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = CliRunner().invoke(app, _probe_args(config_path, "--source", "nope"))
    assert result.exit_code == 2
    assert "未找到源" in result.output


def test_cli_probe_config_error_exits_two(write_config: Any, source_payload: Any) -> None:
    config_path = write_config([source_payload("bad", source_type="ftp")])
    result = CliRunner().invoke(app, _probe_args(config_path, "--all"))
    assert result.exit_code == 2
    assert "配置错误" in result.output


def test_run_with_probe_flag_logs_probe_and_still_collects(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
    caplog: Any,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    db_path = tmp_path / "run.db"
    caplog.set_level("INFO", logger="crawl_flow")

    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "2",
            "--probe",
            "--config",
            str(config_path),
            "--db",
            str(db_path),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "ok=2" in result.output
    assert any("probe source=demo_api" in record.getMessage() for record in caplog.records)


def test_run_with_no_probe_skips_probe(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
    caplog: Any,
) -> None:
    config_path = write_config([source_payload("demo_api", probe_enabled=True)])
    caplog.set_level("INFO", logger="crawl_flow")

    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "1",
            "--no-probe",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "run2.db"),
            "--raw-dir",
            str(tmp_path / "raw2"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert not any("probe source=" in record.getMessage() for record in caplog.records)


def test_run_auto_probes_when_source_config_enabled(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
    caplog: Any,
) -> None:
    config_path = write_config([source_payload("demo_api", probe_enabled=True)])
    caplog.set_level("INFO", logger="crawl_flow")

    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api",
            "--limit",
            "1",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "run3.db"),
            "--raw-dir",
            str(tmp_path / "raw3"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert any("probe source=demo_api" in record.getMessage() for record in caplog.records)


def test_config_rejects_invalid_probe_fields() -> None:
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        SourceConfig(
            name="bad",
            type="api",
            base_url="http://x.test",
            probe_timeout_seconds=0,
        )
    with pytest.raises(ValidationError):
        SourceConfig(
            name="bad2",
            type="api",
            base_url="http://x.test",
            headers={"X-Bad": "line1\nline2"},
        )


def test_config_yaml_accepts_probe_and_header_settings(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    from crawl_flow.config import load_sources

    config_path = write_config(
        [
            source_payload(
                "demo_api",
                probe_enabled=True,
                probe_timeout_seconds=1.5,
                user_agent="agent/2.0",
                headers={"X-Env": "test"},
            )
        ],
        filename="probe.yaml",
    )
    sources = load_sources(config_path)
    assert sources[0].probe_enabled is True
    assert sources[0].probe_timeout_seconds == 1.5
    assert sources[0].user_agent == "agent/2.0"
    assert sources[0].headers["X-Env"] == "test"
