"""代理池：切换重试、全部失败、配置校验、冷却恢复与管道联动。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.config import ProxyConfig, SourceConfig
from crawl_flow.errors import FetchError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.fetch.proxy_pool import ProxyPool
from crawl_flow.models import RawRecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

from tests.mock_proxy import MockProxy, dead_proxy_url


# ---------------------------------------------------------------------------
# 一、Fetcher + ProxyPool：正常切换
# ---------------------------------------------------------------------------


def test_switch_to_next_proxy_on_connection_error(
    mock_base_url: str, make_source: Any
) -> None:
    dead = dead_proxy_url()
    good = MockProxy(mode="forward").start()
    try:
        source = make_source(
            "proxy_switch",
            proxies={"urls": [dead, good.url], "failure_threshold": 1},
        )
        pool = ProxyPool(
            source.proxies.urls,
            failure_threshold=source.proxies.failure_threshold,
            cooldown_seconds=source.proxies.cooldown_seconds,
        )
        with Fetcher(timeout=5.0, proxy_pool=pool) as fetcher:
            result = fetcher.get(f"{mock_base_url}/api/items/api-1")

        assert result.status_code == 200
        assert result.proxy_used == good.url
        assert good.request_count == 1
        # 第一个代理因连接失败被计数并进入冷却。
        assert pool.state(dead).cooled_down_until is not None
        assert pool.state(good.url).consecutive_failures == 0
    finally:
        good.stop()


def test_switch_to_next_proxy_on_5xx_and_403(
    mock_base_url: str, make_source: Any
) -> None:
    bad_502 = MockProxy(mode="502").start()
    bad_403 = MockProxy(mode="403").start()
    good = MockProxy(mode="forward").start()
    try:
        source = make_source(
            "proxy_status_switch",
            proxies={
                "urls": [bad_502.url, bad_403.url, good.url],
                "failure_threshold": 1,
            },
        )
        pool = ProxyPool(
            source.proxies.urls,
            failure_threshold=1,
            cooldown_seconds=60,
        )
        with Fetcher(timeout=5.0, proxy_pool=pool) as fetcher:
            result = fetcher.get(f"{mock_base_url}/api/items/api-1")

        assert result.status_code == 200
        assert result.proxy_used == good.url
        assert bad_502.request_count == 1
        assert bad_403.request_count == 1
    finally:
        bad_502.stop()
        bad_403.stop()
        good.stop()


def test_non_retryable_404_is_returned_without_switch(
    mock_base_url: str, make_source: Any
) -> None:
    good = MockProxy(mode="forward").start()
    try:
        source = make_source(
            "proxy_404",
            proxies={"urls": [good.url], "failure_threshold": 1},
        )
        pool = ProxyPool(source.proxies.urls, failure_threshold=1, cooldown_seconds=60)
        with Fetcher(timeout=5.0, proxy_pool=pool) as fetcher:
            result = fetcher.get(f"{mock_base_url}/api/items/missing-id")

        assert result.status_code == 404
        assert result.proxy_used == good.url
        assert good.request_count == 1
        assert pool.state(good.url).consecutive_failures == 0
    finally:
        good.stop()


def test_max_three_proxies_attempted(mock_base_url: str, make_source: Any) -> None:
    # 4 个全故障代理：同一条请求只能尝试前 3 个。
    proxies = [MockProxy(mode="502").start() for _ in range(4)]
    try:
        source = make_source(
            "proxy_max_attempts",
            proxies={
                "urls": [proxy.url for proxy in proxies],
                "failure_threshold": 10,
            },
        )
        pool = ProxyPool(source.proxies.urls, failure_threshold=10, cooldown_seconds=60)
        with Fetcher(timeout=5.0, proxy_pool=pool) as fetcher:
            with pytest.raises(FetchError) as excinfo:
                fetcher.get(f"{mock_base_url}/api/items/api-1")

        assert excinfo.value.status_code == 502
        assert excinfo.value.proxy_used == proxies[2].url
        assert "attempts=3" in str(excinfo.value)
        assert [proxy.request_count for proxy in proxies] == [1, 1, 1, 0]
    finally:
        for proxy in proxies:
            proxy.stop()


# ---------------------------------------------------------------------------
# 二、所有代理均失败
# ---------------------------------------------------------------------------


def test_all_proxies_failed_raises_fetch_error(
    mock_base_url: str, make_source: Any
) -> None:
    proxies = [MockProxy(mode="502").start() for _ in range(2)]
    try:
        source = make_source(
            "proxy_all_failed",
            proxies={
                "urls": [proxy.url for proxy in proxies],
                "failure_threshold": 1,
            },
        )
        pool = ProxyPool(source.proxies.urls, failure_threshold=1, cooldown_seconds=60)
        with Fetcher(timeout=5.0, proxy_pool=pool) as fetcher:
            with pytest.raises(FetchError) as excinfo:
                fetcher.get(f"{mock_base_url}/api/items/api-1")

        error = excinfo.value
        assert error.status_code == 502
        assert error.proxy_used == proxies[-1].url
        assert "所有代理均请求失败" in str(error)
        for proxy in proxies:
            assert proxy.request_count == 1
    finally:
        for proxy in proxies:
            proxy.stop()


def test_all_proxies_connection_error_raises_fetch_error(
    mock_base_url: str, make_source: Any
) -> None:
    urls = [dead_proxy_url(), dead_proxy_url()]
    source = make_source(
        "proxy_dead",
        proxies={"urls": urls, "failure_threshold": 1},
    )
    pool = ProxyPool(source.proxies.urls, failure_threshold=1, cooldown_seconds=60)
    with Fetcher(timeout=2.0, proxy_pool=pool) as fetcher:
        with pytest.raises(FetchError) as excinfo:
            fetcher.get(f"{mock_base_url}/api/items/api-1")

    assert excinfo.value.status_code is None
    assert excinfo.value.proxy_used == urls[-1]
    assert "所有代理均请求失败" in str(excinfo.value)


def test_pipeline_marks_failed_when_all_proxies_fail(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    # 列表页照常转发，详情请求由代理直接回 502：验证单条错误隔离不被代理改变。
    bad = MockProxy(mode="detail-502").start()
    try:
        source = make_source(
            "demo_api_proxy_fail",
            detail_path="/api/items/{id}?detail-fail=1",
            proxies={"urls": [bad.url], "failure_threshold": 1},
        )
        summary = run_source(source, db, raw_store, limit=2)

        assert (summary.ok, summary.failed) == (0, 2)
        assert db.count_raw("demo_api_proxy_fail", "failed") == 2

        with Session(db.engine) as session:
            rows = list(
                session.execute(select(RawRecordRow)).scalars()
            )
        assert len(rows) == 2
        for row in rows:
            assert row.status == "failed"
            assert row.http_status == 502
            assert row.proxy_used == bad.url
            payload = json.loads(Path(row.raw_path).read_text(encoding="utf-8"))
            assert payload["proxy_used"] == bad.url
            assert payload["status"] == 502
    finally:
        bad.stop()


# ---------------------------------------------------------------------------
# 三、SourceConfig / ProxyConfig 严格校验
# ---------------------------------------------------------------------------


def test_invalid_proxy_protocol_rejected() -> None:
    with pytest.raises(ValidationError) as excinfo:
        ProxyConfig(urls=["ftp://127.0.0.1:21"])
    assert "协议必须是" in str(excinfo.value)


def test_proxy_url_missing_host_or_port_rejected() -> None:
    with pytest.raises(ValidationError):
        ProxyConfig(urls=["http://127.0.0.1"])
    with pytest.raises(ValidationError):
        ProxyConfig(urls=["socks5://:1080"])


def test_duplicate_and_empty_proxy_urls_rejected() -> None:
    with pytest.raises(ValidationError, match="重复"):
        ProxyConfig(urls=["http://127.0.0.1:8081", "http://127.0.0.1:8081"])
    with pytest.raises(ValidationError):
        ProxyConfig(urls=["  "])


def test_invalid_threshold_and_cooldown_rejected() -> None:
    with pytest.raises(ValidationError):
        ProxyConfig(urls=["http://127.0.0.1:8081"], failure_threshold=0)
    with pytest.raises(ValidationError):
        ProxyConfig(urls=["http://127.0.0.1:8081"], cooldown_seconds=0)
    with pytest.raises(ValidationError):
        ProxyConfig(urls=[])


def test_unknown_proxy_field_rejected_by_strict_config() -> None:
    with pytest.raises(ValidationError):
        ProxyConfig(
            urls=["http://127.0.0.1:8081"],
            unknown_knob=True,  # type: ignore[call-arg]
        )


def test_source_config_accepts_supported_proxy_schemes(
    mock_base_url: str, make_source: Any
) -> None:
    source = make_source(
        "proxy_schemes",
        proxies={
            "urls": [
                "http://127.0.0.1:8081",
                "https://127.0.0.1:8082",
                "socks5://127.0.0.1:1080",
            ],
            "failure_threshold": 2,
            "cooldown_seconds": 30,
        },
    )
    assert source.proxies is not None
    assert source.proxies.failure_threshold == 2
    assert source.proxies.cooldown_seconds == 30


def test_fetcher_builds_clients_for_all_supported_schemes(make_source: Any) -> None:
    source = make_source(
        "proxy_client_schemes",
        proxies={
            "urls": [
                "http://127.0.0.1:8081",
                "https://127.0.0.1:8082",
                "socks5://127.0.0.1:1080",
            ],
        },
    )
    pool = ProxyPool(source.proxies.urls, failure_threshold=3, cooldown_seconds=60)
    with Fetcher(timeout=5.0, proxy_pool=pool) as fetcher:
        for proxy_url in source.proxies.urls:
            # 构建不发起连接；socks5 依赖 socksio，缺失时这里会抛错。
            client = fetcher._client_for(proxy_url)
            assert client.is_closed is False


def test_cli_invalid_proxy_config_exits_2(
    write_config: Any, source_payload: Any, tmp_path: Path
) -> None:
    config_path = write_config(
        [
            source_payload(
                "demo_api_bad_proxy",
                proxies={"urls": ["ftp://127.0.0.1:21"]},
            )
        ]
    )
    result = CliRunner().invoke(
        app,
        [
            "run",
            "--source",
            "demo_api_bad_proxy",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "bad.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 2
    assert "配置错误" in result.output


# ---------------------------------------------------------------------------
# 四、冷却到期自动恢复
# ---------------------------------------------------------------------------


class FakeClock:
    """可手动推进的单调时钟。"""

    def __init__(self) -> None:
        self.current = 1000.0

    def __call__(self) -> float:
        return self.current

    def advance(self, seconds: float) -> None:
        self.current += seconds


def test_proxy_cools_down_and_recovers_after_ttl() -> None:
    clock = FakeClock()
    pool = ProxyPool(
        ["http://127.0.0.1:8081", "http://127.0.0.1:8082"],
        failure_threshold=2,
        cooldown_seconds=60,
        clock=clock,
    )

    # 连续失败达到阈值：进入冷却并被 acquire_order 跳过。
    pool.report_failure("http://127.0.0.1:8081")
    assert pool.state("http://127.0.0.1:8081").consecutive_failures == 1
    pool.report_failure("http://127.0.0.1:8081")
    cooled = pool.state("http://127.0.0.1:8081")
    assert cooled.cooled_down_until == 1060.0
    assert pool.acquire_order() == ["http://127.0.0.1:8082"]

    # 冷却期内不可用。
    clock.advance(59)
    assert pool.acquire_order() == ["http://127.0.0.1:8082"]

    # 到期后自动恢复。
    clock.advance(2)
    assert pool.acquire_order() == [
        "http://127.0.0.1:8081",
        "http://127.0.0.1:8082",
    ]
    recovered = pool.state("http://127.0.0.1:8081")
    assert recovered.is_available(clock())
    assert recovered.consecutive_failures == 0


def test_success_resets_failure_count() -> None:
    pool = ProxyPool(
        ["http://127.0.0.1:8081"], failure_threshold=2, cooldown_seconds=60
    )
    pool.report_failure("http://127.0.0.1:8081")
    pool.report_success("http://127.0.0.1:8081")
    assert pool.state("http://127.0.0.1:8081").consecutive_failures == 0


def test_recovered_proxy_serves_request_again(
    mock_base_url: str, make_source: Any
) -> None:
    clock = FakeClock()
    good = MockProxy(mode="forward").start()
    try:
        pool = ProxyPool(
            [good.url], failure_threshold=1, cooldown_seconds=30, clock=clock
        )
        with Fetcher(timeout=5.0, proxy_pool=pool) as fetcher:
            # 模拟一次失败把代理打入冷却。
            pool.report_failure(good.url)
            assert pool.acquire_order() == [good.url]  # 全冷却时兜底返回

            clock.advance(31)
            result = fetcher.get(f"{mock_base_url}/api/items/api-1")
            assert result.status_code == 200
            assert result.proxy_used == good.url
            assert pool.state(good.url).consecutive_failures == 0
    finally:
        good.stop()


# ---------------------------------------------------------------------------
# 五、管道与存储联动：proxy_used 落 raw_records 与快照，stats 不受影响
# ---------------------------------------------------------------------------


def test_pipeline_records_proxy_used_on_success(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    good = MockProxy(mode="forward").start()
    try:
        source = make_source(
            "demo_api_proxied",
            detail_path="/api/items/{id}",
            proxies={"urls": [good.url], "failure_threshold": 2},
        )
        summary = run_source(source, db, raw_store, limit=2)

        assert (summary.ok, summary.failed) == (2, 0)
        with Session(db.engine) as session:
            rows = list(
                session.execute(
                    select(RawRecordRow).order_by(RawRecordRow.external_id)
                ).scalars()
            )
        assert len(rows) == 2
        for row in rows:
            assert row.proxy_used == good.url
            payload = json.loads(Path(row.raw_path).read_text(encoding="utf-8"))
            assert payload["proxy_used"] == good.url

        # db stats 的统计口径不包含代理字段，结果结构与口径保持不变。
        stats = db.stats()
        assert stats["totals"] == {"records": 2, "raw_ok": 2, "raw_failed": 0}
        assert stats["sources"][0]["source"] == "demo_api_proxied"
    finally:
        good.stop()


def test_dry_run_with_proxy_writes_nothing(
    db: Database,
    raw_store: RawStore,
    make_source: Any,
) -> None:
    good = MockProxy(mode="forward").start()
    try:
        source = make_source(
            "demo_api_proxy_dry",
            detail_path="/api/items/{id}",
            proxies={"urls": [good.url]},
        )
        summary = run_source(source, db, raw_store, limit=2, dry_run=True)
        assert (summary.ok, summary.failed) == (2, 0)
        assert db.count_raw() == 0
        assert db.count_records() == 0
        assert not any(raw_store.root.rglob("*.json"))
    finally:
        good.stop()


def test_direct_source_keeps_null_proxy_used(
    api_source: SourceConfig,
    db: Database,
    raw_store: RawStore,
) -> None:
    # 未配置代理的源：完全直连，proxy_used 始终为 None。
    run_source(api_source, db, raw_store, limit=2)
    with Session(db.engine) as session:
        rows = list(session.execute(select(RawRecordRow)).scalars())
    assert len(rows) == 2
    assert all(row.proxy_used is None for row in rows)
    for row in rows:
        payload = json.loads(Path(row.raw_path).read_text(encoding="utf-8"))
        assert payload["proxy_used"] is None
