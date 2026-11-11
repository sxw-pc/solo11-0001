"""指标 HTTP 端点：仅本机访问、路径行为、内容格式。"""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from crawl_flow.observability.metrics_server import MetricsServer


@pytest.fixture
def server() -> Iterator[MetricsServer]:
    instance = MetricsServer("127.0.0.1", 0, lambda: "# TYPE demo_metric gauge\ndemo_metric 1\n")
    instance.start()
    try:
        yield instance
    finally:
        instance.stop()


def test_metrics_endpoint_returns_prometheus_text(server: MetricsServer) -> None:
    response = httpx.get(f"http://127.0.0.1:{server.port}/metrics", timeout=5)
    assert response.status_code == 200
    assert response.headers["Content-Type"].startswith("text/plain")
    assert "version=0.0.4" in response.headers["Content-Type"]
    assert "demo_metric 1" in response.text


def test_healthz(server: MetricsServer) -> None:
    response = httpx.get(f"http://127.0.0.1:{server.port}/healthz", timeout=5)
    assert response.status_code == 200
    assert response.text == "ok\n"


def test_unknown_path_is_404(server: MetricsServer) -> None:
    response = httpx.get(f"http://127.0.0.1:{server.port}/config", timeout=5)
    assert response.status_code == 404
    response = httpx.get(f"http://127.0.0.1:{server.port}/", timeout=5)
    assert response.status_code == 404


def test_server_binds_loopback_only(server: MetricsServer) -> None:
    assert server._server.server_address[0] == "127.0.0.1"


def test_render_called_per_request() -> None:
    values = iter(["first 1\n", "second 1\n"])
    instance = MetricsServer("127.0.0.1", 0, lambda: next(values))
    instance.start()
    try:
        assert httpx.get(f"http://127.0.0.1:{instance.port}/metrics", timeout=5).text == "first 1\n"
        assert httpx.get(f"http://127.0.0.1:{instance.port}/metrics", timeout=5).text == "second 1\n"
    finally:
        instance.stop()
