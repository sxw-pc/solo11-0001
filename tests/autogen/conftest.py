"""autogen 测试公共夹具：博客 mock 站点、零限速客户端、临时样本库。"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from pathlib import Path

import pytest
from werkzeug.serving import make_server

from mock_site.blog.app import create_app

from crawl_flow.autogen.limiter import reset_global_limiter
from crawl_flow.autogen.polite import PoliteFetcher
from crawl_flow.autogen.sample_store import SampleStore
from crawl_flow.fetch.client import Fetcher

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def blog_server() -> Iterator[str]:
    """随机端口启动 v1 结构的博客 mock 站点，返回根地址。"""

    app = create_app(default_variant="v1")
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture
def blog_app():
    """直接返回 Flask app（测试内可改写 BLOG_VARIANT 模拟改版）。"""

    return create_app(default_variant="v1")


@pytest.fixture
def blog_server_factory():
    """按指定变体启动站点的工厂。"""

    created: list[tuple[object, object]] = []

    def _start(variant: str = "v1") -> tuple[str, object]:
        app = create_app(default_variant=variant)
        server = make_server("127.0.0.1", 0, app)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        created.append((server, thread))
        return f"http://127.0.0.1:{server.server_port}", app

    yield _start
    for server, thread in created:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture(autouse=True)
def _zero_rate_limiter():
    """默认把全局限速间隔调为 0，保证测试快速；速率约束另用专门测试验证。"""

    reset_global_limiter(min_interval_seconds=0.0)
    yield


@pytest.fixture
def polite_fetcher() -> Iterator[PoliteFetcher]:
    with PoliteFetcher(fetcher=Fetcher(timeout=5.0)) as fetcher:
        yield fetcher


@pytest.fixture
def sample_store(tmp_path: Path) -> SampleStore:
    return SampleStore(tmp_path / "samples")


@pytest.fixture
def sample_root(tmp_path: Path) -> Path:
    return tmp_path / "samples"
