"""pytest 公共夹具：随机端口 mock 站点、临时数据库、配置工厂。"""

from __future__ import annotations

import sys
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml
from werkzeug.serving import make_server

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from mock_site.app import create_app  # noqa: E402

from crawl_flow.config import SourceConfig  # noqa: E402
from crawl_flow.cursor.store import CursorStore  # noqa: E402
from crawl_flow.fetch.client import Fetcher  # noqa: E402
from crawl_flow.storage.db import Database  # noqa: E402
from crawl_flow.storage.raw_store import RawStore  # noqa: E402


@pytest.fixture(scope="session")
def mock_base_url() -> Iterator[str]:
    """在 127.0.0.1 的随机端口起 mock 站点，整个测试会话复用。"""

    app = create_app(REPO_ROOT / "mock_site" / "fixtures")
    server = make_server("127.0.0.1", 0, app)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)


@pytest.fixture
def fetcher() -> Iterator[Fetcher]:
    with Fetcher(timeout=5.0) as client:
        yield client


@pytest.fixture
def api_source(mock_base_url: str) -> SourceConfig:
    return SourceConfig(
        name="demo_api",
        type="api",
        base_url=mock_base_url,
        page_size=5,
    )


@pytest.fixture
def html_source(mock_base_url: str) -> SourceConfig:
    return SourceConfig(
        name="demo_html",
        type="html",
        base_url=mock_base_url,
        page_size=5,
    )


@pytest.fixture
def make_source(mock_base_url: str) -> Any:
    def _make(name: str = "demo_api", source_type: str = "api", **overrides: Any) -> SourceConfig:
        payload: dict[str, Any] = {
            "name": name,
            "type": source_type,
            "base_url": mock_base_url,
        }
        payload.update(overrides)
        return SourceConfig.model_validate(payload)

    return _make


@pytest.fixture
def db(tmp_path: Path) -> Iterator[Database]:
    database = Database(tmp_path / "crawl.db")
    database.init()
    try:
        yield database
    finally:
        database.close()


@pytest.fixture
def raw_store(tmp_path: Path) -> RawStore:
    return RawStore(tmp_path / "raw")


@pytest.fixture
def cursor_store(tmp_path: Path) -> CursorStore:
    return CursorStore(tmp_path / "cursors")


@pytest.fixture
def write_config(tmp_path: Path) -> Any:
    def _write(sources: list[dict[str, Any]], filename: str = "sources.yaml") -> Path:
        path = tmp_path / filename
        path.write_text(
            yaml.safe_dump({"sources": sources}, allow_unicode=True, sort_keys=False),
            encoding="utf-8",
        )
        return path

    return _write


@pytest.fixture
def source_payload(mock_base_url: str) -> Any:
    def _payload(name: str = "demo_api", source_type: str = "api", **overrides: Any) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": name,
            "type": source_type,
            "base_url": mock_base_url,
        }
        payload.update(overrides)
        return payload

    return _payload
