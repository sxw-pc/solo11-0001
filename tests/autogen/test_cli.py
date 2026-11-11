"""autogen CLI：生成、校验退出码与失败原因、样本查看。"""

from __future__ import annotations

import json
import threading
from pathlib import Path

import pytest
from typer.testing import CliRunner
from werkzeug.serving import make_server

from crawl_flow.autogen.limiter import reset_global_limiter
from crawl_flow.cli import app
from mock_site.blog.app import create_app

runner = CliRunner()


@pytest.fixture
def blog_url() -> str:
    server = make_server("127.0.0.1", 0, create_app("v1"))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def test_cli_generate_writes_spec_and_source(blog_url: str, tmp_path: Path) -> None:
    reset_global_limiter(min_interval_seconds=0.0)
    out = tmp_path / "generated"
    result = runner.invoke(
        app,
        [
            "autogen",
            "generate",
            blog_url + "/posts",
            "--field",
            "title",
            "--field",
            "content",
            "--field",
            "published_at",
            "--field",
            "category",
            "--field",
            "tags",
            "--output",
            str(out),
            "--sample-root",
            str(tmp_path / "samples"),
            "--min-interval",
            "0",
        ],
    )
    assert result.exit_code == 0, result.output
    specs = list(out.glob("*.spec.json"))
    sources = list(out.glob("*.source.yaml"))
    assert len(specs) == 1 and len(sources) == 1
    payload = json.loads(specs[0].read_text(encoding="utf-8"))
    assert payload["engine"] == "generic_html"


def test_cli_unsupported_site_exit_code_and_reason(blog_url: str, tmp_path: Path) -> None:
    reset_global_limiter(min_interval_seconds=0.0)
    result = runner.invoke(
        app,
        [
            "autogen",
            "generate",
            blog_url + "/login-required/posts",
            "--field",
            "title",
            "--output",
            str(tmp_path / "out"),
            "--sample-root",
            str(tmp_path / "samples"),
            "--min-interval",
            "0",
        ],
    )
    assert result.exit_code == 3
    assert "不支持的站点" in result.output


def test_cli_sandbox_violation_exit_code(blog_url: str, tmp_path: Path) -> None:
    reset_global_limiter(min_interval_seconds=0.0)
    result = runner.invoke(
        app,
        [
            "autogen",
            "generate",
            blog_url + "/evil/posts",
            "--field",
            "title",
            "--output",
            str(tmp_path / "out"),
            "--sample-root",
            str(tmp_path / "samples"),
            "--min-interval",
            "0",
        ],
    )
    assert result.exit_code == 4
    assert "沙箱" in result.output


def test_cli_validate_healthy_then_drifted(blog_url: str, tmp_path: Path) -> None:
    reset_global_limiter(min_interval_seconds=0.0)
    out = tmp_path / "generated"
    runner.invoke(
        app,
        [
            "autogen",
            "generate",
            blog_url + "/posts",
            "--field",
            "title",
            "--field",
            "content",
            "--output",
            str(out),
            "--sample-root",
            str(tmp_path / "samples"),
            "--min-interval",
            "0",
        ],
        catch_exceptions=False,
    )
    spec_file = next(out.glob("*.spec.json"))

    healthy = runner.invoke(
        app,
        ["autogen", "validate", str(spec_file), "--min-interval", "0"],
        catch_exceptions=False,
    )
    assert healthy.exit_code == 0

    # 起一个 v2 结构的站点，用 --entry-url 指向它模拟改版
    server_v2 = make_server("127.0.0.1", 0, create_app("v2"))
    threading.Thread(target=server_v2.serve_forever, daemon=True).start()
    v2_base = f"http://127.0.0.1:{server_v2.server_port}"
    try:
        drifted = runner.invoke(
            app,
            [
                "autogen",
                "validate",
                str(spec_file),
                "--entry-url",
                v2_base + "/posts",
                "--min-interval",
                "0",
            ],
            catch_exceptions=False,
        )
        assert drifted.exit_code == 6
        assert "调整建议" in drifted.output
    finally:
        server_v2.shutdown()


def test_cli_samples_command(blog_url: str, tmp_path: Path) -> None:
    reset_global_limiter(min_interval_seconds=0.0)
    sample_root = tmp_path / "samples"
    runner.invoke(
        app,
        [
            "autogen",
            "generate",
            blog_url + "/posts",
            "--field",
            "title",
            "--field",
            "content",
            "--output",
            str(tmp_path / "out"),
            "--sample-root",
            str(sample_root),
            "--min-interval",
            "0",
        ],
        catch_exceptions=False,
    )
    result = runner.invoke(
        app,
        ["autogen", "samples", blog_url + "/posts", "--sample-root", str(sample_root)],
    )
    assert result.exit_code == 0
    assert "保留版本" in result.output
