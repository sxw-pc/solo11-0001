"""CLI --watch 参数解析与首次配置失败的退出码。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

import crawl_flow.cli as cli_module
from crawl_flow.cli import app
from crawl_flow.pipeline.run import RunSummary

runner = CliRunner()


def test_cli_watch_enters_watch_loop_and_prints_round(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    received: dict[str, Any] = {}

    def fake_watch_loop(
        watcher: Any,
        db: Any,
        raw_store: Any,
        fetcher: Any,
        **kwargs: Any,
    ) -> list[RunSummary]:
        received["watcher"] = watcher
        received["db"] = db
        received["raw_store"] = raw_store
        received["fetcher"] = fetcher
        received.update(kwargs)
        summaries = [RunSummary(source="demo_api", ok=1)]
        kwargs["on_round"](1, summaries)
        return summaries

    monkeypatch.setattr(cli_module, "run_watch_loop", fake_watch_loop)

    result = runner.invoke(
        app,
        [
            "run",
            "--all",
            "--watch",
            "--watch-interval",
            "0.1",
            "--limit",
            "1",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "watch.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )

    assert result.exit_code == 0, result.output
    assert [source.name for source in received["watcher"].sources] == ["demo_api"]
    assert received["poll_interval"] == 0.1
    assert received["limit"] == 1
    assert "第 1 轮汇总" in result.output
    assert "source=demo_api ok=1" in result.output


def test_cli_watch_initial_config_error_exits_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [source_payload("demo_api", base_url="http://${WATCH_UNSET_VARIABLE}")]
    )

    result = runner.invoke(
        app,
        [
            "run",
            "--all",
            "--watch",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "watch.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )

    assert result.exit_code == 2
    assert "配置错误" in result.output
    assert "WATCH_UNSET_VARIABLE" in result.output


def test_cli_watch_requires_source_selector(tmp_path: Path, write_config: Any, source_payload: Any) -> None:
    config_path = write_config([source_payload("demo_api")])
    result = runner.invoke(
        app,
        [
            "run",
            "--watch",
            "--config",
            str(config_path),
            "--db",
            str(tmp_path / "watch.db"),
            "--raw-dir",
            str(tmp_path / "raw"),
        ],
    )
    assert result.exit_code == 2
    assert "必须指定 --source 或 --all" in result.output
