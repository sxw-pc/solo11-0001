"""ConfigWatcher：标准库轮询、成功重载与失败保留旧配置。"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

from crawl_flow.config import SourceConfig, load_sources
from crawl_flow.config_watch import ConfigWatcher


def _write_sources(path: Path, sources: list[dict[str, Any]]) -> Path:
    path.write_text(
        yaml.safe_dump({"sources": sources}, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return path


def test_poll_returns_none_without_file_change(
    write_config: Any,
    source_payload: Any,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    watcher = ConfigWatcher(config_path)

    assert [source.name for source in watcher.load()] == ["demo_api"]
    assert watcher.poll() is None
    assert [source.name for source in watcher.sources] == ["demo_api"]


def test_poll_reloads_added_and_modified_enabled_sources(
    tmp_path: Path,
    mock_base_url: str,
) -> None:
    config_path = tmp_path / "sources.yaml"

    def payload(name: str, **overrides: Any) -> dict[str, Any]:
        item: dict[str, Any] = {"name": name, "type": "api", "base_url": mock_base_url}
        item.update(overrides)
        return item

    _write_sources(config_path, [payload("demo_api", page_size=5)])
    watcher = ConfigWatcher(config_path)
    watcher.load()

    _write_sources(
        config_path,
        [
            payload("demo_api", page_size=10),
            payload("demo_api_new"),
            payload("demo_api_disabled", enabled=False),
        ],
    )

    reloaded = watcher.poll()
    assert reloaded is not None
    by_name = {source.name: source for source in reloaded}
    assert set(by_name) == {"demo_api", "demo_api_new", "demo_api_disabled"}
    assert by_name["demo_api"].page_size == 10
    assert by_name["demo_api_new"].enabled is True
    assert by_name["demo_api_disabled"].enabled is False
    # --all 的既有选择逻辑只运行 enabled 源。
    enabled = [source.name for source in reloaded if source.enabled]
    assert enabled == ["demo_api", "demo_api_new"]


def test_reload_failure_keeps_previous_config_and_logs_warning(
    tmp_path: Path,
    mock_base_url: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config_path = tmp_path / "sources.yaml"
    _write_sources(
        config_path,
        [{"name": "demo_api", "type": "api", "base_url": mock_base_url}],
    )
    watcher = ConfigWatcher(config_path)
    watcher.load()

    config_path.write_text("sources: [\n", encoding="utf-8")  # 非法 YAML

    with caplog.at_level(logging.WARNING):
        reloaded = watcher.poll()

    assert reloaded is None
    assert [source.name for source in watcher.sources] == ["demo_api"]
    assert "config reload failed" in caplog.text
    assert "keep_previous=true" in caplog.text


def test_missing_config_file_does_not_overwrite_existing_sources(
    tmp_path: Path,
    mock_base_url: str,
) -> None:
    config_path = tmp_path / "sources.yaml"
    _write_sources(
        config_path,
        [{"name": "demo_api", "type": "api", "base_url": mock_base_url}],
    )
    watcher = ConfigWatcher(config_path)
    watcher.load()

    config_path.unlink()
    assert watcher.poll() is None
    assert [source.name for source in watcher.sources] == ["demo_api"]


def test_same_content_rewrite_is_not_reloaded(tmp_path: Path, mock_base_url: str) -> None:
    config_path = tmp_path / "sources.yaml"
    text = yaml.safe_dump(
        {"sources": [{"name": "demo_api", "type": "api", "base_url": mock_base_url}]},
        sort_keys=False,
    )
    config_path.write_text(text, encoding="utf-8")

    calls: list[Path] = []

    def counting_loader(path: Path) -> list[SourceConfig]:
        calls.append(Path(path))
        return load_sources(path)

    watcher = ConfigWatcher(config_path, loader=counting_loader)
    watcher.load()

    # 即使 mtime 变化，内容哈希不变也不应触发 loader。
    config_path.write_text(text, encoding="utf-8")
    assert watcher.poll() is None
    assert len(calls) == 1


def test_watcher_module_only_imports_standard_library_and_internal_modules() -> None:
    import ast
    from pathlib import Path as TestPath

    module_path = TestPath(__file__).resolve().parents[1] / (
        "src/crawl_flow/config_watch.py"
    )
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.append(node.module)

    assert imported
    for name in imported:
        root = name.split(".", 1)[0]
        assert root == "crawl_flow" or root in sys.stdlib_module_names
