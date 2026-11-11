"""配置中的 ``${ENV_VAR}`` 环境变量占位与受保护字段校验。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from crawl_flow.cli import app
from crawl_flow.errors import ConfigError

runner = CliRunner()


def test_load_sources_expands_non_enum_string_fields(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [
            source_payload(
                "demo_api",
                base_url="${MOCK_BASE_URL}/v1",
                timeout_seconds="${TIMEOUT}",
                page_size="${PAGE_SIZE}",
                list_path="${LIST_PATH}",
                detail_path="/prefix/${DETAIL_TOKEN}/{id}",
                field_map={"title": "title", "url": "${URL_FIELD}"},
            )
        ]
    )
    # 显式传入 environ，保证测试不依赖机器上的环境变量。
    environ = {
        "MOCK_BASE_URL": "http://127.0.0.1:8010",
        "TIMEOUT": "3.5",
        "PAGE_SIZE": "7",
        "LIST_PATH": "/custom/items",
        "DETAIL_TOKEN": "items",
        "URL_FIELD": "link",
    }

    from crawl_flow.config import load_sources

    [source] = load_sources(config_path, environ=environ)

    assert source.base_url == "http://127.0.0.1:8010/v1"
    assert source.timeout_seconds == 3.5
    assert source.page_size == 7
    assert source.list_path == "/custom/items"
    assert source.detail_path == "/prefix/items/{id}"
    assert source.field_map.url == "link"


def test_one_string_can_reference_multiple_variables(
    write_config: Any,
    source_payload: Any,
) -> None:
    config_path = write_config(
        [source_payload("demo_api", base_url="${PROTO}://${HOST}:${PORT}")]
    )

    from crawl_flow.config import load_sources

    [source] = load_sources(
        config_path,
        environ={"PROTO": "https", "HOST": "example.test", "PORT": "8443"},
    )
    assert source.base_url == "https://example.test:8443"


def test_missing_env_variable_raises_config_error_with_field_path(
    write_config: Any,
    source_payload: Any,
) -> None:
    config_path = write_config(
        [source_payload("demo_api", base_url="http://${MISSING_HOST}")]
    )

    from crawl_flow.config import load_sources

    with pytest.raises(ConfigError) as excinfo:
        load_sources(config_path, environ={})
    message = str(excinfo.value)
    assert "MISSING_HOST" in message
    assert "sources[1].base_url" in message


def test_missing_env_variable_from_cli_exits_2(
    write_config: Any,
    source_payload: Any,
    tmp_path: Path,
) -> None:
    config_path = write_config(
        [source_payload("demo_api", base_url="http://${CRAWL_FLOW_UNSET_BASE_URL}")]
    )

    result = runner.invoke(
        app,
        [
            "run",
            "--all",
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
    assert "CRAWL_FLOW_UNSET_BASE_URL" in result.output


def test_placeholder_uses_process_environment(
    write_config: Any,
    source_payload: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config_path = write_config([source_payload("demo_api", base_url="${CRAWL_FLOW_BASE}")])
    monkeypatch.setenv("CRAWL_FLOW_BASE", "http://127.0.0.1:8010")

    from crawl_flow.config import ENV_PLACEHOLDER, load_sources

    # 不调用任何 shell；不同平台都按同一个正则和 os.environ 映射解析。
    assert ENV_PLACEHOLDER.findall("${A} ${B_2} ${1_INVALID}") == ["A", "B_2"]
    [source] = load_sources(config_path)
    assert source.base_url == "http://127.0.0.1:8010"


def test_empty_env_value_is_present_but_fails_existing_validation(
    write_config: Any,
    source_payload: Any,
) -> None:
    config_path = write_config([source_payload("demo_api", base_url="${EMPTY_BASE}")])

    from crawl_flow.config import load_sources

    with pytest.raises(ConfigError) as excinfo:
        load_sources(config_path, environ={"EMPTY_BASE": ""})
    # 变量存在，不应报“环境变量缺失”；仍走原有 pydantic 校验。
    assert "环境变量缺失" not in str(excinfo.value)
    assert "校验失败" in str(excinfo.value)


@pytest.mark.parametrize("field", ["name", "type"])
def test_enum_like_fields_reject_placeholders(
    write_config: Any,
    source_payload: Any,
    field: str,
) -> None:
    payload = source_payload("demo_api")
    payload[field] = "${PROTECTED}"
    config_path = write_config([payload])

    from crawl_flow.config import load_sources

    with pytest.raises(ConfigError) as excinfo:
        load_sources(config_path, environ={"PROTECTED": "demo_api"})
    message = str(excinfo.value)
    assert "占位符" in message
    assert field in message


def test_extra_forbid_still_rejects_unknown_fields_after_expansion(
    write_config: Any,
    source_payload: Any,
) -> None:
    payload = source_payload("demo_api", unknown_field="${UNKNOWN_VALUE}")
    config_path = write_config([payload])

    from crawl_flow.config import load_sources

    with pytest.raises(ConfigError) as excinfo:
        load_sources(config_path, environ={"UNKNOWN_VALUE": "x"})
    assert "校验失败" in str(excinfo.value)
    assert "unknown_field" in str(excinfo.value)
