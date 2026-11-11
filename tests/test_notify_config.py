"""通知配置校验：非法 webhook、notify 段类型与默认值。"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from crawl_flow.config import (
    DEFAULT_ALERT_FAILURE_RATE,
    DEFAULT_NOTIFY_RETRIES,
    DEFAULT_NOTIFY_TIMEOUT_SECONDS,
    NotifySettings,
    load_config,
)
from crawl_flow.errors import ConfigError


def test_notify_defaults_when_section_absent(
    write_config: Any,
    source_payload: Any,
) -> None:
    config_path = write_config([source_payload("demo_api")])
    loaded = load_config(config_path)
    assert loaded.notify.webhooks == []
    assert loaded.notify.timeout_seconds == DEFAULT_NOTIFY_TIMEOUT_SECONDS
    assert loaded.notify.retries == DEFAULT_NOTIFY_RETRIES
    assert loaded.notify.alert_failure_rate == DEFAULT_ALERT_FAILURE_RATE
    assert all(source.notify is None for source in loaded.sources)


def test_global_notify_section_is_parsed(
    tmp_path: Path,
    source_payload: Any,
    mock_base_url: str,
) -> None:
    import yaml

    config_path = tmp_path / "sources.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "notify": {
                    "webhooks": ["http://127.0.0.1:9000/hook/a"],
                    "timeout_seconds": 3.0,
                    "retries": 4,
                    "alert_failure_rate": 0.25,
                },
                "sources": [source_payload("demo_api")],
            }
        ),
        encoding="utf-8",
    )
    loaded = load_config(config_path)
    assert loaded.notify.webhooks == ["http://127.0.0.1:9000/hook/a"]
    assert loaded.notify.timeout_seconds == 3.0
    assert loaded.notify.retries == 4
    assert loaded.notify.alert_failure_rate == 0.25


def test_source_notify_override_is_parsed(
    write_config: Any,
    source_payload: Any,
) -> None:
    payload = source_payload(
        "demo_api",
        notify={"webhooks": ["http://127.0.0.1:9000/oncall"]},
    )
    loaded = load_config(write_config([payload]))
    source = loaded.sources[0]
    assert source.notify is not None
    assert source.notify.webhooks == ["http://127.0.0.1:9000/oncall"]


@pytest.mark.parametrize(
    "bad_url",
    ["ftp://127.0.0.1/hook", "not-a-url", "http://", ""],
)
def test_invalid_global_webhook_rejected(
    tmp_path: Path,
    source_payload: Any,
    bad_url: str,
) -> None:
    import yaml

    config_path = tmp_path / "bad.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "notify": {"webhooks": [bad_url]},
                "sources": [source_payload("demo_api")],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="notify 校验失败"):
        load_config(config_path)


def test_invalid_source_webhook_rejected(
    write_config: Any,
    source_payload: Any,
) -> None:
    payload = source_payload("demo_api", notify={"webhooks": ["gopher://x/y"]})
    with pytest.raises(ConfigError, match="sources\\[1\\] 校验失败"):
        load_config(write_config([payload]))


def test_notify_section_wrong_type_raises_config_error(
    tmp_path: Path,
    source_payload: Any,
) -> None:
    import yaml

    config_path = tmp_path / "bad.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {"notify": ["should", "be", "mapping"], "sources": [source_payload("demo_api")]}
        ),
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="notify 必须是映射"):
        load_config(config_path)


def test_unknown_notify_field_rejected() -> None:
    with pytest.raises(Exception):  # noqa: PT011 - pydantic ValidationError
        NotifySettings.model_validate(
            {"webhooks": ["http://127.0.0.1/hook"], "unexpected": 1}
        )
