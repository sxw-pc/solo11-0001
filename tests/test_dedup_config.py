"""去重配置加载与校验测试。"""

from __future__ import annotations

import pytest
import yaml

from crawl_flow.dedup.config import (
    DedupConfig,
    FingerprintRuleConfig,
    SimilarityConfig,
    load_dedup_config,
)
from crawl_flow.errors import ConfigError


def test_default_config_when_path_missing(tmp_path) -> None:
    config = load_dedup_config(None)
    assert isinstance(config, DedupConfig)
    assert config.rule.version == 1
    assert config.rule.hash_algorithm == "sha256"
    assert config.similarity.threshold == 0.9
    assert config.fingerprint_db == "data/fingerprints.db"


def test_load_full_config_from_yaml(tmp_path) -> None:
    payload = {
        "fingerprint_db": str(tmp_path / "fp.db"),
        "rule": {"version": 3, "hash_algorithm": "md5", "min_content_length": 30},
        "similarity": {"enabled": True, "threshold": 0.85, "bands": 8},
    }
    path = tmp_path / "dedup.yaml"
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    config = load_dedup_config(path)
    assert config.rule.version == 3
    assert config.rule.hash_algorithm == "md5"
    assert config.rule.min_content_length == 30
    assert config.similarity.threshold == 0.85
    assert config.similarity.bands == 8


def test_missing_file_raises(tmp_path) -> None:
    with pytest.raises(ConfigError):
        load_dedup_config(tmp_path / "not-exist.yaml")


def test_empty_file_uses_defaults(tmp_path) -> None:
    path = tmp_path / "empty.yaml"
    path.write_text("", encoding="utf-8")
    assert load_dedup_config(path).rule.version == 1


def test_unknown_field_rejected(tmp_path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text(yaml.safe_dump({"unknown": 1}), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_dedup_config(path)


def test_invalid_threshold_rejected() -> None:
    with pytest.raises(Exception):
        SimilarityConfig(threshold=1.5)


def test_bands_must_divide_64() -> None:
    with pytest.raises(Exception):
        SimilarityConfig(bands=3)
    # 合法值不报错
    for bands in (1, 2, 4, 8):
        assert SimilarityConfig(bands=bands).bands == bands


def test_invalid_algorithm_rejected() -> None:
    with pytest.raises(Exception):
        FingerprintRuleConfig(hash_algorithm="sha512-not-allowed")


def test_invalid_yaml_raises(tmp_path) -> None:
    path = tmp_path / "broken.yaml"
    path.write_text("rule: [", encoding="utf-8")
    with pytest.raises(ConfigError):
        load_dedup_config(path)
