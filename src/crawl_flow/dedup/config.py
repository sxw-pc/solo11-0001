"""去重模块配置：规则、相似度阈值、存储位置均可独立配置。

配置文件为独立 YAML（默认 ``config/dedup.yaml``），与源采集配置
``sources.yaml`` 完全分离，不共享 schema，避免侵入原采集配置。
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from crawl_flow.dedup.fingerprint import SUPPORTED_HASH_ALGORITHMS
from crawl_flow.errors import ConfigError


class FingerprintRuleConfig(BaseModel):
    """指纹生成规则配置，对应 :class:`~crawl_flow.dedup.fingerprint.FingerprintRule`。"""

    model_config = ConfigDict(extra="forbid")

    version: int = Field(
        default=1, ge=1, description="规则版本号，调整规则后递增以触发重算"
    )
    hash_algorithm: str = Field(default="sha256", description="精确指纹哈希算法")
    lowercase: bool = True
    strip_punctuation: bool = True
    cjk_bigram: bool = True
    min_content_length: int = Field(
        default=20, ge=0, description="正文短于该字符数则跳过指纹"
    )
    min_title_length: int = Field(
        default=2, ge=0, description="标题短于该字符数则跳过"
    )

    @field_validator("hash_algorithm")
    @classmethod
    def _check_algorithm(cls, value: str) -> str:
        if value not in SUPPORTED_HASH_ALGORITHMS:
            raise ValueError(
                f"不支持的哈希算法 {value!r}，可选: {', '.join(SUPPORTED_HASH_ALGORITHMS)}"
            )
        return value


class SimilarityConfig(BaseModel):
    """相似度检测配置。"""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = Field(default=True, description="是否计算 SimHash 并识别近似重复")
    threshold: float = Field(default=0.9, ge=0.0, le=1.0, description="相似度阈值，[0,1]")
    bands: int = Field(default=4, ge=1, le=8, description="LSH 分带数，需能整除 64")

    @field_validator("bands")
    @classmethod
    def _check_bands(cls, value: int) -> int:
        if 64 % value != 0:
            raise ValueError("bands 必须能整除 SimHash 位数 64（可选 1/2/4/8）")
        return value


class DedupConfig(BaseModel):
    """去重模块顶层配置。"""

    model_config = ConfigDict(extra="forbid")

    fingerprint_db: str = Field(
        default="data/fingerprints.db",
        description="指纹独立 SQLite 文件；与采集主库物理隔离",
    )
    rule: FingerprintRuleConfig = Field(default_factory=FingerprintRuleConfig)
    similarity: SimilarityConfig = Field(default_factory=SimilarityConfig)


def load_dedup_config(path: str | Path | None) -> DedupConfig:
    """读取去重配置 YAML；``path`` 为空或文件不存在时返回全默认配置。

    与采集配置的严格模式不同：去重配置是可选的，缺省即默认值，便于
    在不改任何配置的情况下直接使用 CLI。
    """

    if path is None:
        return DedupConfig()
    config_path = Path(path)
    if not config_path.is_file():
        raise ConfigError(f"去重配置文件不存在: {config_path}")
    try:
        payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:
        raise ConfigError(f"去重配置 YAML 解析失败: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"去重配置文件读取失败: {exc}") from exc

    if payload is None:
        return DedupConfig()
    if not isinstance(payload, dict):
        raise ConfigError("去重配置顶层必须是映射结构")
    try:
        return DedupConfig.model_validate(payload)
    except ValidationError as exc:
        raise ConfigError(f"去重配置校验失败: {exc}") from exc
