"""去重模块配置：指纹规则、相似度阈值、YAML 加载与规则版本计算。"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from crawl_flow.errors import ConfigError

SUPPORTED_HASH_ALGORITHMS = ("sha256", "sha1", "md5", "blake2b")


class FieldSelection(BaseModel):
    """参与指纹计算的字段开关。"""

    model_config = ConfigDict(extra="forbid")

    title: bool = True
    content: bool = True


class NormalizationRules(BaseModel):
    """指纹前的文本归一化规则，逐项可开关。"""

    model_config = ConfigDict(extra="forbid")

    nfkc: bool = True
    lowercase: bool = True
    remove_whitespace: bool = True
    remove_punctuation: bool = True


class ExactRules(BaseModel):
    """精确指纹（完全重复识别）规则。"""

    model_config = ConfigDict(extra="forbid")

    algorithm: str = "sha256"

    @field_validator("algorithm")
    @classmethod
    def _check_algorithm(cls, value: str) -> str:
        if value not in SUPPORTED_HASH_ALGORITHMS:
            raise ValueError(
                f"algorithm 仅支持 {', '.join(SUPPORTED_HASH_ALGORITHMS)}"
            )
        return value


class SimilarityRules(BaseModel):
    """相似指纹（SimHash）规则：中文按字符 n-gram，无需预训练模型。"""

    model_config = ConfigDict(extra="forbid")

    enabled: bool = True
    bits: int = 64
    threshold: float = Field(default=0.9, gt=0.0, le=1.0)
    cjk_ngram: int = Field(default=2, ge=1, le=4)
    title_weight: int = Field(default=3, ge=1, le=10)

    @field_validator("bits")
    @classmethod
    def _check_bits(cls, value: int) -> int:
        if value % 8 != 0 or not 16 <= value <= 128:
            raise ValueError("bits 必须是 16–128 之间 8 的倍数")
        return value


class DedupConfig(BaseModel):
    """去重模块完整配置，对应 ``config/dedup.yaml``。"""

    model_config = ConfigDict(extra="forbid")

    rule_version: str | None = Field(default=None, max_length=32)
    min_content_length: int = Field(default=20, ge=0)
    fields: FieldSelection = Field(default_factory=FieldSelection)
    normalization: NormalizationRules = Field(default_factory=NormalizationRules)
    exact: ExactRules = Field(default_factory=ExactRules)
    similarity: SimilarityRules = Field(default_factory=SimilarityRules)

    def effective_version(self) -> str:
        """当前规则集的生效版本。

        版本 = 显式 ``rule_version``（可选）+ 规则内容的哈希。任何规则
        变化都会改变版本，从而触发历史指纹的批量重算；未配置
        ``rule_version`` 时完全由规则哈希决定。
        """

        canonical = json.dumps(
            self.model_dump(exclude={"rule_version"}),
            sort_keys=True,
            ensure_ascii=False,
        )
        rules_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:12]
        if self.rule_version:
            return f"{self.rule_version}+{rules_hash}"
        return f"auto-{rules_hash}"


def load_dedup_config(path: str | Path) -> DedupConfig:
    """读取并校验去重配置，任何问题统一抛 :class:`ConfigError`。"""

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
        payload = {}
    if not isinstance(payload, dict):
        raise ConfigError("去重配置顶层必须是映射")
    try:
        return DedupConfig.model_validate(payload)
    except ValidationError as exc:
        raise ConfigError(f"去重配置校验失败: {exc}") from exc
