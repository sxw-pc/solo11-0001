"""去重模块自己的 SQLAlchemy 表定义，建在独立指纹库文件中。

与采集主库（``records`` / ``raw_records``）物理隔离：本模块的所有表只
创建在 ``fingerprint_db`` 指向的 SQLite 文件里，主库不新增、不修改任何表，
去重结果也只写入自己的标记表，绝不删除或改写原始采集数据。
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class DedupBase(DeclarativeBase):
    """去重模块独立的声明式基类，与 ``crawl_flow.models.Base`` 互不影响。"""


class FingerprintRow(DedupBase):
    """``content_fingerprints``：每条采集记录对应的内容指纹。"""

    __tablename__ = "content_fingerprints"
    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_fingerprints_source_external"),
        Index("ix_fingerprints_content_hash", "content_hash"),
        Index("ix_fingerprints_rule_version", "rule_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    external_id: Mapped[str] = mapped_column(String(256), nullable=False)
    # 冗余标题（不存正文全文），让标记表在主库离线时仍可直接展示。
    title: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    title_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    content_hash: Mapped[str | None] = mapped_column(String(128), nullable=True)
    # SimHash 以 16 位十六进制存储，规避 SQLite 有符号 64 位整数上限问题。
    simhash_hex: Mapped[str | None] = mapped_column(String(16), nullable=True)
    rule_version: Mapped[int] = mapped_column(Integer, nullable=False)
    rule_signature: Mapped[str] = mapped_column(String(512), nullable=False)
    content_length: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    skipped: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    skip_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class DedupGroupRow(DedupBase):
    """``dedup_groups``：重复/相似簇的汇总行，只做标记，不删除任何原始数据。"""

    __tablename__ = "dedup_groups"
    __table_args__ = (
        Index("ix_dedup_groups_type_version", "group_type", "rule_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    # exact：内容哈希完全一致；similar：SimHash 达到阈值的近似重复。
    group_type: Mapped[str] = mapped_column(String(16), nullable=False)
    rule_version: Mapped[int] = mapped_column(Integer, nullable=False)
    # exact 用内容哈希；similar 用成员键稳定派生的标识。
    group_key: Mapped[str] = mapped_column(String(128), nullable=False)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    source_count: Mapped[int] = mapped_column(Integer, nullable=False)
    # 关联的全部来源，JSON 数组持久化，跨源重复可直接读出所有来源。
    sources: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    max_hamming: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_similarity: Mapped[float | None] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class DedupGroupMemberRow(DedupBase):
    """``dedup_group_members``：簇与采集条目（source + external_id）的关联。"""

    __tablename__ = "dedup_group_members"
    __table_args__ = (
        UniqueConstraint(
            "group_id", "source", "external_id", name="uq_dedup_members_group_record"
        ),
        Index("ix_dedup_members_record", "source", "external_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(
        ForeignKey("dedup_groups.id", ondelete="CASCADE"), nullable=False
    )
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    external_id: Mapped[str] = mapped_column(String(256), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
