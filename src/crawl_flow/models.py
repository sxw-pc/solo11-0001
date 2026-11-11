"""数据模型：pydantic 领域对象 + SQLAlchemy 表定义。"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import (
    DateTime,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

RawStatus = Literal["ok", "failed"]


class Record(BaseModel):
    """归一化后的标准记录，对应 ``records`` 表。"""

    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    external_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    category: str | None = None
    tags: list[str] = Field(default_factory=list)
    published_at: datetime | None = None
    content: str = ""
    url: str = ""
    fetched_at: datetime | None = None


class RawRecord(BaseModel):
    """原始响应的元信息，对应 ``raw_records`` 表。"""

    model_config = ConfigDict(extra="forbid")

    source: str = Field(min_length=1)
    external_id: str = Field(min_length=1)
    url: str
    http_status: int | None = None
    fetched_at: datetime
    status: RawStatus
    error: str | None = None
    raw_path: str | None = None

    @field_validator("error")
    @classmethod
    def _trim_error(cls, value: str | None) -> str | None:
        if value is None:
            return None
        trimmed = value.strip()
        return trimmed or None


class Base(DeclarativeBase):
    """SQLAlchemy 声明式基类。"""


class RawRecordRow(Base):
    """``raw_records``：每次采集的原始响应元信息，(source, external_id) 唯一。"""

    __tablename__ = "raw_records"
    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_raw_records_source_external"),
        Index("ix_raw_records_status", "status"),
        Index("ix_raw_records_fetched_at", "fetched_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    external_id: Mapped[str] = mapped_column(String(256), nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)


class RecordRow(Base):
    """``records``：归一化后的结构化记录，(source, external_id) 唯一。"""

    __tablename__ = "records"
    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_records_source_external"),
        Index("ix_records_source_category", "source", "category"),
        Index("ix_records_fetched_at", "fetched_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    external_id: Mapped[str] = mapped_column(String(256), nullable=False)
    title: Mapped[str] = mapped_column(String(512), nullable=False)
    category: Mapped[str | None] = mapped_column(String(128), nullable=True)
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    published_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    url: Mapped[str] = mapped_column(String(2048), nullable=False, default="")
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class ArchiveSettingsRow(Base):
    """``archive_settings``：归档开关与目录（单例行，存在即表示启用跨库查询）。"""

    __tablename__ = "archive_settings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, default=1)
    archive_dir: Mapped[str] = mapped_column(String(1024), nullable=False)
    cold_raw_dir: Mapped[str] = mapped_column(String(1024), nullable=False)
    retention_days: Mapped[int] = mapped_column(Integer, nullable=False, default=90)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class ArchiveCheckpointRow(Base):
    """``archive_checkpoints``：每个 (scope, 归档目录) 的断点续传游标。"""

    __tablename__ = "archive_checkpoints"
    __table_args__ = (
        UniqueConstraint("scope", "archive_dir", name="uq_archive_checkpoints_scope_dir"),
        Index("ix_archive_checkpoints_status", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    scope: Mapped[str] = mapped_column(String(128), nullable=False)
    archive_dir: Mapped[str] = mapped_column(String(1024), nullable=False)
    cutoff: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    cursor_records: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cursor_raw: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    archived_records: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    archived_raw: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
