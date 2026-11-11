"""数据模型：pydantic 领域对象 + SQLAlchemy 表定义。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

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


class WideRecordRow(Base):
    """``wide_records``：跨源合并后的统一宽表，(table_name, entity_key) 唯一。

    宽表字段按配置动态生成，统一存进 ``fields`` JSON，避免为每张宽表
    物理建表；``table_name`` 由配置白名单校验，不可注入。
    """

    __tablename__ = "wide_records"
    __table_args__ = (
        UniqueConstraint("table_name", "entity_key", name="uq_wide_records_table_entity"),
        Index("ix_wide_records_table_status", "table_name", "status"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String(128), nullable=False)
    entity_key: Mapped[str] = mapped_column(String(512), nullable=False)
    # ok：唯一命中；conflict：多命中按冲突策略合并
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    primary_source: Mapped[str] = mapped_column(String(128), nullable=False)
    sources: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    fields: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    conflict_detail: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class CorrelationEventRow(Base):
    """``correlation_events``：关联失败/冲突留痕，不中断主采集流程。

    同一条本地记录的同一事件类型按 (local_source, local_external_id,
    event_type) 幂等覆盖。
    """

    __tablename__ = "correlation_events"
    __table_args__ = (
        UniqueConstraint(
            "local_source",
            "local_external_id",
            "event_type",
            name="uq_correlation_events_record_type",
        ),
        Index("ix_correlation_events_type", "event_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    table_name: Mapped[str] = mapped_column(String(128), nullable=False)
    local_source: Mapped[str] = mapped_column(String(128), nullable=False)
    local_external_id: Mapped[str] = mapped_column(String(256), nullable=False)
    remote_source: Mapped[str] = mapped_column(String(128), nullable=False)
    # missing_field / type_mismatch / not_matched / conflict / error
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    detail: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
