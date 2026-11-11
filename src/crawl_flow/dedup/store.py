"""去重模块的独立持久化：指纹表与去重标记表。

模块自带三张表（``content_fingerprints`` / ``dedup_groups`` /
``dedup_group_members``），使用独立的声明式基类，与采集管道的
``records`` / ``raw_records`` 完全隔离：本模块对原表只读，去重结论
只写标记表，绝不修改或删除原始数据。
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import (
    Boolean,
    DateTime,
    Engine,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    create_engine,
    delete,
    func,
    select,
)
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker

from crawl_flow.errors import StoreError
from crawl_flow.models import RecordRow
from crawl_flow.dedup.similarity import DuplicateGroup


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DedupBase(DeclarativeBase):
    """去重模块专用的声明式基类（与采集管道的 Base 隔离）。"""


class FingerprintRow(DedupBase):
    """``content_fingerprints``：每条记录的指纹，(source, external_id) 唯一。"""

    __tablename__ = "content_fingerprints"
    __table_args__ = (
        UniqueConstraint("source", "external_id", name="uq_fingerprint_source_external"),
        Index("ix_fingerprint_exact_hash", "exact_hash"),
        Index("ix_fingerprint_rule_version", "rule_version"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    external_id: Mapped[str] = mapped_column(String(256), nullable=False)
    rule_version: Mapped[str] = mapped_column(String(64), nullable=False)
    exact_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    title_hash: Mapped[str] = mapped_column(String(128), nullable=False)
    simhash: Mapped[str | None] = mapped_column(String(32), nullable=True)
    content_length: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class DedupGroupRow(DedupBase):
    """``dedup_groups``：一个重复/相似内容组。"""

    __tablename__ = "dedup_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_key: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    match_type: Mapped[str] = mapped_column(String(16), nullable=False)
    member_count: Mapped[int] = mapped_column(Integer, nullable=False)
    rule_version: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)


class DedupGroupMemberRow(DedupBase):
    """``dedup_group_members``：组内成员标记，记录所有关联来源与条目标识。"""

    __tablename__ = "dedup_group_members"
    __table_args__ = (
        UniqueConstraint("group_id", "source", "external_id", name="uq_dedup_member"),
        Index("ix_dedup_member_record", "source", "external_id"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    group_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("dedup_groups.id"), nullable=False
    )
    source: Mapped[str] = mapped_column(String(128), nullable=False)
    external_id: Mapped[str] = mapped_column(String(256), nullable=False)
    similarity: Mapped[float] = mapped_column(Float, nullable=False)
    is_representative: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class DedupStore:
    """去重模块的 SQLite 封装；只读原表，只写模块自有表。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine: Engine = _create_engine(self.path)
        self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

    def init(self) -> None:
        """创建去重模块自有表（幂等）；不触碰采集管道的表。"""

        try:
            DedupBase.metadata.create_all(self.engine)
        except SQLAlchemyError as exc:
            raise StoreError(f"去重表建表失败 db={self.path} error={exc}") from exc

    # ---- 读取采集管道的 records（只读） ----

    def iter_records(self, source: str | None = None) -> list[RecordRow]:
        """读取待计算指纹的结构化记录；对原表只读。"""

        try:
            with Session(self.engine) as session:
                statement = select(RecordRow).order_by(RecordRow.id)
                if source is not None:
                    statement = statement.where(RecordRow.source == source)
                return list(session.execute(statement).scalars().all())
        except SQLAlchemyError as exc:
            raise StoreError(
                f"records 读取失败（请先运行采集入库）db={self.path} error={exc}"
            ) from exc

    # ---- 指纹表 ----

    def fingerprint_map(self) -> dict[tuple[str, str], FingerprintRow]:
        with Session(self.engine) as session:
            rows = session.execute(select(FingerprintRow)).scalars().all()
            return {(row.source, row.external_id): row for row in rows}

    def list_fingerprints(self, rule_version: str | None = None) -> list[FingerprintRow]:
        with Session(self.engine) as session:
            statement = select(FingerprintRow).order_by(FingerprintRow.id)
            if rule_version is not None:
                statement = statement.where(FingerprintRow.rule_version == rule_version)
            return list(session.execute(statement).scalars().all())

    def upsert_fingerprints(self, rows: Iterable[dict[str, Any]]) -> int:
        """批量幂等写入指纹（单事务）；重复键只更新指纹字段。"""

        values = list(rows)
        if not values:
            return 0
        update_fields = (
            "rule_version",
            "exact_hash",
            "content_hash",
            "title_hash",
            "simhash",
            "content_length",
            "updated_at",
        )
        try:
            with self._session_factory() as session:
                statement = sqlite_insert(FingerprintRow).values(values)
                statement = statement.on_conflict_do_update(
                    index_elements=["source", "external_id"],
                    set_={field: statement.excluded[field] for field in update_fields},
                )
                session.execute(statement)
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"指纹写入失败 db={self.path} error={exc}") from exc
        return len(values)

    def delete_fingerprints(self, keys: Iterable[tuple[str, str]]) -> int:
        """删除指定 (source, external_id) 的指纹（用于清理失效/孤儿指纹）。"""

        key_list = list(keys)
        if not key_list:
            return 0
        try:
            with self._session_factory() as session:
                for source, external_id in key_list:
                    session.execute(
                        delete(FingerprintRow).where(
                            FingerprintRow.source == source,
                            FingerprintRow.external_id == external_id,
                        )
                    )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"指纹清理失败 db={self.path} error={exc}") from exc
        return len(key_list)

    # ---- 去重标记 ----

    def replace_groups(self, groups: list[DuplicateGroup], rule_version: str) -> None:
        """事务性重建全部去重标记；只动标记表，原始记录不受影响。"""

        import hashlib

        now = _utcnow()
        try:
            with self._session_factory() as session:
                session.execute(delete(DedupGroupMemberRow))
                session.execute(delete(DedupGroupRow))
                for group in groups:
                    representative = next(m for m in group.members if m.is_representative)
                    group_key = hashlib.sha256(
                        f"{rule_version}|{representative.source}|"
                        f"{representative.external_id}".encode("utf-8")
                    ).hexdigest()[:32]
                    row = DedupGroupRow(
                        group_key=group_key,
                        match_type=group.match_type,
                        member_count=len(group.members),
                        rule_version=rule_version,
                        created_at=now,
                    )
                    session.add(row)
                    session.flush()
                    for member in group.members:
                        session.add(
                            DedupGroupMemberRow(
                                group_id=row.id,
                                source=member.source,
                                external_id=member.external_id,
                                similarity=member.similarity,
                                is_representative=member.is_representative,
                            )
                        )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"去重标记写入失败 db={self.path} error={exc}") from exc

    def list_groups(self, limit: int | None = None) -> list[DedupGroupRow]:
        with Session(self.engine) as session:
            statement = select(DedupGroupRow).order_by(DedupGroupRow.id)
            if limit is not None:
                statement = statement.limit(limit)
            return list(session.execute(statement).scalars().all())

    def list_members(self, group_id: int) -> list[DedupGroupMemberRow]:
        with Session(self.engine) as session:
            statement = (
                select(DedupGroupMemberRow)
                .where(DedupGroupMemberRow.group_id == group_id)
                .order_by(DedupGroupMemberRow.is_representative.desc(), DedupGroupMemberRow.id)
            )
            return list(session.execute(statement).scalars().all())

    def stats(self) -> dict[str, Any]:
        """指纹与去重标记的汇总统计。"""

        with Session(self.engine) as session:
            fingerprints = int(
                session.execute(select(func.count()).select_from(FingerprintRow)).scalar_one()
            )
            versions = dict(
                session.execute(
                    select(FingerprintRow.rule_version, func.count()).group_by(
                        FingerprintRow.rule_version
                    )
                ).all()
            )
            groups = int(
                session.execute(select(func.count()).select_from(DedupGroupRow)).scalar_one()
            )
            marked = int(
                session.execute(
                    select(func.count()).select_from(DedupGroupMemberRow)
                ).scalar_one()
            )
            by_type = dict(
                session.execute(
                    select(DedupGroupRow.match_type, func.count()).group_by(
                        DedupGroupRow.match_type
                    )
                ).all()
            )
        return {
            "fingerprints": fingerprints,
            "fingerprint_versions": versions,
            "groups": groups,
            "marked_records": marked,
            "groups_by_type": by_type,
        }

    def close(self) -> None:
        self.engine.dispose()


def _create_engine(path: Path) -> Engine:
    if str(path) == ":memory:":
        return create_engine("sqlite://", future=True)
    return create_engine(f"sqlite:///{path}", future=True)
