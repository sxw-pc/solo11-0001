"""指纹与去重标记的持久化：独立 SQLite，重启后无需重算。

所有写入仅发生在去重模块自己的库文件中；对采集主库只读不写。
指纹按 ``(source, external_id)`` 幂等 upsert，去重簇采用「整组快照
事务重建」：每次识别先在事务内清空旧标记再写新标记，保证标记始终与
当前指纹集合一致，且全程不触碰采集数据。
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from sqlalchemy import Engine, create_engine, delete, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from crawl_flow.dedup.models import (
    DedupBase,
    DedupGroupMemberRow,
    DedupGroupRow,
    FingerprintRow,
)
from crawl_flow.errors import StoreError

FINGERPRINT_UPDATE_FIELDS = (
    "title",
    "title_hash",
    "content_hash",
    "simhash_hex",
    "rule_version",
    "rule_signature",
    "content_length",
    "skipped",
    "skip_reason",
    "updated_at",
)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class FingerprintStore:
    """指纹库读写封装，SQLAlchemy 异常统一转 :class:`StoreError`。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        if str(self.path) != ":memory:":
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self.engine: Engine = self._create_engine(self.path)
        self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)

    @staticmethod
    def _create_engine(path: Path) -> Engine:
        if str(path) == ":memory:":
            return create_engine("sqlite://", future=True)
        return create_engine(f"sqlite:///{path}", future=True)

    def init(self) -> None:
        """创建指纹库全部表与索引（幂等）。"""

        try:
            DedupBase.metadata.create_all(self.engine)
        except SQLAlchemyError as exc:
            raise StoreError(f"指纹库建表失败 db={self.path} error={exc}") from exc

    def close(self) -> None:
        self.engine.dispose()

    # ------------------------------------------------------------------
    # 指纹写入 / 查询
    # ------------------------------------------------------------------

    def upsert_fingerprint(self, values: dict[str, Any]) -> None:
        """幂等写入单条指纹；同键重复写入只更新指纹与时间戳。"""

        now = utcnow()
        payload = {"created_at": now, **values, "updated_at": now}
        try:
            with self._session_factory() as session:
                statement = sqlite_insert(FingerprintRow).values(**payload)
                statement = statement.on_conflict_do_update(
                    index_elements=["source", "external_id"],
                    set_={field: statement.excluded[field] for field in FINGERPRINT_UPDATE_FIELDS},
                )
                session.execute(statement)
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(
                f"指纹写入失败 source={values.get('source')} "
                f"external_id={values.get('external_id')} error={exc}"
            ) from exc

    def get_fingerprint(self, source: str, external_id: str) -> FingerprintRow | None:
        with self._session_factory() as session:
            return session.execute(
                select(FingerprintRow).where(
                    FingerprintRow.source == source,
                    FingerprintRow.external_id == external_id,
                )
            ).scalar_one_or_none()

    def iter_fingerprints(
        self, *, rule_version: int | None = None, include_skipped: bool = False
    ) -> list[FingerprintRow]:
        """读取指纹行；默认只返回成功生成正文指纹的条目。"""

        statement = select(FingerprintRow)
        if rule_version is not None:
            statement = statement.where(FingerprintRow.rule_version == rule_version)
        if not include_skipped:
            statement = statement.where(FingerprintRow.skipped.is_(False))
        statement = statement.order_by(FingerprintRow.source, FingerprintRow.external_id)
        with self._session_factory() as session:
            return list(session.execute(statement).scalars().all())

    def count_fingerprints(self, *, rule_version: int | None = None) -> int:
        statement = select(func.count()).select_from(FingerprintRow)
        if rule_version is not None:
            statement = statement.where(FingerprintRow.rule_version == rule_version)
        with self._session_factory() as session:
            return int(session.execute(statement).scalar_one())

    def count_skipped(self, *, rule_version: int | None = None) -> int:
        statement = (
            select(func.count())
            .select_from(FingerprintRow)
            .where(FingerprintRow.skipped.is_(True))
        )
        if rule_version is not None:
            statement = statement.where(FingerprintRow.rule_version == rule_version)
        with self._session_factory() as session:
            return int(session.execute(statement).scalar_one())

    def delete_fingerprints_not_in(
        self, keys: Iterable[tuple[str, str]]
    ) -> int:
        """删除不属于当前记录键集合的指纹（全量重算的清理步骤）。"""

        key_list = list(keys)
        with self._session_factory() as session:
            existing = session.execute(
                select(FingerprintRow.source, FingerprintRow.external_id)
            ).all()
            stale = [row for row in existing if (row[0], row[1]) not in set(key_list)]
            for source, external_id in stale:
                session.execute(
                    delete(FingerprintRow).where(
                        FingerprintRow.source == source,
                        FingerprintRow.external_id == external_id,
                    )
                )
            session.commit()
            return len(stale)

    # ------------------------------------------------------------------
    # 去重标记（簇）写入 / 查询
    # ------------------------------------------------------------------

    def replace_groups(self, groups: list[dict[str, Any]]) -> int:
        """在单个事务内用新的簇快照替换全部去重标记，返回写入簇数。

        只重建指纹库自己的标记表，采集主库的 ``records`` 等表完全不受影响。
        """

        now = utcnow()
        try:
            with self._session_factory() as session:
                session.execute(delete(DedupGroupMemberRow))
                session.execute(delete(DedupGroupRow))
                for group in groups:
                    group_row = DedupGroupRow(
                        group_type=group["group_type"],
                        rule_version=group["rule_version"],
                        group_key=group["group_key"],
                        size=group["size"],
                        source_count=group["source_count"],
                        sources=list(group["sources"]),
                        max_hamming=group.get("max_hamming"),
                        min_similarity=group.get("min_similarity"),
                        created_at=now,
                    )
                    session.add(group_row)
                    session.flush()  # 取得自增主键
                    for member in group["members"]:
                        session.add(
                            DedupGroupMemberRow(
                                group_id=group_row.id,
                                source=member["source"],
                                external_id=member["external_id"],
                                title=member.get("title", ""),
                                created_at=now,
                            )
                        )
                session.commit()
        except SQLAlchemyError as exc:
            raise StoreError(f"去重标记写入失败 error={exc}") from exc
        return len(groups)

    def list_groups(
        self, *, group_type: str | None = None, rule_version: int | None = None
    ) -> list[DedupGroupRow]:
        statement = select(DedupGroupRow).order_by(
            DedupGroupRow.group_type, DedupGroupRow.id
        )
        if group_type is not None:
            statement = statement.where(DedupGroupRow.group_type == group_type)
        if rule_version is not None:
            statement = statement.where(DedupGroupRow.rule_version == rule_version)
        with self._session_factory() as session:
            return list(session.execute(statement).scalars().all())

    def list_members(self, group_id: int) -> list[DedupGroupMemberRow]:
        with self._session_factory() as session:
            return list(
                session.execute(
                    select(DedupGroupMemberRow)
                    .where(DedupGroupMemberRow.group_id == group_id)
                    .order_by(DedupGroupMemberRow.source, DedupGroupMemberRow.external_id)
                ).scalars().all()
            )

    def groups_for_record(self, source: str, external_id: str) -> list[DedupGroupRow]:
        """查询某条采集记录命中的所有重复/相似簇（含全部关联来源）。"""

        with self._session_factory() as session:
            return list(
                session.execute(
                    select(DedupGroupRow)
                    .join(DedupGroupMemberRow, DedupGroupMemberRow.group_id == DedupGroupRow.id)
                    .where(
                        DedupGroupMemberRow.source == source,
                        DedupGroupMemberRow.external_id == external_id,
                    )
                    .order_by(DedupGroupRow.group_type, DedupGroupRow.id)
                ).scalars().all()
            )

    def count_groups(
        self, *, group_type: str | None = None, rule_version: int | None = None
    ) -> int:
        statement = select(func.count()).select_from(DedupGroupRow)
        if group_type is not None:
            statement = statement.where(DedupGroupRow.group_type == group_type)
        if rule_version is not None:
            statement = statement.where(DedupGroupRow.rule_version == rule_version)
        with self._session_factory() as session:
            return int(session.execute(statement).scalar_one())
