"""只读访问采集主库的 ``records`` 表。

去重模块通过独立 SQLAlchemy 引擎以只读方式读取结构化正文与标题，
不导入、不修改主库的 :class:`~crawl_flow.storage.db.Database` 及其会话，
也不会对主库执行任何写操作，保证与采集入库逻辑完全解耦。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import MetaData, Table, create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from crawl_flow.errors import StoreError


@dataclass(frozen=True)
class RecordData:
    """采集记录的只读视图，只取去重需要的四个字段。"""

    source: str
    external_id: str
    title: str
    content: str


class RecordReader:
    """以反射方式读取主库 ``records`` 表，引擎独立、默认只读连接。"""

    def __init__(self, db_path: str | Path | Engine) -> None:
        if isinstance(db_path, Engine):
            # 测试场景：复用内存库引擎。
            self.engine: Engine = db_path
            self._owns_engine = False
        else:
            path = Path(db_path)
            if not path.is_file():
                raise StoreError(f"采集主库不存在: {path}")
            # mode=ro：若代码有误尝试写主库，SQLite 会直接拒绝而非静默改数据。
            self.engine = create_engine(
                f"sqlite:///file:{path}?mode=ro&uri=true", future=True
            )
            self._owns_engine = True
        self._records_table: Table | None = None

    def _table(self) -> Table:
        if self._records_table is None:
            try:
                metadata = MetaData()
                metadata.reflect(bind=self.engine, only=("records",))
            except SQLAlchemyError as exc:
                raise StoreError(f"读取主库表结构失败 error={exc}") from exc
            if "records" not in metadata.tables:
                raise StoreError("主库中不存在 records 表，请先运行采集")
            self._records_table = metadata.tables["records"]
        return self._records_table

    def iter_records(self, *, source: str | None = None) -> list[RecordData]:
        """读取全部（或指定源）结构化记录，按源与条目标识排序。"""

        table = self._table()
        statement = select(
            table.c.source, table.c.external_id, table.c.title, table.c.content
        )
        if source is not None:
            statement = statement.where(table.c.source == source)
        statement = statement.order_by(table.c.source, table.c.external_id)
        try:
            with self.engine.connect() as connection:
                rows = connection.execute(statement).all()
        except SQLAlchemyError as exc:
            raise StoreError(f"读取主库 records 失败 error={exc}") from exc
        return [
            RecordData(
                source=row[0],
                external_id=row[1],
                title=row[2] or "",
                content=row[3] or "",
            )
            for row in rows
        ]

    def close(self) -> None:
        if self._owns_engine:
            self.engine.dispose()
