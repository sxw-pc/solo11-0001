"""跨 UTC 零点与时间一致性：raw 分目录、留档/DB 时间一致、重复采集幂等。"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from crawl_flow.config import FieldMapping, SourceConfig
from crawl_flow.models import RawRecordRow, RecordRow
from crawl_flow.pipeline import run as run_module
from crawl_flow.pipeline.run import process_item
from crawl_flow.sources.base import ListItem, SourceAdapter
from crawl_flow.fetch.types import FetchResult
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore

UTC = timezone.utc


class _FakeAdapter(SourceAdapter):
    """不发起网络请求的测试适配器，详情字段可在构造时指定。"""

    def __init__(
        self,
        source: SourceConfig,
        detail_fields: dict[str, Any],
        *,
        status_code: int = 200,
    ) -> None:
        super().__init__(source, fetcher=None)  # type: ignore[arg-type]
        self._detail_fields = detail_fields
        self._status_code = status_code

    def list_url(self, page: int) -> str:
        return f"{self.source.base_url}/list?page={page}"

    def detail_url(self, external_id: str) -> str:
        return f"{self.source.base_url}/item/{external_id}"

    def parse_list(self, result: FetchResult) -> list[ListItem]:
        return [ListItem(external_id="x-1", url=self.detail_url("x-1"))]

    def parse_detail(self, result: FetchResult, external_id: str) -> dict[str, Any]:
        return dict(self._detail_fields)

    def fetch_detail(self, external_id: str) -> FetchResult:
        return FetchResult(
            url=self.detail_url(external_id),
            status_code=self._status_code,
            headers={"content-type": "application/json"},
            body=json.dumps(self._detail_fields, ensure_ascii=False),
        )


def _source(**field_map_overrides: Any) -> SourceConfig:
    return SourceConfig(
        name="time_demo",
        type="api",
        base_url="http://mock.test",
        field_map=FieldMapping(**field_map_overrides),
    )


def _freeze_now(monkeypatch: pytest.MonkeyPatch, frozen: datetime) -> None:
    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz: Any = None) -> datetime:  # type: ignore[override]
            if tz is None:
                return frozen.replace(tzinfo=None)
            return frozen.astimezone(tz)

    monkeypatch.setattr(run_module, "datetime", _FrozenDateTime)


def test_raw_directory_uses_utc_date_of_fetched_at_across_midnight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    source = _source()
    adapter = _FakeAdapter(source, {"title": "标题", "published_at": "2024年5月1日"})

    # 北京时间 3 月 2 日 07:30 —— UTC 仍是 3 月 1 日 23:30
    beijing = timezone(timedelta(hours=8))
    frozen = datetime(2024, 3, 2, 7, 30, tzinfo=beijing)
    _freeze_now(monkeypatch, frozen)

    outcome = process_item(
        adapter,
        ListItem(external_id="x-1", url=adapter.detail_url("x-1")),
        db=db,
        raw_store=raw_store,
        dry_run=False,
        logger=__import__("logging").getLogger("test"),
    )
    assert outcome.status == "ok"

    # raw 文件必须落在 UTC 日期 2024-03-01 目录，而不是本地日历日 2024-03-02
    files = list(raw_store.root.rglob("x-1.json"))
    assert len(files) == 1
    assert files[0].parent.name == "2024-03-01"
    assert files[0].parent.parent.name == "time_demo"

    with Session(db.engine) as session:
        raw_row = session.execute(
            select(RawRecordRow).where(RawRecordRow.external_id == "x-1")
        ).scalar_one()
    # DB 中 fetched_at 的 UTC 日期同样是 2024-03-01
    assert raw_row.fetched_at.strftime("%Y-%m-%d") == "2024-03-01"
    db.close()


def test_raw_payload_fetched_at_matches_database_exactly(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    source = _source()
    adapter = _FakeAdapter(source, {"title": "标题"})
    frozen = datetime(2024, 6, 1, 0, 0, tzinfo=UTC)
    _freeze_now(monkeypatch, frozen)

    process_item(
        adapter,
        ListItem(external_id="x-1", url=adapter.detail_url("x-1")),
        db=db,
        raw_store=raw_store,
        dry_run=False,
        logger=__import__("logging").getLogger("test"),
    )

    raw_file = next(raw_store.root.rglob("x-1.json"))
    payload = json.loads(raw_file.read_text(encoding="utf-8"))

    with Session(db.engine) as session:
        raw_row = session.execute(
            select(RawRecordRow).where(RawRecordRow.external_id == "x-1")
        ).scalar_one()
        record_row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "x-1")
        ).scalar_one()

    db_text = raw_row.fetched_at.strftime("%Y-%m-%d %H:%M:%S")
    # 留档字段与 DB 记录是同一时刻、同一规范文本
    assert payload["fetched_at"] == db_text
    assert record_row.fetched_at.strftime("%Y-%m-%d %H:%M:%S") == db_text
    # 两者都是无时区 UTC，且没有任何偏移后缀
    assert raw_row.fetched_at.tzinfo is None
    assert record_row.fetched_at.tzinfo is None
    assert "+" not in payload["fetched_at"]
    db.close()


def test_aware_published_at_is_stored_as_naive_utc(tmp_path: Path) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    source = _source(published_at="published_at")
    adapter = _FakeAdapter(
        source,
        {"title": "标题", "published_at": "2024-03-01T09:30:00+08:00"},
    )

    process_item(
        adapter,
        ListItem(external_id="x-1", url=adapter.detail_url("x-1")),
        db=db,
        raw_store=raw_store,
        dry_run=False,
        logger=__import__("logging").getLogger("test"),
    )

    with Session(db.engine) as session:
        row = session.execute(
            select(RecordRow).where(RecordRow.external_id == "x-1")
        ).scalar_one()
    # 09:30+08:00 == 01:30 UTC，且为 naive
    assert row.published_at == datetime(2024, 3, 1, 1, 30)
    assert row.published_at.tzinfo is None
    db.close()


def test_repeated_crawls_produce_identical_published_at(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    source = _source(published_at="published_at")

    def _run(frozen: datetime) -> datetime:
        _freeze_now(monkeypatch, frozen)
        adapter = _FakeAdapter(
            source, {"title": "标题", "published_at": "3天前"}
        )
        process_item(
            adapter,
            ListItem(external_id="x-1", url=adapter.detail_url("x-1")),
            db=db,
            raw_store=raw_store,
            dry_run=False,
            logger=__import__("logging").getLogger("test"),
        )
        with Session(db.engine) as session:
            row = session.execute(
                select(RecordRow).where(RecordRow.external_id == "x-1")
            ).scalar_one()
        return row.published_at

    # 两次采集发生在不同时刻；相对时间锚定各自 fetched_at，解析规则保持确定
    first = _run(datetime(2024, 5, 10, 12, 0, tzinfo=UTC))
    second = _run(datetime(2024, 5, 20, 8, 0, tzinfo=UTC))
    assert first == datetime(2024, 5, 7, 12, 0)
    assert second == datetime(2024, 5, 17, 8, 0)

    # 同一时刻重跑（重复采集同一份响应），published_at 必须完全一致
    same_moment = datetime(2024, 5, 20, 8, 0, tzinfo=UTC)
    again = _run(same_moment)
    assert again == second

    # 固定格式输入（非相对时间）在任意采集时刻结果都不变
    def _run_fixed(frozen: datetime) -> datetime:
        _freeze_now(monkeypatch, frozen)
        adapter = _FakeAdapter(
            source, {"title": "标题", "published_at": "2024年5月1日 10:00"}
        )
        process_item(
            adapter,
            ListItem(external_id="x-2", url=adapter.detail_url("x-2")),
            db=db,
            raw_store=raw_store,
            dry_run=False,
            logger=__import__("logging").getLogger("test"),
        )
        with Session(db.engine) as session:
            row = session.execute(
                select(RecordRow).where(RecordRow.external_id == "x-2")
            ).scalar_one()
        return row.published_at

    fixed_first = _run_fixed(datetime(2024, 5, 10, 12, 0, tzinfo=UTC))
    fixed_second = _run_fixed(datetime(2024, 9, 1, 0, 0, tzinfo=UTC))
    assert fixed_first == fixed_second == datetime(2024, 5, 1, 10, 0)
    db.close()


def test_failed_item_raw_directory_and_payload_stay_consistent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    db = Database(tmp_path / "crawl.db")
    db.init()
    raw_store = RawStore(tmp_path / "raw")
    source = _source()
    adapter = _FakeAdapter(source, {"title": "标题"}, status_code=500)
    frozen = datetime(2024, 3, 2, 7, 30, tzinfo=timezone(timedelta(hours=8)))
    _freeze_now(monkeypatch, frozen)

    outcome = process_item(
        adapter,
        ListItem(external_id="x-1", url=adapter.detail_url("x-1")),
        db=db,
        raw_store=raw_store,
        dry_run=False,
        logger=__import__("logging").getLogger("test"),
    )
    assert outcome.status == "failed"

    raw_file = next(raw_store.root.rglob("x-1.json"))
    assert raw_file.parent.name == "2024-03-01"
    payload = json.loads(raw_file.read_text(encoding="utf-8"))
    with Session(db.engine) as session:
        row = session.execute(
            select(RawRecordRow).where(RawRecordRow.external_id == "x-1")
        ).scalar_one()
    assert payload["fetched_at"] == row.fetched_at.strftime("%Y-%m-%d %H:%M:%S")
    assert row.status == "failed"
    db.close()
