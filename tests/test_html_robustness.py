"""多编码、畸形 HTML、缺字段与完全不可解析页面的端到端容错。

通过 mock 站点的 ``/lab/*`` 实验区跑完整管道，验证：

- 多编码页面入库后是统一 Unicode；
- 非致命 HTML 语法错误（未闭合标签）只要必填字段能提取就正常处理；
- 选择器缺可选字段记默认值，缺必填字段才记 failed；
- 完全不可解析的响应记 failed，错误写入现有 error 字段，不中断整轮；
- raw 留档是原始响应字节（base64），不转码。
"""

from __future__ import annotations

import base64
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from crawl_flow.config import SourceConfig
from crawl_flow.errors import ParseError
from crawl_flow.fetch.client import Fetcher
from crawl_flow.models import RawRecordRow, RecordRow
from crawl_flow.pipeline.run import run_source
from crawl_flow.sources.demo_html import DemoHtmlAdapter
from crawl_flow.storage.db import Database
from crawl_flow.storage.raw_store import RawStore
from mock_site.app import (
    LAB_CASES,
    LAB_CATEGORY,
    LAB_CONTENT_P1,
    LAB_CONTENT_P2,
    LAB_TAGS,
    LAB_TITLE,
)

OK_CASES = [
    "gbk",
    "gb2312",
    "utf8-bom",
    "wrong-header",
    "bad-charset",
    "meta-only",
    "header-wins",
    "malformed",
    "minimal",
]
FAILED_CASES = ["garbage", "no-title", "no-content"]


@pytest.fixture
def lab_source(mock_base_url: str) -> SourceConfig:
    return SourceConfig(
        name="lab_html",
        type="html",
        base_url=mock_base_url,
        list_path="/lab/list",
        detail_path="/lab/item/{id}",
    )


@pytest.fixture
def lab_adapter(lab_source: SourceConfig, fetcher: Fetcher) -> DemoHtmlAdapter:
    return DemoHtmlAdapter(lab_source, fetcher)


# ---- 适配器层：畸形 HTML 与选择器缺字段 ----


def test_malformed_html_extracts_required_fields(
    lab_adapter: DemoHtmlAdapter,
) -> None:
    result = lab_adapter.fetch_detail("malformed")
    fields = lab_adapter.parse_detail(result, "malformed")

    assert fields["title"] == LAB_TITLE
    assert fields["category"] == LAB_CATEGORY
    assert fields["published_at"] == "2024-03-06"
    assert LAB_CONTENT_P1 in fields["content"]
    assert LAB_CONTENT_P2 in fields["content"]


def test_minimal_page_missing_optional_selectors(
    lab_adapter: DemoHtmlAdapter,
) -> None:
    result = lab_adapter.fetch_detail("minimal")
    fields = lab_adapter.parse_detail(result, "minimal")

    assert fields["title"] == LAB_TITLE
    assert fields["category"] is None
    assert fields["tags"] == []
    assert fields["published_at"] is None


def test_missing_required_title_raises_parse_error(
    lab_adapter: DemoHtmlAdapter,
) -> None:
    result = lab_adapter.fetch_detail("no-title")
    with pytest.raises(ParseError) as excinfo:
        lab_adapter.parse_detail(result, "no-title")
    assert "h1.title" in excinfo.value.reason


def test_missing_required_content_raises_parse_error(
    lab_adapter: DemoHtmlAdapter,
) -> None:
    result = lab_adapter.fetch_detail("no-content")
    with pytest.raises(ParseError) as excinfo:
        lab_adapter.parse_detail(result, "no-content")
    assert "div.content" in excinfo.value.reason


def test_garbage_page_raises_parse_error(
    lab_adapter: DemoHtmlAdapter,
) -> None:
    result = lab_adapter.fetch_detail("garbage")
    with pytest.raises(ParseError):
        lab_adapter.parse_detail(result, "garbage")


# ---- 管道层：多编码统一 Unicode、失败隔离、原始字节留档 ----


def test_pipeline_counts_ok_and_failed(
    lab_source: SourceConfig, db: Database, raw_store: RawStore
) -> None:
    summary = run_source(lab_source, db, raw_store)

    assert (summary.ok, summary.failed, summary.skipped) == (
        len(OK_CASES),
        len(FAILED_CASES),
        0,
    )
    assert db.count_records("lab_html") == len(OK_CASES)
    assert db.count_raw("lab_html", "ok") == len(OK_CASES)
    assert db.count_raw("lab_html", "failed") == len(FAILED_CASES)


def test_records_are_uniform_unicode_across_encodings(
    lab_source: SourceConfig, db: Database, raw_store: RawStore
) -> None:
    run_source(lab_source, db, raw_store)

    with Session(db.engine) as session:
        rows = {
            row.external_id: row
            for row in session.execute(select(RecordRow)).scalars()
        }

    assert set(rows) == set(OK_CASES)
    for external_id, row in rows.items():
        # 不同编码的同一页面，入库后是同一份 Unicode 文本
        assert row.title == LAB_TITLE, external_id
        assert not row.title.startswith("\ufeff"), external_id
        assert "\ufffd" not in row.title, external_id
        if external_id == "minimal":
            assert row.category is None
            assert row.tags == []
            assert row.published_at is None
        else:
            assert row.category == LAB_CATEGORY
            assert row.tags == LAB_TAGS.split("、")
            assert row.published_at == datetime(2024, 3, 6)
        if external_id != "minimal":
            assert LAB_CONTENT_P1 in row.content
            assert LAB_CONTENT_P2 in row.content
    # GBK 扩展字符「〇」在 gb2312 声明下也正确还原
    assert "二〇二四年" in rows["gb2312"].content


def test_failed_items_record_error_field_and_keep_running(
    lab_source: SourceConfig, db: Database, raw_store: RawStore
) -> None:
    summary = run_source(lab_source, db, raw_store)

    assert summary.ok == len(OK_CASES)  # 失败条目没有中断整轮
    with Session(db.engine) as session:
        rows = {
            row.external_id: row
            for row in session.execute(
                select(RawRecordRow).where(RawRecordRow.status == "failed")
            ).scalars()
        }

    assert set(rows) == set(FAILED_CASES)
    assert "h1.title" in rows["garbage"].error
    assert "h1.title" in rows["no-title"].error
    assert "div.content" in rows["no-content"].error
    for row in rows.values():
        assert row.http_status == 200  # 响应本身成功，是内容提不出必填字段
        assert row.raw_path is not None


def test_raw_archive_keeps_original_bytes_untranscoded(
    lab_source: SourceConfig, db: Database, raw_store: RawStore
) -> None:
    run_source(lab_source, db, raw_store)

    with Session(db.engine) as session:
        rows = list(session.execute(select(RawRecordRow)).scalars())

    assert len(rows) == len(OK_CASES) + len(FAILED_CASES)
    for row in rows:
        payload = json.loads(Path(row.raw_path).read_text(encoding="utf-8"))
        archived = base64.b64decode(payload["body_base64"])
        # 留档字节与 mock 站点发出的原始字节完全一致（含 GBK 字节与 BOM）
        assert archived == LAB_CASES[row.external_id][1], row.external_id

    gbk_payload = json.loads(
        Path(next(r.raw_path for r in rows if r.external_id == "gbk")).read_text(
            encoding="utf-8"
        )
    )
    assert gbk_payload["encoding"] == "gbk"
    assert gbk_payload["encoding_source"] == "header"
    # 留档的是 GBK 原始字节，直接按 UTF-8 读应是乱码/失败，证明未转码
    archived = base64.b64decode(gbk_payload["body_base64"])
    with pytest.raises(UnicodeDecodeError):
        archived.decode("utf-8")
