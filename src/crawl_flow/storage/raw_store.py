"""原始响应落盘：``data/raw/{source}/{YYYY-MM-DD}/{external_id}.json``。

落盘逻辑（:meth:`RawStore.save`）保持采集管道原有原子写入流程不变；
本模块只在其后追加只读的检索/导出辅助方法，按需加载单个 raw 文件，
不提供任何目录全量扫描入口。
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from crawl_flow.errors import StoreError
from crawl_flow.models import RawRecord

SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")
MAX_ID_LENGTH = 120

# 正文检索默认最多检查多少个候选 raw 文件，避免关键词命中过少时全量扫盘。
DEFAULT_BODY_SCAN_LIMIT = 1000


def safe_external_id(external_id: str) -> str:
    """把 external_id 转成安全的文件名片段。"""

    candidate = SAFE_NAME.sub("_", external_id).strip("._")
    return (candidate or "unknown")[:MAX_ID_LENGTH]


@dataclass(frozen=True)
class RawContent:
    """单个 raw 文件的读取结果。

    ``ok`` 为 False 时 ``payload`` 为空字典、``error`` 给出可读原因，
    调用方据此把损坏/无权限/缺失的文件标记出来，而不是中断整轮检索。
    """

    path: str
    ok: bool
    payload: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    @property
    def body(self) -> str:
        body = self.payload.get("body", "")
        return body if isinstance(body, str) else json.dumps(body, ensure_ascii=False)


@dataclass(frozen=True)
class BodySearchResult:
    """正文关键词过滤后的结果。"""

    matched: list[RawRecord]
    read_errors: list[RawContent]
    scanned: int


class RawStore:
    """负责把原始响应写成 JSON 文件，不读取数据库。"""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def save(
        self,
        *,
        source: str,
        external_id: str,
        payload: dict[str, Any],
        fetched_at: datetime,
    ) -> str:
        """原子写入一份原始响应，返回文件路径；失败抛 :class:`StoreError`。"""

        directory = self.root / source / fetched_at.strftime("%Y-%m-%d")
        path = directory / f"{safe_external_id(external_id)}.json"
        temp_path = path.with_name(f"{path.name}.tmp")
        try:
            directory.mkdir(parents=True, exist_ok=True)
            temp_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            temp_path.replace(path)
        except OSError as exc:
            raise StoreError(f"原始响应写入失败 path={path} error={exc}") from exc
        return str(path)

    def read_raw(self, path: str | Path) -> RawContent:
        """按需读取并解析一个 raw 文件，任何异常都收敛为 :class:`RawContent`。

        覆盖的异常场景：文件缺失、权限不足、JSON 损坏、顶层不是 JSON 对象、
        空文件/空响应（合法但 ``body`` 为空串）。该方法不抛异常，保证检索与
        导出在遇到坏文件时可以继续处理其余条目。
        """

        file_path = Path(path)
        try:
            raw_text = file_path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return RawContent(str(file_path), False, error="raw 文件不存在")
        except PermissionError as exc:
            return RawContent(str(file_path), False, error=f"raw 文件权限不足: {exc}")
        except OSError as exc:
            return RawContent(str(file_path), False, error=f"raw 文件读取失败: {exc}")

        if not raw_text.strip():
            return RawContent(str(file_path), False, error="raw 文件为空")
        try:
            payload = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            return RawContent(
                str(file_path),
                False,
                error=f"raw 文件 JSON 损坏: {exc.msg} (line {exc.lineno} column {exc.colno})",
            )
        if not isinstance(payload, dict):
            return RawContent(
                str(file_path), False, error="raw 文件顶层不是 JSON 对象"
            )
        return RawContent(str(file_path), True, payload=payload)

    def read_for_record(self, record: RawRecord) -> RawContent:
        """按 raw_records 行里登记的 ``raw_path`` 读取；未登记路径视为缺失。"""

        if not record.raw_path:
            return RawContent("", False, error="raw_records 未登记 raw_path")
        return self.read_raw(record.raw_path)

    def filter_by_body(
        self,
        candidates: list[RawRecord],
        keyword: str,
    ) -> BodySearchResult:
        """在数据库候选集之上按正文关键词过滤，只加载候选行对应的 raw 文件。

        关键词大小写敏感地做子串匹配（与 SQL ``LIKE`` 的错误关键词行为一致），
        匹配范围仅限 raw JSON 的 ``body`` 字段；坏文件不计入命中，但通过
        ``read_errors`` 返回，不中断处理。
        """

        matched: list[RawRecord] = []
        read_errors: list[RawContent] = []
        for record in candidates:
            content = self.read_for_record(record)
            if not content.ok:
                read_errors.append(content)
                continue
            if keyword in content.body:
                matched.append(record)
        return BodySearchResult(
            matched=matched,
            read_errors=read_errors,
            scanned=len(candidates),
        )
