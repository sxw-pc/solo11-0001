"""SQLite 预算账本：独立新表、UTC 自然日分区、原子记账与启动完整性校验。

设计要点：

- 使用独立的标准库 ``sqlite3`` 连接，不经过 SQLAlchemy，保证预算模块
  与业务表/ORM 完全解耦；表 ``budget_usage`` 是账本独占的新表。
- 每个额度操作都在 ``BEGIN IMMEDIATE`` 短事务里完成「读取当日计数 →
  校验上限 → 自增写回」，配合 WAL 与 ``busy_timeout``，多个采集进程
  并发记账时不会重计、漏计或超额。
- 启动时依次执行 ``PRAGMA integrity_check``、表结构比对与逐行数据校验，
  任何一项不过都抛 :class:`BudgetCorruptError`，拒绝启动而不是静默丢账。
- 按 UTC 自然日分区（``usage_date`` 列），跨天自动落到新行，旧数据保留
  备查，无需定时清理。
"""

from __future__ import annotations

import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from crawl_flow.budget.errors import BudgetCorruptError, BudgetError
from crawl_flow.budget.limits import BudgetLimits

TABLE_NAME = "budget_usage"
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
BUSY_TIMEOUT_SECONDS = 10
# 多个采集进程同时首次建库时，WAL 切换与建表需要排他锁；
# busy_timeout 对 PRAGMA journal_mode 不生效，需要应用层重试。
# 指数退避：0.05, 0.1, 0.2, ... 累计约 10 秒，与 busy_timeout 对齐。
STARTUP_MAX_ATTEMPTS = 15
STARTUP_RETRY_DELAY_SECONDS = 0.05

# 账本独占表的权威 DDL。启动时会与 sqlite_master 里实际存储的 DDL
# 规范化后逐字符比对，防止表被外部以错误结构重建。
SCHEMA_SQL = (
    "CREATE TABLE budget_usage (\n"
    "  source TEXT NOT NULL,\n"
    "  usage_date TEXT NOT NULL,\n"
    "  request_count INTEGER NOT NULL DEFAULT 0 CHECK (request_count >= 0),\n"
    "  response_bytes INTEGER NOT NULL DEFAULT 0 CHECK (response_bytes >= 0),\n"
    "  updated_at TEXT NOT NULL,\n"
    "  PRIMARY KEY (source, usage_date)\n"
    ")"
)


@dataclass(frozen=True)
class BudgetCounters:
    """某源某日的已用额度快照。"""

    source: str
    usage_date: str
    request_count: int
    response_bytes: int

    def remaining_requests(self, limits: BudgetLimits) -> int | None:
        if limits.daily_request_limit is None:
            return None
        return max(0, limits.daily_request_limit - self.request_count)

    def remaining_bytes(self, limits: BudgetLimits) -> int | None:
        if limits.daily_bytes_limit is None:
            return None
        return max(0, limits.daily_bytes_limit - self.response_bytes)


def utc_today() -> str:
    """当前 UTC 自然日，``YYYY-MM-DD``。"""

    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_ddl(sql: str) -> str:
    """忽略空白差异后比较 DDL，保证语义结构仍然一致。"""

    collapsed = re.sub(r"\s+", " ", sql).strip()
    # SQLite 回读的 DDL 会丢掉括号旁的空白，统一去掉括号相邻空白再比较。
    return re.sub(r"\s*([()])\s*", r"\1", collapsed)


class BudgetLedger:
    """单文件 SQLite 预算账本，线程安全（同进程多线程共享一个连接）。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.is_memory = str(self.path) == ":memory:"
        if not self.is_memory:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        # 并发首建时 WAL 切换/建表可能短暂拿不到排他锁，重试若干次；
        # 只有「文件不是数据库」这类确定性损坏才立即拒绝启动。
        last_locked: sqlite3.OperationalError | None = None
        for attempt in range(STARTUP_MAX_ATTEMPTS):
            self._conn = self._connect()
            try:
                self._initialize()
                self._verify_integrity()
                return
            except BudgetCorruptError:
                self.close()
                raise
            except sqlite3.OperationalError as exc:
                self.close()
                if self._is_lock_error(exc) and not self.is_memory:
                    last_locked = exc
                    if attempt + 1 < STARTUP_MAX_ATTEMPTS:
                        time.sleep(STARTUP_RETRY_DELAY_SECONDS * (2**attempt))
                        continue
                raise BudgetError(f"账本初始化失败 db={self.path} error={exc}") from exc
            except sqlite3.Error as exc:
                self.close()
                raise BudgetError(f"账本初始化失败 db={self.path} error={exc}") from exc
        raise BudgetError(
            f"账本初始化失败：等待并发建库锁超时 db={self.path} error={last_locked}"
        )

    @staticmethod
    def _is_lock_error(exc: sqlite3.Error) -> bool:
        message = str(exc).lower()
        return "locked" in message or "busy" in message

    # ---- 连接与建表 ----
    def _connect(self) -> sqlite3.Connection:
        try:
            conn = sqlite3.connect(
                str(self.path),
                timeout=BUSY_TIMEOUT_SECONDS,
                isolation_level=None,  # 手动事务，避免隐式 BEGIN 干扰
                check_same_thread=False,
            )
        except sqlite3.Error as exc:
            raise BudgetError(f"账本打开失败 db={self.path} error={exc}") from exc
        # 忙时等待而不是立刻报 locked，给并发进程留出串行化窗口。
        conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_SECONDS * 1000}")
        if not self.is_memory:
            self._enable_wal(conn)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _enable_wal(self, conn: sqlite3.Connection) -> None:
        """启用 WAL。

        ``PRAGMA journal_mode=WAL`` 不受 busy_timeout 保护，两个进程
        同时首次建库时会立刻报 ``database is locked``，在连接内按
        指数退避重试；非锁类错误（文件不是数据库）属于确定性损坏。
        """

        for attempt in range(STARTUP_MAX_ATTEMPTS):
            try:
                conn.execute("PRAGMA journal_mode = WAL")
                return
            except sqlite3.OperationalError as exc:
                if not self._is_lock_error(exc) or attempt + 1 >= STARTUP_MAX_ATTEMPTS:
                    conn.close()
                    raise
                time.sleep(STARTUP_RETRY_DELAY_SECONDS * (2**attempt))
            except sqlite3.DatabaseError as exc:
                conn.close()
                raise BudgetCorruptError(
                    f"账本文件无法识别或已损坏 db={self.path} error={exc}"
                ) from exc

    def _initialize(self) -> None:
        """幂等建表；新库直接创建，既有库交给完整性校验。"""

        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS budget_usage ("
            "source TEXT NOT NULL, "
            "usage_date TEXT NOT NULL, "
            "request_count INTEGER NOT NULL DEFAULT 0 CHECK (request_count >= 0), "
            "response_bytes INTEGER NOT NULL DEFAULT 0 CHECK (response_bytes >= 0), "
            "updated_at TEXT NOT NULL, "
            "PRIMARY KEY (source, usage_date))"
        )

    # ---- 启动完整性校验：损坏即拒绝启动 ----
    def _verify_integrity(self) -> None:
        rows = self._conn.execute("PRAGMA integrity_check").fetchall()
        result = [row[0] for row in rows]
        if result != ["ok"]:
            raise BudgetCorruptError(
                f"账本完整性检查失败 db={self.path} result={result[:5]}"
            )

        actual_ddl = self._conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (TABLE_NAME,),
        ).fetchone()
        if actual_ddl is None or not actual_ddl[0]:
            raise BudgetCorruptError(f"账本缺少 {TABLE_NAME} 表 db={self.path}")
        if _normalize_ddl(actual_ddl[0]) != _normalize_ddl(SCHEMA_SQL):
            raise BudgetCorruptError(
                f"账本表结构与当前版本不一致 db={self.path} "
                f"actual={_normalize_ddl(actual_ddl[0])!r} "
                f"expected={_normalize_ddl(SCHEMA_SQL)!r}"
            )

        # 逐行校验：CHECK 约束只挡住新写入，旧数据可能来自被篡改的文件。
        for source, usage_date, requests, bytes_, updated_at in self._conn.execute(
            "SELECT source, usage_date, request_count, response_bytes, updated_at "
            "FROM budget_usage"
        ):
            self._validate_row(source, usage_date, requests, bytes_, updated_at)

    def _validate_row(
        self,
        source: Any,
        usage_date: Any,
        requests: Any,
        response_bytes: Any,
        updated_at: Any,
    ) -> None:
        problems: list[str] = []
        if not isinstance(source, str) or not source:
            problems.append("source 缺失或类型错误")
        if not isinstance(usage_date, str) or not DATE_PATTERN.match(usage_date or ""):
            problems.append(f"usage_date 非法: {usage_date!r}")
        # sqlite 动态类型可能存进 TEXT/REAL，必须严格核对。
        if isinstance(requests, bool) or not isinstance(requests, int) or requests < 0:
            problems.append(f"request_count 非法: {requests!r}")
        if isinstance(response_bytes, bool) or not isinstance(response_bytes, int) or response_bytes < 0:
            problems.append(f"response_bytes 非法: {response_bytes!r}")
        if not isinstance(updated_at, str) or not updated_at:
            problems.append("updated_at 缺失或类型错误")
        if problems:
            raise BudgetCorruptError(
                f"账本数据行损坏 source={source!r} date={usage_date!r} "
                f"problems={problems}"
            )

    # ---- 记账（均为原子短事务） ----
    def reserve_request(
        self,
        limits: BudgetLimits,
        *,
        usage_date: str | None = None,
    ) -> BudgetCounters:
        """预留一次请求额度并把请求计数加一。

        在 HTTP 请求**发出之前**调用：任何一个维度触顶都会抛
        :class:`BudgetExceededError` 且计数不变，因此熔断的那次请求
        永远不会真正发出去。
        """

        day = usage_date or utc_today()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            current = self._read_locked(limits.source, day)
            # 字节上限可能在上一个响应记账后已经触顶，下一次请求同样要拦住。
            if (
                limits.daily_bytes_limit is not None
                and current.response_bytes >= limits.daily_bytes_limit
            ):
                self._conn.rollback()
                self._raise_exceeded(
                    limits,
                    day,
                    metric="bytes",
                    used=current.response_bytes,
                    limit=limits.daily_bytes_limit,
                )
            next_requests = current.request_count + 1
            if (
                limits.daily_request_limit is not None
                and next_requests > limits.daily_request_limit
            ):
                self._conn.rollback()
                self._raise_exceeded(
                    limits,
                    day,
                    metric="requests",
                    used=current.request_count,
                    limit=limits.daily_request_limit,
                )
            self._write_locked(
                limits.source,
                day,
                request_count=next_requests,
                response_bytes=current.response_bytes,
            )
            self._conn.commit()
        except BudgetCorruptError:
            self._rollback_safely()
            raise
        except sqlite3.Error as exc:
            self._rollback_safely()
            raise BudgetError(
                f"请求额度记账失败 source={limits.source} error={exc}"
            ) from exc
        return self.get_counters(limits.source, usage_date=day)

    def add_response_bytes(
        self,
        source: str,
        byte_count: int,
        *,
        usage_date: str | None = None,
    ) -> BudgetCounters:
        """收到响应后累加实际字节数，返回最新快照。"""

        if byte_count < 0:
            raise BudgetError(f"响应字节数不能为负 source={source} bytes={byte_count}")
        day = usage_date or utc_today()
        try:
            self._conn.execute("BEGIN IMMEDIATE")
            current = self._read_locked(source, day)
            self._write_locked(
                source,
                day,
                request_count=current.request_count,
                response_bytes=current.response_bytes + byte_count,
            )
            self._conn.commit()
        except sqlite3.Error as exc:
            self._rollback_safely()
            raise BudgetError(
                f"响应字节记账失败 source={source} error={exc}"
            ) from exc
        return self.get_counters(source, usage_date=day)

    def _read_locked(self, source: str, usage_date: str) -> BudgetCounters:
        row = self._conn.execute(
            "SELECT request_count, response_bytes FROM budget_usage "
            "WHERE source = ? AND usage_date = ?",
            (source, usage_date),
        ).fetchone()
        if row is None:
            return BudgetCounters(
                source=source,
                usage_date=usage_date,
                request_count=0,
                response_bytes=0,
            )
        requests, response_bytes = row
        # 运行中再次校验，防止文件在进程存活期间被外部篡改。
        self._validate_row(source, usage_date, requests, response_bytes, _utc_now_iso())
        return BudgetCounters(source, usage_date, int(requests), int(response_bytes))

    def _write_locked(
        self,
        source: str,
        usage_date: str,
        *,
        request_count: int,
        response_bytes: int,
    ) -> None:
        self._conn.execute(
            "INSERT INTO budget_usage "
            "(source, usage_date, request_count, response_bytes, updated_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(source, usage_date) DO UPDATE SET "
            "request_count = excluded.request_count, "
            "response_bytes = excluded.response_bytes, "
            "updated_at = excluded.updated_at",
            (source, usage_date, request_count, response_bytes, _utc_now_iso()),
        )

    # ---- 查询 ----
    def get_counters(
        self, source: str, *, usage_date: str | None = None
    ) -> BudgetCounters:
        """读取某源当日计数；当天尚无记录返回零值快照。"""

        day = usage_date or utc_today()
        try:
            row = self._conn.execute(
                "SELECT request_count, response_bytes FROM budget_usage "
                "WHERE source = ? AND usage_date = ?",
                (source, day),
            ).fetchone()
        except sqlite3.Error as exc:
            raise BudgetError(f"账本查询失败 source={source} error={exc}") from exc
        if row is None:
            return BudgetCounters(source=source, usage_date=day, request_count=0, response_bytes=0)
        requests, response_bytes = row
        self._validate_row(source, day, requests, response_bytes, _utc_now_iso())
        return BudgetCounters(source, day, int(requests), int(response_bytes))

    def list_counters(self, *, usage_date: str | None = None) -> list[BudgetCounters]:
        """列出当天所有有记账记录的源。"""

        day = usage_date or utc_today()
        try:
            rows = self._conn.execute(
                "SELECT source, usage_date, request_count, response_bytes, updated_at "
                "FROM budget_usage WHERE usage_date = ? ORDER BY source",
                (day,),
            ).fetchall()
        except sqlite3.Error as exc:
            raise BudgetError(f"账本批量查询失败 error={exc}") from exc
        result: list[BudgetCounters] = []
        for source, date_, requests, bytes_, updated_at in rows:
            self._validate_row(source, date_, requests, bytes_, updated_at)
            result.append(BudgetCounters(source, date_, int(requests), int(bytes_)))
        return result

    def is_byte_limit_reached(
        self,
        limits: BudgetLimits,
        *,
        usage_date: str | None = None,
    ) -> bool:
        """字节上限是否已经触顶（用于在响应记账后阻断下一次请求）。"""

        if limits.daily_bytes_limit is None:
            return False
        counters = self.get_counters(limits.source, usage_date=usage_date)
        return counters.response_bytes >= limits.daily_bytes_limit

    def _raise_exceeded(
        self,
        limits: BudgetLimits,
        usage_date: str,
        *,
        metric: str,
        used: int,
        limit: int,
    ) -> None:
        from crawl_flow.budget.errors import BudgetExceededError

        if metric == "requests":
            reason = f"每日请求次数上限 {limit} 已用尽"
        else:
            reason = f"每日响应字节上限 {limit} 已用尽"
        raise BudgetExceededError(
            limits.source,
            reason,
            metric=metric,
            used=used,
            limit=limit,
            usage_date=usage_date,
        )

    def _rollback_safely(self) -> None:
        try:
            self._conn.rollback()
        except sqlite3.Error:
            pass

    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error:
            pass
