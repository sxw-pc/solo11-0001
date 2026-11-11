"""部署级运行互斥锁。

同一 SQLite 数据库同一时刻只允许一个 ``crawl-flow run`` 写入进程。

实现基于操作系统级建议锁（POSIX 的 ``fcntl.flock``、Windows 的
``msvcrt.locking``），而不是「锁文件存在即占用」的朴素方案：

* 锁由内核持有，进程正常退出、被 ``kill -9`` 或机器重启后都会自动释放，
  残留的锁文件不会永久挡住后续运行；
* 锁与打开文件描述符绑定，关闭文件即释放，不存在手工清理状态；
* 锁文件里只写入持有者的诊断信息（PID、命令行、启动时间），不作为
  占用判断依据，所以里面的过期内容没有任何副作用。
"""

from __future__ import annotations

import os
import platform
import sys
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from typing import TextIO

from crawl_flow.errors import LockUnavailable

LOCK_SUFFIX = ".run.lock"
WaitingCallback = Callable[[], None]


class RunLock:
    """针对某个数据库文件的进程级互斥锁。

    典型用法::

        lock = RunLock(Path("data/crawl.db"))
        lock.acquire(timeout=0)
        try:
            ...  # 写库的采集流程
        finally:
            lock.release()

    注意 ``lock`` 必须被变量持有到业务结束：锁与打开的文件描述符绑定，
    对象一旦被垃圾回收就会关闭描述符、提前释放锁。也可以用
    ``with RunLock(path).acquire() as lock:``（with 会保持引用）。

    ``:memory:`` 数据库没有可共享的文件，锁退化为空操作（每个进程各自独立）。
    """

    def __init__(self, db_path: str | Path) -> None:
        self.db_path = Path(db_path)
        self._file: TextIO | None = None

    @property
    def lock_path(self) -> Path | None:
        """锁文件路径；``:memory:`` 数据库返回 ``None``。"""

        if str(self.db_path) == ":memory:":
            return None
        return self.db_path.with_name(self.db_path.name + LOCK_SUFFIX)

    def acquire(
        self,
        *,
        timeout: float | None = 0.0,
        on_waiting: WaitingCallback | None = None,
    ) -> "RunLock":
        """获取锁。

        ``timeout``：

        * ``0``（默认）：拿不到立刻抛 :class:`LockUnavailable`；
        * 正数：最多等待这么多秒，超时仍拿不到则抛 :class:`LockUnavailable`；
        * ``None``：一直排队，直到拿到锁。

        ``on_waiting`` 是可选的无参回调，发现锁被占用、进入等待前调用一次，
        供 CLI 打印提示。
        """

        path = self.lock_path
        if path is None:
            return self
        path.parent.mkdir(parents=True, exist_ok=True)
        # 以 a+ 打开：锁文件可被多个进程同时打开，互斥由内核锁保证；
        # 进程被强杀后文件可能残留，但其中的锁已经随进程消失。
        self._file = open(path, "a+", encoding="utf-8")
        try:
            if platform.system() == "Windows":
                self._acquire_windows(timeout, on_waiting)
            else:
                self._acquire_posix(timeout, on_waiting)
        except BaseException:
            # 加锁失败时关闭描述符，避免泄漏文件句柄。
            self._close_file()
            raise
        self._write_owner()
        return self

    def release(self) -> None:
        """释放锁并关闭描述符；未持锁时为空操作。"""

        if self._file is None:
            return
        self._close_file()
        # 不删除锁文件：删除会和其他等待者的打开句柄产生竞争；
        # 锁文件体积恒定（持有者信息只有一行），且 *.run.lock 已被 gitignore。

    def holder_pid(self) -> int | None:
        """读取锁文件里记录的持有者 PID，仅用于报错提示，不参与占用判断。"""

        path = self.lock_path
        if path is None or not path.exists():
            return None
        try:
            head = path.read_text(encoding="utf-8").strip().splitlines()[0]
        except (OSError, IndexError):
            return None
        marker = "pid="
        if marker not in head:
            return None
        raw = head.split(marker, 1)[1].split()[0]
        try:
            return int(raw)
        except ValueError:
            return None

    def __enter__(self) -> "RunLock":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()

    # -- 内部实现 -----------------------------------------------------------

    def _acquire_posix(self, timeout: float | None, on_waiting: WaitingCallback | None) -> None:
        import fcntl

        assert self._file is not None
        if timeout == 0:
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise self._unavailable() from exc
            return

        deadline = None if timeout is None else time.monotonic() + timeout
        notified = False
        while True:
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except BlockingIOError:
                if not notified:
                    if on_waiting is not None:
                        on_waiting()
                    notified = True
                if deadline is not None and time.monotonic() >= deadline:
                    raise self._unavailable()
                time.sleep(0.1)

    def _acquire_windows(
        self, timeout: float | None, on_waiting: WaitingCallback | None
    ) -> None:
        import msvcrt

        assert self._file is not None
        # a+ 模式打开时文件指针位于 EOF，显式回到第 0 字节，
        # 保证所有进程争用同一个锁字节（内核锁，进程死亡自动释放）。
        self._file.seek(0)
        deadline = None if timeout is None else time.monotonic() + timeout
        notified = False
        while True:
            try:
                msvcrt.locking(self._file.fileno(), msvcrt.LK_NBLCK, 1)
                return
            except OSError:
                if timeout == 0:
                    raise self._unavailable()
                if not notified:
                    if on_waiting is not None:
                        on_waiting()
                    notified = True
                if deadline is not None and time.monotonic() >= deadline:
                    raise self._unavailable()
                self._file.seek(0)
                time.sleep(0.1)

    def _write_owner(self) -> None:
        if self._file is None:
            return
        try:
            self._file.seek(0)
            self._file.truncate()
            self._file.write(
                "pid={pid} started_at={started} cmd={cmd}\n".format(
                    pid=os.getpid(),
                    started=datetime.now(timezone.utc).isoformat(),
                    cmd=" ".join(sys.argv[:3]),
                )
            )
            self._file.flush()
        except OSError:
            # 诊断信息写不进去不影响互斥本身。
            pass

    def _unavailable(self) -> LockUnavailable:
        holder = self.holder_pid()
        holder_text = f"，持有者 pid={holder}" if holder else ""
        return LockUnavailable(
            f"数据库 {self.db_path} 正被另一个 run 进程占用{holder_text}；"
            "如需排队等待请加 --wait-for-lock（可用 --lock-timeout 限制等待秒数）"
        )

    def _close_file(self) -> None:
        if self._file is not None:
            try:
                self._file.close()
            finally:
                self._file = None
