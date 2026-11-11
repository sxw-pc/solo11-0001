"""部署级互斥锁：flock 互斥、等待/超时、强杀后自动释放。"""

from __future__ import annotations

import os
import platform
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from crawl_flow.errors import LockUnavailable
from crawl_flow.storage.locking import LOCK_SUFFIX, RunLock

pytestmark = pytest.mark.skipif(
    platform.system() == "Windows", reason="flock 行为测试仅覆盖 POSIX"
)


def test_memory_db_lock_is_noop(tmp_path: Path) -> None:
    lock = RunLock(":memory:")
    assert lock.lock_path is None
    assert lock.acquire() is lock
    lock.release()


def test_second_nonblocking_acquire_is_rejected(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl.db"
    first = RunLock(db_path).acquire()
    try:
        second = RunLock(db_path)
        with pytest.raises(LockUnavailable) as excinfo:
            second.acquire()
        assert str(db_path) in str(excinfo.value)
        assert "--wait-for-lock" in str(excinfo.value)
    finally:
        first.release()


def test_locks_on_different_dbs_are_independent(tmp_path: Path) -> None:
    first = RunLock(tmp_path / "a.db").acquire()
    other = RunLock(tmp_path / "b.db").acquire()
    first.release()
    other.release()


def test_waiting_acquire_succeeds_after_release(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl.db"
    holder = RunLock(db_path).acquire()
    waiting = RunLock(db_path)
    notifications = 0

    def on_waiting() -> None:
        nonlocal notifications
        notifications += 1
        holder.release()  # 模拟持锁进程很快结束

    start = time.monotonic()
    waiting.acquire(timeout=10, on_waiting=on_waiting)
    assert time.monotonic() - start < 5
    assert notifications == 1
    waiting.release()


def test_timeout_raises_lock_unavailable(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl.db"
    holder = RunLock(db_path).acquire()
    try:
        with pytest.raises(LockUnavailable):
            RunLock(db_path).acquire(timeout=0.3)
    finally:
        holder.release()


def test_indefinite_wait_uses_none_timeout(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl.db"
    holder = RunLock(db_path).acquire()
    waiting = RunLock(db_path)

    def release_soon() -> None:
        time.sleep(0.2)
        holder.release()

    import threading

    thread = threading.Thread(target=release_soon, daemon=True)
    thread.start()
    waiting.acquire(timeout=None)  # 不应抛异常
    thread.join(timeout=5)
    waiting.release()


def test_lock_released_after_holder_process_killed(tmp_path: Path) -> None:
    """持锁进程被 kill -9 后，内核自动释放锁，新进程可立即拿到。"""

    db_path = tmp_path / "crawl.db"
    lock_path = db_path.with_name(db_path.name + LOCK_SUFFIX)
    # 注意：锁对象必须被变量持有，否则会被 GC 关闭 fd 而提前释放。
    holder_code = textwrap.dedent(
        f"""
        import sys, time
        sys.path.insert(0, {sys.path[0]!r})
        from crawl_flow.storage.locking import RunLock
        lock = RunLock({str(db_path)!r})
        lock.acquire()
        print("locked", flush=True)
        time.sleep(60)
        """
    )
    proc = subprocess.Popen(
        [sys.executable, "-I", "-c", holder_code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert proc.stdout is not None
        assert proc.stdout.readline().strip() == "locked"
        # 锁文件残留，但锁归内核持有：父进程此刻必须拿不到锁。
        assert lock_path.exists()
        with pytest.raises(LockUnavailable):
            RunLock(db_path).acquire()

        proc.kill()
        proc.wait(timeout=10)
        assert proc.returncode == -9

        # 立即重跑必须拿到锁，无需人工清理残留锁文件。
        start = time.monotonic()
        retry = RunLock(db_path).acquire(timeout=5)
        assert time.monotonic() - start < 2
        retry.release()
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)


def test_holder_pid_is_diagnostic_only(tmp_path: Path) -> None:
    db_path = tmp_path / "crawl.db"
    lock = RunLock(db_path).acquire()
    try:
        assert lock.holder_pid() == os.getpid()
    finally:
        lock.release()
    # 释放后锁文件仍在（不删除），但不影响再次加锁。
    assert db_path.with_name(db_path.name + LOCK_SUFFIX).exists()
    RunLock(db_path).acquire().release()
