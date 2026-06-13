"""
单实例锁（§21.4）—— 防两个进程同时跑（executor / monitor / openclaw / pusher）。

Unix：fcntl 排他文件锁。
Windows：msvcrt.locking 字节范围锁（同样进程退出自动释放，无 stale pidfile）。
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


class AlreadyRunning(RuntimeError):
    """已有同名实例持锁。"""


class SingleInstance:
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fd = None

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        if sys.platform == "win32":
            self._acquire_win()
        else:
            self._acquire_unix()

    def _acquire_unix(self) -> None:
        import fcntl
        self._fd = open(self.lock_path, "w")
        try:
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            self._fd.close()
            self._fd = None
            raise AlreadyRunning(f"已有实例在运行（lock: {self.lock_path}）") from e
        self._fd.seek(0)
        self._fd.truncate()
        self._fd.write(str(os.getpid()))
        self._fd.flush()

    def _acquire_win(self) -> None:
        import msvcrt
        self._fd = open(self.lock_path, "w")
        try:
            # 锁第 0 字节；进程退出时 OS 自动释放
            msvcrt.locking(self._fd.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as e:
            self._fd.close()
            self._fd = None
            raise AlreadyRunning(f"已有实例在运行（lock: {self.lock_path}）") from e
        self._fd.write(str(os.getpid()))
        self._fd.flush()

    def release(self) -> None:
        if self._fd is not None:
            if sys.platform == "win32":
                import msvcrt
                try:
                    self._fd.seek(0)
                    msvcrt.locking(self._fd.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            else:
                import fcntl
                fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
            self._fd.close()
            self._fd = None

    def __enter__(self) -> "SingleInstance":
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        self.release()
