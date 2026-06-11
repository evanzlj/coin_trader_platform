"""
单实例锁（§21.4）—— 防两个 executor 同时跑导致重复下单。

用 fcntl 排他文件锁；进程退出（正常/崩溃）锁自动释放，无 stale pidfile 问题。
executor 只在 unix（btc-ml / Mac 开发）跑，不在 Windows 跑。
"""
from __future__ import annotations

import fcntl
import os
from pathlib import Path


class AlreadyRunning(RuntimeError):
    """已有 executor 实例持锁。"""


class SingleInstance:
    def __init__(self, lock_path: Path) -> None:
        self.lock_path = lock_path
        self._fd = None

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = open(self.lock_path, "w")
        try:
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            self._fd.close()
            self._fd = None
            raise AlreadyRunning(
                f"已有 executor 实例在运行（lock: {self.lock_path}）"
            ) from e
        self._fd.seek(0)
        self._fd.truncate()
        self._fd.write(str(os.getpid()))
        self._fd.flush()

    def release(self) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
            self._fd.close()
            self._fd = None

    def __enter__(self) -> "SingleInstance":
        self.acquire()
        return self

    def __exit__(self, *exc) -> None:
        self.release()
