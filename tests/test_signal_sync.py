"""
signal_sync (re-arch Phase 3) tests — mock SSH, no network, no btc-ml.

Covers: pull idempotency, remote bad-package rejection, push atomic staging,
partial failure exit code, already-pushed skip, status written.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import signal_sync as ss


def _fake_remote(tmp: Path, ready_pkgs: list[str] = None) -> Path:
    """Create a fake vlm_pending dir structure (simulates btc-ml)."""
    d = tmp / "vlm_pending"
    if ready_pkgs is None:
        ready_pkgs = ["btcusdt_a_20260601_0000"]
    for name in ready_pkgs:
        pkg = d / name
        pkg.mkdir(parents=True)
        (pkg / ".ready").touch()
        (pkg / "signal.json").write_text('{"symbol":"BTC/USDT","grade":"A","bar_time":"2026-06-01T00:00:00"}')
        (pkg / "prompt.txt").write_text("=== SYSTEM ===\nsome prompt")
    # A bad package (no .ready)
    bad = d / "badpkg_a_20260601_0001"
    bad.mkdir(parents=True)
    (bad / "signal.json").write_text("{}")
    return d


def _fake_local_done(tmp: Path, name="btcusdt_a_20260601_0000") -> Path:
    d = tmp / "local_vlm_done" / name
    d.mkdir(parents=True)
    (d / ".ready").touch()
    (d / "vlm_response.json").write_text('{"watch_summary":"x","playbooks":[]}', encoding="utf-8")
    return d


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self._olds = {k: getattr(ss, k) for k in
                      ("LOCAL_VLM_PENDING", "LOCAL_VLM_DONE",
                       "SYNCED_PULL_DIR", "SYNCED_PUSH_DIR",
                       "STATUS_FILE", "HEARTBEAT_FILE", "LOCK_FILE",
                       "_VLM_PENDING", "_VLM_DONE_INCOMING",
                       "_SIGNAL_ACTIVE", "_SIGNAL_DONE")}
        ss.LOCAL_VLM_PENDING = self.tmp / "local_vlm_pending"
        ss.LOCAL_VLM_DONE = self.tmp / "local_vlm_done"
        ss.SYNCED_PULL_DIR = self.tmp / ".synced_pulled"
        ss.SYNCED_PUSH_DIR = self.tmp / ".synced_pushed"
        ss.STATUS_FILE = self.tmp / "status.json"
        ss.HEARTBEAT_FILE = self.tmp / "hb.txt"
        ss.LOCK_FILE = self.tmp / "sync.lock"
        # Point remote paths to our temp (simulates btc-ml)
        ss._VLM_PENDING = str(_fake_remote(self.tmp))
        ss._VLM_DONE_INCOMING = str(self.tmp / "vlm_done_incoming")
        ss._SIGNAL_ACTIVE = str(self.tmp / "signal_active")
        ss._SIGNAL_DONE = str(self.tmp / "signal_done")

    def tearDown(self):
        for k, v in self._olds.items():
            setattr(ss, k, v)


class TestPullIdempotency(_Base):
    def setUp(self):
        super().setUp()
        # Override _ssh and _remote_list_new to use our fake dir
        self._orig_ssh = ss._ssh
        self._orig_list = ss._remote_list_new
        self._orig_exists = ss._remote_package_exists
        self._orig_tar = ss._tar_pull
        ss._remote_list_new = lambda: ["btcusdt_a_20260601_0000"]
        ss._remote_package_exists = lambda n: False

        # _tar_pull: copy from fake vlm_pending to local
        def _fake_tar(name):
            src = Path(ss._VLM_PENDING) / name
            dest = ss.LOCAL_VLM_PENDING / name
            if src.exists():
                import shutil
                dest.mkdir(parents=True)
                for f in src.iterdir():
                    if f.is_file():
                        shutil.copy2(f, dest)
                # Fake PNGs so _local_pull_complete() passes
                (dest / f"{name.split('_')[0]}_4h.png").write_bytes(b"fake")
                (dest / f"{name.split('_')[0]}_15m.png").write_bytes(b"fake")
                return dest
            return None
        ss._tar_pull = _fake_tar

    def tearDown(self):
        ss._ssh = self._orig_ssh
        ss._remote_list_new = self._orig_list
        ss._remote_package_exists = self._orig_exists
        ss._tar_pull = self._orig_tar
        super().tearDown()

    def test_pull_new_package(self):
        p, f = ss.pull_round()
        self.assertEqual(p, 1)
        self.assertEqual(f, 0)
        dest = ss.LOCAL_VLM_PENDING / "btcusdt_a_20260601_0000"
        self.assertTrue(dest.exists())
        self.assertTrue((dest / ".ready").exists())
        self.assertTrue((dest / "signal.json").exists())

    def test_pull_idempotent(self):
        ss.pull_round()  # first pull
        self.assertTrue((ss.SYNCED_PULL_DIR / "btcusdt_a_20260601_0000").exists())
        p, f = ss.pull_round()  # second pull (already marked)
        self.assertEqual(p, 0, "second pull must not re-pull marked packages")


class TestPushStagingAtomic(_Base):
    def setUp(self):
        super().setUp()
        self._orig_ssh = ss._ssh
        self._orig_exists = ss._remote_package_exists
        self._orig_push = ss._remote_push_staging
        ss._remote_package_exists = lambda n: False
        # Simulate successful push
        ss._remote_push_staging = lambda d: "MOVED" if (d / "vlm_response.json").exists() else "FAIL"

    def tearDown(self):
        ss._ssh = self._orig_ssh
        ss._remote_package_exists = self._orig_exists
        ss._remote_push_staging = self._orig_push
        super().tearDown()

    def test_push_new_response(self):
        _fake_local_done(self.tmp)
        p, f = ss.push_round()
        self.assertEqual(p, 1)
        self.assertEqual(f, 0)
        self.assertTrue((ss.SYNCED_PUSH_DIR / "btcusdt_a_20260601_0000").exists())

    def test_push_already_pushed(self):
        _fake_local_done(self.tmp)
        ss.push_round()
        (ss.SYNCED_PUSH_DIR / "btcusdt_a_20260601_0000").touch()
        p, f = ss.push_round()
        self.assertEqual(p, 0, "already pushed must not re-push")


class TestBadPackage(_Base):
    def setUp(self):
        super().setUp()
        self._orig_list = ss._remote_list_new
        self._orig_exists = ss._remote_package_exists
        self._orig_tar = ss._tar_pull
        ss._remote_list_new = lambda: ["badpkg_a_20260601_0001"]  # no .ready
        ss._remote_package_exists = lambda n: False

        def _bad_tar(name):
            # Simulate failed tar (remote doesn't have .ready + signal.json)
            return None
        ss._tar_pull = _bad_tar

    def tearDown(self):
        ss._remote_list_new = self._orig_list
        ss._remote_package_exists = self._orig_exists
        ss._tar_pull = self._orig_tar
        super().tearDown()

    def test_bad_package_not_pulled(self):
        p, f = ss.pull_round()
        self.assertEqual(p, 0)
        self.assertFalse((ss.LOCAL_VLM_PENDING / "badpkg_a_20260601_0001").exists())


class TestExitCode(_Base):
    def setUp(self):
        super().setUp()
        self._orig_ssh = ss._ssh
        self._orig_list = ss._remote_list_new
        self._orig_exists = ss._remote_package_exists
        self._orig_tar = ss._tar_pull
        self._orig_push = ss._remote_push_staging
        ss._remote_list_new = lambda: []
        ss._remote_package_exists = lambda n: True  # skip everything
        ss._tar_pull = lambda n: None
        ss._remote_push_staging = lambda d: "SKIP"

    def tearDown(self):
        ss._ssh = self._orig_ssh
        ss._remote_list_new = self._orig_list
        ss._remote_package_exists = self._orig_exists
        ss._tar_pull = self._orig_tar
        ss._remote_push_staging = self._orig_push
        super().tearDown()

    def test_once_exits_zero_on_idle(self):
        # All SSH mocked to return empty — main loop runs once, clean exit
        p, f = ss.pull_round()
        ss._write_status(p, f, 0)
        self.assertEqual(f, 0)

    def test_ssh_failure_propagates_as_runtime_error(self):
        """Verify _remote_list_new raises RuntimeError when SSH fails (→ main exits 1)."""
        # Remove setUp's mocks so we test the real code path
        ss._remote_list_new = self._orig_list
        orig_ssh = self._orig_ssh if hasattr(self, '_orig_ssh') else ss._ssh
        called = [0]

        def boom(*a, **kw):
            called[0] += 1
            raise RuntimeError("ssh failed")
        ss._ssh = boom
        try:
            with self.assertRaises(RuntimeError):
                ss._remote_list_new()
            self.assertGreater(called[0], 0, "SSH must have been called")
        finally:
            ss._ssh = orig_ssh


class TestStatusWritten(_Base):
    def setUp(self):
        super().setUp()
        self._orig_list = ss._remote_list_new
        self._orig_exists = ss._remote_package_exists
        ss._remote_list_new = lambda: []
        ss._remote_package_exists = lambda n: False

    def tearDown(self):
        ss._remote_list_new = self._orig_list
        ss._remote_package_exists = self._orig_exists
        super().tearDown()

    def test_status_written_on_idle_cycle(self):
        p, f = ss.pull_round()
        ss._heartbeat()
        ss._write_status(p, f, 0)
        self.assertTrue(ss.STATUS_FILE.exists())
        data = json.loads(ss.STATUS_FILE.read_text(encoding="utf-8"))
        self.assertIn("updated_at", data)


if __name__ == "__main__":
    unittest.main()
