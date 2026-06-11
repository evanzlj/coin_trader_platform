#!/usr/bin/env python3
"""
Executor 主进程（§4, §20）—— 部署在 btc-ml（新加坡）。

每 POLL_SECONDS 一轮（干完再 sleep，非定时器 + 单实例锁，防请求堆积 §4）：
  (a) 管理已开仓：查订单状态推进 ACTIVATED / TP1_HIT（manage_open_position）
  (b) 新 15m 收线 bar：对 WAITING 的 playbook 跑状态机；激活则开仓（slot 分配 + open_position）
  (c) 全 playbook 终态 → 归档 signal_done；写心跳

state.json 由 executor 本地维护（唯一 writer），原子写 tmp + os.replace。
ExecutorEngine.tick() 可注入 reader / brokers 单独测试（mock，不碰交易所）。
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Callable, Optional

import pandas as pd

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from live import exec_config as cfg
from live.broker.base import Broker
from live.data_reader import OhlcvReader
from live.playbook_fsm import (
    Bar, PBStatus, TERMINAL, WaitEvent,
    step_waiting, validate_levels,
)
from live.position_manager import open_position, manage_open_position
from live.slot_pool import Account, build_accounts, build_occupancy, allocate
from live.single_instance import SingleInstance, AlreadyRunning
from live import reconcile
from live import notify

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

WAITING_STATUSES = {
    PBStatus.WAITING_FOR_PRIMARY_TOUCH.value,
    PBStatus.WAITING_FOR_ACTIVATION.value,
}
OPEN_STATUSES = {PBStatus.ACTIVATED.value, PBStatus.TP1_HIT.value}
TERMINAL_VALUES = {s.value for s in TERMINAL}


def make_client_id_base(signal_dir: str, hypothesis: str) -> str:
    """确定性 client_id 前缀（幂等：同 signal+hypothesis 永远同值，§10.1）。
    字母数字、短，兼容 Binance / OKX 格式（P1-5 实测再核对）。"""
    h = hashlib.md5(f"{signal_dir}|{hypothesis}".encode()).hexdigest()[:12]
    return f"x{h}"


class ExecutorEngine:
    def __init__(self, reader: OhlcvReader, brokers: dict[str, Broker],
                 accounts: list[Account],
                 c4_ok: Optional[Callable] = None) -> None:
        self.reader = reader
        self.brokers = brokers              # account_label -> Broker
        self.accounts = accounts
        self.c4_ok = c4_ok
        self.last_processed: dict[str, pd.Timestamp] = self._load_cursor()
        self.last_reconcile: Optional[pd.Timestamp] = None

    # ── state.json I/O ────────────────────────────────────────────────────────

    def load_states(self) -> list[tuple[Path, dict]]:
        """扫 signal_active/，只认带 .ready 且 state.json 完整的包（§20.2）。"""
        out = []
        if not cfg.SIGNAL_ACTIVE.exists():
            return out
        for d in sorted(cfg.SIGNAL_ACTIVE.iterdir()):
            if not d.is_dir() or not (d / ".ready").exists():
                continue
            sf = d / "state.json"
            if not sf.exists():
                continue
            try:
                out.append((d, json.loads(sf.read_text(encoding="utf-8"))))
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("bad state.json %s: %s", d.name, e)
        return out

    @staticmethod
    def save_state(pkg_dir: Path, state: dict) -> None:
        tmp = pkg_dir / "state.json.tmp"
        tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, pkg_dir / "state.json")

    @staticmethod
    def archive(pkg_dir: Path) -> None:
        cfg.SIGNAL_DONE.mkdir(parents=True, exist_ok=True)
        dest = cfg.SIGNAL_DONE / pkg_dir.name
        if dest.exists():
            shutil.rmtree(dest)
        shutil.move(str(pkg_dir), str(dest))

    def _load_cursor(self) -> dict:
        if cfg.CURSOR_FILE.exists():
            try:
                raw = json.loads(cfg.CURSOR_FILE.read_text(encoding="utf-8"))
                return {s: pd.Timestamp(t) for s, t in raw.items()}
            except Exception:
                pass
        return {}

    def _save_cursor(self) -> None:
        cfg.CURSOR_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = cfg.CURSOR_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({s: t.isoformat() for s, t in self.last_processed.items()}),
                       encoding="utf-8")
        os.replace(tmp, cfg.CURSOR_FILE)

    # ── 主步骤 ────────────────────────────────────────────────────────────────

    def tick(self, now: Optional[pd.Timestamp] = None) -> None:
        now = now or pd.Timestamp.now("UTC")
        pkgs = self.load_states()

        # (a) 管理已开仓
        for pkg_dir, state in pkgs:
            symbol = state["symbol"]
            changed = False
            for pb in state["playbooks"]:
                if pb["status"] in OPEN_STATUSES:
                    if (pb.get("exec") or {}).get("manual_override"):
                        continue                       # 人工接管中，executor 让位（§10.3）
                    broker = self.brokers.get(pb["exec"]["account"])
                    if broker is None:
                        continue
                    result = manage_open_position(broker, symbol, pb)
                    if result:
                        changed = True
                        notify.trade_log(result, symbol=symbol,
                                         hypothesis=pb.get("hypothesis"),
                                         account=pb["exec"]["account"],
                                         actual_r=pb["exec"].get("actual_r_usdt"))
            if changed:
                self.save_state(pkg_dir, state)

        # (b) 新收线 bar → WAITING 推进 + 开仓
        symbols = {st["symbol"] for _, st in pkgs}
        for symbol in symbols:
            if self.reader.is_stale(symbol, now=now):
                logger.warning("data stale for %s — skip new entries", symbol)
                continue
            last = self.last_processed.get(symbol)
            latest = self.reader.latest_closed_bar_time(symbol, now=now)
            if latest is None or (last is not None and latest <= last):
                continue
            bars_df = self.reader.load_closed_since(symbol, last, now=now)
            bars = [Bar(r.open_time, r.open, r.high, r.low, r.close)
                    for r in bars_df.itertuples(index=False)]
            for pkg_dir, state in pkgs:
                if state["symbol"] != symbol:
                    continue
                changed = False
                for pb in state["playbooks"]:
                    for bar in bars:
                        if pb["status"] not in WAITING_STATUSES:
                            break
                        ev = step_waiting(pb, bar)
                        if ev != WaitEvent.NONE:
                            changed = True
                        if ev == WaitEvent.ACTIVATE:
                            self._enter(state, symbol, pb, bar, [s for _, s in pkgs])
                if changed:
                    self.save_state(pkg_dir, state)
            self.last_processed[symbol] = latest
        self._save_cursor()

        # (b.5) 定期对账（§21 #5）—— 用内存 pkgs，归档交给 (c)
        if (self.last_reconcile is None
                or (now - self.last_reconcile) >= pd.Timedelta(minutes=cfg.RECONCILE_MINUTES)):
            for pkg_dir, state in pkgs:
                symbol = state["symbol"]
                rec_changed = False
                for pb in state["playbooks"]:
                    if pb["status"] in OPEN_STATUSES and not (pb.get("exec") or {}).get("manual_override"):
                        broker = self.brokers.get(pb["exec"]["account"])
                        if broker and reconcile.reconcile_position(broker, symbol, pb) != "ok":
                            rec_changed = True
                if rec_changed:
                    self.save_state(pkg_dir, state)
            self.last_reconcile = now

        # (c) 归档终态 + 心跳
        for pkg_dir, state in pkgs:
            if all(pb["status"] in TERMINAL_VALUES for pb in state["playbooks"]):
                logger.info("all playbooks terminal — archiving %s", pkg_dir.name)
                self.archive(pkg_dir)
        self._heartbeat(now)

    def _enter(self, state: dict, symbol: str, pb: dict, bar: Bar,
               all_states: list) -> None:
        """激活进场：校验 → 分配 slot → 开仓。失败不占 slot、不记 ACTIVATED。
        all_states = 本 tick 内存中的所有 state（含已激活的），用于 occ 防同 tick 超分配。"""
        ok, reason = validate_levels(pb)
        if not ok:
            pb["status"] = PBStatus.DONE_CANCELLED.value
            pb["result"] = f"invalid:{reason}"
            logger.warning("%s %s level invalid: %s", symbol, pb.get("hypothesis"), reason)
            return

        margin = cfg.SYMBOL_MARGIN[symbol]
        occ = build_occupancy(all_states)
        account = allocate(self.accounts, symbol, margin, occ, c4_ok=self.c4_ok)
        if account is None:
            pb["status"] = PBStatus.DONE_CANCELLED.value
            pb["result"] = "skipped_no_slot"
            notify.trade_log("SKIPPED_NO_SLOT", symbol=symbol, hypothesis=pb.get("hypothesis"))
            notify.feishu_alert(f"no slot for {symbol} {pb.get('hypothesis')}")
            return

        broker = self.brokers[account]
        base = make_client_id_base(state["signal_dir"], pb["hypothesis"])
        try:
            ex = open_position(broker, symbol, pb, bar.close, account, margin, base)
            pb["exec"] = ex
            pb["status"] = PBStatus.ACTIVATED.value
            logger.info("ENTERED %s %s @ %s acc=%s qty=%s 1R=%.2f",
                        symbol, pb.get("hypothesis"), ex["entry_price"], account,
                        ex["qty"], ex["actual_r_usdt"])
            notify.trade_log("ACTIVATED", symbol=symbol, hypothesis=pb.get("hypothesis"),
                             account=account, exchange=ex["exchange"], entry=ex["entry_price"],
                             qty=ex["qty"], actual_r=ex["actual_r_usdt"])
        except Exception as e:
            pb["status"] = PBStatus.DONE_CANCELLED.value
            pb["result"] = f"entry_failed:{e}"
            logger.error("ENTRY FAILED %s %s: %s", symbol, pb.get("hypothesis"), e)
            notify.trade_log("ERROR_ENTRY_FAILED", symbol=symbol, hypothesis=pb.get("hypothesis"), error=str(e))
            notify.feishu_alert(f"ENTRY FAILED {symbol} {pb.get('hypothesis')}: {e}")

    def _heartbeat(self, now: pd.Timestamp) -> None:
        cfg.HEARTBEAT_FILE.parent.mkdir(parents=True, exist_ok=True)
        cfg.HEARTBEAT_FILE.write_text(now.isoformat())


# ── main（部署入口；broker 构建等 adapter #4/#5）──────────────────────────────

def build_brokers(keys: dict) -> dict[str, Broker]:
    """按 keys 构建每账户 Broker（binance→BinanceBroker, okx→OKXBroker）。
    交易所 SDK 延迟到此 import（Mac 开发机未装，不影响 import executor / 跑单测）。"""
    from live.broker.binance import BinanceBroker
    from live.broker.okx import OKXBroker
    brokers: dict[str, Broker] = {}
    for a in keys["binance"]:
        brokers[a["label"]] = BinanceBroker(a["label"], a["api_key"], a["secret"],
                                            cfg.binance_base_url())
    for a in keys["okx"]:
        brokers[a["label"]] = OKXBroker(a["label"], a["api_key"], a["secret"],
                                        a["passphrase"], cfg.okx_simulated_flag())
    return brokers


def main() -> None:
    lock = SingleInstance(cfg.LOCK_FILE)
    try:
        lock.acquire()
    except AlreadyRunning as e:
        logger.error("%s", e)
        sys.exit(1)

    try:
        from live.keys_loader import load_keys
        keys = load_keys()
        accounts = build_accounts(keys)
        brokers = build_brokers(keys)
        reader = OhlcvReader()
        engine = ExecutorEngine(reader, brokers, accounts)
        reconcile.startup_reconcile(engine)
        logger.info("executor started (ENV=%s, %d accounts)", cfg.ENV, len(accounts))
        while True:
            try:
                engine.tick()
            except Exception:
                logger.exception("tick error")
            time.sleep(cfg.POLL_SECONDS)
    finally:
        lock.release()


if __name__ == "__main__":
    main()
