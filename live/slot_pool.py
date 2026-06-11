"""
Slot 分配器（§9）。

10 账户 × 每账户最多 2 笔 = 20 slot 上限。slot 在「激活进场」时占用，监控阶段不占。
**不维护独立 slot 表**：占用从所有 signal_active 的 state.json 重建（崩溃重启可恢复）。

分配约束（满足全部才可入，满足者中**纯随机**选）：
  max  每账户 ≤ MAX_PER_ACCOUNT 笔
  C1   账户剩余 margin ≥ 本笔 margin
  C2   账户内无相同 symbol（避免 hedge 合并 + Binance symbol 级杠杆冲突）
  C4   OKX 该 symbol 取整偏差超阈值 → 排除该所账户（通过 c4_ok 注入，adapter 接入后填）
无可用账户 → None（skipped_no_slot）。
"""
from __future__ import annotations

import random
from dataclasses import dataclass
from typing import Callable, Optional

from live import exec_config as cfg

ACTIVE_STATUS = {"ACTIVATED", "TP1_HIT"}


@dataclass
class Account:
    label: str
    exchange: str       # "binance" | "okx"
    capital: float


def build_accounts(keys: dict) -> list[Account]:
    """从 keys（keys_loader.load_keys 的结果）构建账户列表。"""
    accounts: list[Account] = []
    for ex in ("binance", "okx"):
        for a in keys.get(ex, []):
            accounts.append(Account(a["label"], ex, cfg.CAPITAL_PER_ACCOUNT))
    return accounts


@dataclass
class Occupancy:
    """account_label -> list[(symbol, margin)]，从 state.json 重建。"""
    by_account: dict

    def used_margin(self, label: str) -> float:
        return sum(m for _, m in self.by_account.get(label, []))

    def symbols(self, label: str) -> set:
        return {s for s, _ in self.by_account.get(label, [])}

    def count(self, label: str) -> int:
        return len(self.by_account.get(label, []))

    def total(self) -> int:
        return sum(len(v) for v in self.by_account.values())


def build_occupancy(states: list[dict]) -> Occupancy:
    """从 signal_active 的 state.json 列表重建占用（仅 ACTIVATED/TP1_HIT 的 pb）。"""
    by: dict = {}
    for st in states:
        symbol = st.get("symbol")
        for pb in st.get("playbooks", []):
            if pb.get("status") in ACTIVE_STATUS:
                ex = pb.get("exec") or {}
                acc = ex.get("account")
                margin = ex.get("margin")
                if acc and margin is not None:
                    by.setdefault(acc, []).append((symbol, float(margin)))
    return Occupancy(by_account=by)


def allocate(
    accounts: list[Account],
    symbol: str,
    margin: float,
    occupancy: Occupancy,
    c4_ok: Optional[Callable[[Account, str], bool]] = None,
    rng: Optional[random.Random] = None,
) -> Optional[str]:
    """从满足约束的账户中纯随机选一个，返回 label；无可用 → None。"""
    rng = rng or random
    cands = list(accounts)
    rng.shuffle(cands)
    for a in cands:
        if occupancy.count(a.label) >= cfg.MAX_PER_ACCOUNT:        # max
            continue
        if occupancy.used_margin(a.label) + margin > a.capital:    # C1
            continue
        if symbol in occupancy.symbols(a.label):                   # C2
            continue
        if c4_ok is not None and not c4_ok(a, symbol):             # C4
            continue
        return a.label
    return None
