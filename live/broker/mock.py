"""
Mock broker（测试用，§14 基础设施）—— 实现 Broker 接口，记录调用、可编程成交。

不连任何交易所。测试通过 fill_order(oid) 模拟交易所端某挂单成交（并相应减持仓）。
"""
from __future__ import annotations

from typing import Optional

from live.broker.base import (
    Broker, Side, PosSide, OrderState,
    Fill, Position, OrderStatus, SymbolSpec,
    open_side, close_side,
)


class MockBroker(Broker):
    def __init__(self, label: str = "mock_0", exchange: str = "mock",
                 spec: Optional[SymbolSpec] = None,
                 fill_price: float = 100.0, balance: float = 120.0,
                 fail_on: Optional[set] = None) -> None:
        self.label = label
        self.exchange = exchange
        self.fail_on = fail_on or set()      # 方法名集合 → 注入抛错（故障注入测试）
        self._spec = spec or SymbolSpec("*", 0.001, 0.1, 0.001, 5.0)
        self._fill_price = fill_price
        self._balance = balance
        self.orders: dict[str, OrderStatus] = {}
        self._meta: dict[str, dict] = {}        # oid -> {symbol, pos_side, qty, kind}
        self.positions: dict[tuple, Position] = {}
        self.calls: list = []
        self.leverage_set: dict = {}
        self._oid = 0

    def _next_oid(self) -> str:
        self._oid += 1
        return f"o{self._oid}"

    # ── 账户 / 规格 ──
    def get_available_balance(self) -> float:
        return self._balance

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        s = self._spec
        return SymbolSpec(symbol, s.qty_step, s.price_tick, s.min_qty, s.min_notional)

    def set_leverage(self, symbol: str, pos_side: PosSide, leverage: int) -> None:
        self.leverage_set[(symbol, pos_side)] = leverage
        self.calls.append(("set_leverage", symbol, pos_side, leverage))

    # ── 查询 ──
    def get_position(self, symbol: str, pos_side: PosSide) -> Optional[Position]:
        return self.positions.get((symbol, pos_side))

    def get_order(self, symbol: str, order_id: Optional[str] = None,
                  client_id: Optional[str] = None) -> Optional[OrderStatus]:
        if order_id is not None:
            return self.orders.get(order_id)
        for o in self.orders.values():
            if o.client_id == client_id:
                return o
        return None

    # ── 下单 ──
    def market_open(self, symbol, pos_side, qty, client_id) -> Fill:
        oid = self._next_oid()
        price = self._fill_price
        self.positions[(symbol, pos_side)] = Position(symbol, pos_side, qty, price)
        self.calls.append(("market_open", symbol, pos_side, qty, client_id))
        return Fill(oid, client_id, symbol, pos_side, open_side(pos_side), price, qty)

    def market_close(self, symbol, pos_side, qty, client_id) -> Fill:
        if "market_close" in self.fail_on:
            raise RuntimeError("injected market_close fail")
        self._reduce_position(symbol, pos_side, qty)
        oid = self._next_oid()
        self.calls.append(("market_close", symbol, pos_side, qty, client_id))
        return Fill(oid, client_id, symbol, pos_side, close_side(pos_side), self._fill_price, qty)

    def place_reduce_limit(self, symbol, pos_side, qty, price, client_id) -> str:
        if "place_reduce_limit" in self.fail_on:
            raise RuntimeError("injected place_reduce_limit fail")
        return self._place(symbol, pos_side, qty, "limit", client_id,
                           ("place_reduce_limit", symbol, pos_side, qty, price, client_id))

    def place_stop_market(self, symbol, pos_side, qty, stop_price, client_id) -> str:
        if "place_stop_market" in self.fail_on:
            raise RuntimeError("injected place_stop_market fail")
        return self._place(symbol, pos_side, qty, "stop", client_id,
                           ("place_stop_market", symbol, pos_side, qty, stop_price, client_id))

    def cancel_order(self, symbol, order_id=None, client_id=None) -> None:
        if "cancel_order" in self.fail_on:
            raise RuntimeError("injected cancel_order fail")
        if order_id and order_id in self.orders:
            self.orders[order_id].state = OrderState.CANCELED
        self.calls.append(("cancel_order", symbol, order_id, client_id))

    # ── 内部 ──
    def _place(self, symbol, pos_side, qty, kind, client_id, call) -> str:
        oid = self._next_oid()
        self.orders[oid] = OrderStatus(oid, client_id, OrderState.NEW, 0.0, 0.0)
        self._meta[oid] = {"symbol": symbol, "pos_side": pos_side, "qty": qty, "kind": kind}
        self.calls.append(call + (oid,))
        return oid

    def _reduce_position(self, symbol, pos_side, qty) -> None:
        pos = self.positions.get((symbol, pos_side))
        if pos:
            pos.qty -= qty
            if pos.qty <= 1e-9:
                del self.positions[(symbol, pos_side)]

    # ── 测试辅助：模拟交易所端某挂单成交 ──
    def fill_order(self, oid: str, price: Optional[float] = None) -> None:
        o = self.orders[oid]
        m = self._meta[oid]
        o.state = OrderState.FILLED
        o.filled_qty = m["qty"]
        o.avg_price = price if price is not None else self._fill_price
        self._reduce_position(m["symbol"], m["pos_side"], m["qty"])
