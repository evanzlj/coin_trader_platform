"""
Broker 统一接口（§8.1）+ 通用数据类。

executor 只调本接口；BinanceBroker / OKXBroker 各自实现，抹平两家差异
（数量单位、posSide/side、止损单类型、精度、ENV 切换等，见 §8）。

hedge 模式开 / 平语义（§8.4，不用 reduceOnly）：
  开多 LONG+BUY  ·  平多 LONG+SELL
  开空 SHORT+SELL ·  平空 SHORT+BUY
TP / SL 这类减仓单：与持仓同 pos_side、相反 side（由 adapter 内部按 close_side 推）。

一个 Broker 实例 = 一个账户（label，如 binance_0 / okx_demo）。
"""
from __future__ import annotations

import math
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional


# ── 枚举 ───────────────────────────────────────────────────────────────────────

class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class PosSide(str, Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class OrderState(str, Enum):
    NEW              = "NEW"               # 已挂未成交
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED           = "FILLED"
    CANCELED         = "CANCELED"
    REJECTED         = "REJECTED"
    EXPIRED          = "EXPIRED"
    UNKNOWN          = "UNKNOWN"           # 查单 API 异常，状态未知（不臆测，§19）


# ── 数据类 ─────────────────────────────────────────────────────────────────────

@dataclass
class Fill:
    """市价单成交结果。"""
    order_id: str
    client_id: str
    symbol: str
    pos_side: PosSide
    side: Side
    price: float                  # 实际成交均价
    qty: float                    # 实际成交数量（币）
    raw: Optional[dict] = None    # 原始返回（排错用）


@dataclass
class Position:
    symbol: str
    pos_side: PosSide
    qty: float                    # 持仓数量（币，>0）
    entry_price: float
    raw: Optional[dict] = None


@dataclass
class OrderStatus:
    order_id: str
    client_id: str
    state: OrderState
    filled_qty: float
    avg_price: float
    raw: Optional[dict] = None


@dataclass
class SymbolSpec:
    """合约规格（精度 / 最小量），启动时从交易所拉。币数量口径（OKX 张数已折算）。"""
    symbol: str
    qty_step: float               # 数量步长（币）
    price_tick: float             # 价格步长
    min_qty: float                # 最小下单量（币）
    min_notional: float           # 最小名义（USDT），无则 0


# ── 开 / 平语义 helper（§8.4）─────────────────────────────────────────────────

def open_side(pos_side: PosSide) -> Side:
    """开仓 side：开多→BUY，开空→SELL。"""
    return Side.BUY if pos_side == PosSide.LONG else Side.SELL


def close_side(pos_side: PosSide) -> Side:
    """平 / 减仓 side：平多→SELL，平空→BUY。"""
    return Side.SELL if pos_side == PosSide.LONG else Side.BUY


# ── 步长工具 ───────────────────────────────────────────────────────────────────

def floor_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return math.floor(round(x / step, 9)) * step


def round_to_step(x: float, step: float) -> float:
    if step <= 0:
        return x
    return round(x / step) * step


# ── 接口 ───────────────────────────────────────────────────────────────────────

def safe_get_order(broker: "Broker", symbol: str,
                   order_id: Optional[str] = None,
                   client_id: Optional[str] = None) -> Optional["OrderStatus"]:
    """get_order 的安全封装：任何异常（含 adapter 内部 _inst_meta / public API 抖动）
    归一为 UNKNOWN，**绝不抛**（§22 不变量3：查询异常只能保持态，不打崩流程）。"""
    try:
        return broker.get_order(symbol, order_id=order_id, client_id=client_id)
    except Exception:
        return OrderStatus(order_id or "", client_id or "", OrderState.UNKNOWN, 0.0, 0.0)


class Broker(ABC):
    """统一下单接口。executor 只调这些方法；两家 adapter 各实现。"""

    label: str          # 账户标识，如 binance_0 / okx_demo
    exchange: str       # "binance" | "okx"

    # 账户 / 规格 ----------------------------------------------------------------
    @abstractmethod
    def get_available_balance(self) -> float:
        ...

    @abstractmethod
    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        ...

    @abstractmethod
    def set_leverage(self, symbol: str, pos_side: PosSide, leverage: int) -> None:
        ...

    # 持仓 / 订单查询（对账用）----------------------------------------------------
    @abstractmethod
    def get_position(self, symbol: str, pos_side: PosSide) -> Optional[Position]:
        ...

    @abstractmethod
    def get_order(self, symbol: str, order_id: Optional[str] = None,
                  client_id: Optional[str] = None) -> Optional[OrderStatus]:
        ...

    # 下单 ----------------------------------------------------------------------
    @abstractmethod
    def market_open(self, symbol: str, pos_side: PosSide, qty: float,
                    client_id: str) -> Fill:
        ...

    @abstractmethod
    def market_close(self, symbol: str, pos_side: PosSide, qty: float,
                     client_id: str) -> Fill:
        ...

    @abstractmethod
    def place_reduce_limit(self, symbol: str, pos_side: PosSide, qty: float,
                           price: float, client_id: str) -> str:
        """挂减仓限价单（TP）。side = close_side(pos_side)。返回 order_id。"""
        ...

    @abstractmethod
    def place_stop_market(self, symbol: str, pos_side: PosSide, qty: float,
                          stop_price: float, client_id: str) -> str:
        """挂减仓止损市价单（SL），触发价口径 last（§6.2）。返回 order_id。"""
        ...

    @abstractmethod
    def cancel_order(self, symbol: str, order_id: Optional[str] = None,
                     client_id: Optional[str] = None) -> None:
        ...

    # 精度（通用实现，基于 SymbolSpec）-------------------------------------------
    def round_qty(self, symbol: str, qty: float) -> float:
        return floor_to_step(qty, self.get_symbol_spec(symbol).qty_step)

    def round_price(self, symbol: str, price: float) -> float:
        return round_to_step(price, self.get_symbol_spec(symbol).price_tick)
