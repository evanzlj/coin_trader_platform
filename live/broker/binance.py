"""
Binance USDⓈ-M adapter（#4），实现 Broker 接口。

SDK: binance-futures-connector（UMFutures）。ENV 切换走 base_url（testnet/live，§19）。
hedge 模式（§8.4）：positionSide=LONG/SHORT + 反向 side，**不传 reduceOnly**。
触发价口径 last（§6.2）：STOP_MARKET workingType=CONTRACT_PRICE。
一个实例 = 一个账户。
"""
from __future__ import annotations

import time
from typing import Optional

from binance.um_futures import UMFutures

from live.broker.base import (
    Broker, Side, PosSide, OrderState,
    Fill, Position, OrderStatus, SymbolSpec,
    open_side, close_side,
)

_STATE = {
    "NEW": OrderState.NEW,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "FILLED": OrderState.FILLED,
    "CANCELED": OrderState.CANCELED,
    "REJECTED": OrderState.REJECTED,
    "EXPIRED": OrderState.EXPIRED,
    "EXPIRED_IN_MATCH": OrderState.EXPIRED,
}

# Algo（条件单）状态 → 统一态。字段/取值待 testnet 实测确认后微调。
_ALGO_STATE = {
    "NEW": OrderState.NEW, "WORKING": OrderState.NEW, "ACTIVE": OrderState.NEW,
    "TRIGGERED": OrderState.FILLED, "FINISHED": OrderState.FILLED, "FILLED": OrderState.FILLED,
    "CANCELLED": OrderState.CANCELED, "CANCELED": OrderState.CANCELED,
    "EXPIRED": OrderState.EXPIRED, "REJECTED": OrderState.REJECTED,
}


def _sym(symbol: str) -> str:
    return symbol.replace("/", "")


class BinanceBroker(Broker):
    def __init__(self, label: str, api_key: str, secret: str, base_url: str) -> None:
        self.label = label
        self.exchange = "binance"
        self.client = UMFutures(key=api_key, secret=secret, base_url=base_url)
        self._spec_cache: dict[str, SymbolSpec] = {}

    # ── 账户 / 规格 ────────────────────────────────────────────────────────────

    def get_available_balance(self) -> float:
        for a in self.client.balance():
            if a.get("asset") == "USDT":
                return float(a.get("availableBalance") or 0)
        return 0.0

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        if symbol in self._spec_cache:
            return self._spec_cache[symbol]
        info = self.client.exchange_info()
        target = _sym(symbol)
        for s in info["symbols"]:
            if s["symbol"] == target:
                f = {x["filterType"]: x for x in s["filters"]}
                spec = SymbolSpec(
                    symbol=symbol,
                    qty_step=float(f["LOT_SIZE"]["stepSize"]),
                    price_tick=float(f["PRICE_FILTER"]["tickSize"]),
                    min_qty=float(f["LOT_SIZE"]["minQty"]),
                    min_notional=float(f.get("MIN_NOTIONAL", {}).get("notional", 0) or 0),
                )
                self._spec_cache[symbol] = spec
                return spec
        raise ValueError(f"symbol not found in exchange_info: {symbol}")

    def set_leverage(self, symbol: str, pos_side: PosSide, leverage: int) -> None:
        sym = _sym(symbol)
        try:
            self.client.change_margin_type(symbol=sym, marginType="ISOLATED")
        except Exception:
            pass  # 已是 ISOLATED 会报 -4046，忽略
        self.client.change_leverage(symbol=sym, leverage=int(leverage))

    # ── 查询 ──────────────────────────────────────────────────────────────────

    def get_position(self, symbol: str, pos_side: PosSide) -> Optional[Position]:
        risks = self.client.get_position_risk(symbol=_sym(symbol))
        for r in risks:
            if r.get("positionSide") == pos_side.value:
                amt = float(r.get("positionAmt") or 0)
                if abs(amt) > 1e-12:
                    return Position(symbol, pos_side, abs(amt),
                                    float(r.get("entryPrice") or 0), raw=r)
        return None

    def get_order(self, symbol: str, order_id: Optional[str] = None,
                  client_id: Optional[str] = None) -> Optional[OrderStatus]:
        sym = _sym(symbol)
        try:
            if order_id and order_id.startswith("algo:"):
                od = self.client.sign_request("GET", "/fapi/v1/algoOrder",
                                              {"symbol": sym, "algoId": int(order_id.split(":", 1)[1])})
                st = od.get("algoStatus") or od.get("status")
                return OrderStatus(order_id, od.get("clientAlgoId", ""),
                                   _ALGO_STATE.get(st, OrderState.NEW),
                                   float(od.get("executedQty") or 0),
                                   float(od.get("avgPrice") or 0), raw=od)
            if order_id is not None:
                oid = order_id.split(":", 1)[1] if order_id.startswith("ord:") else order_id
                od = self.client.query_order(symbol=sym, orderId=int(oid))
            else:
                od = self.client.query_order(symbol=sym, origClientOrderId=client_id)
        except Exception:
            return None
        return OrderStatus(
            str(od["orderId"]), od.get("clientOrderId", ""),
            _STATE.get(od.get("status"), OrderState.NEW),
            float(od.get("executedQty") or 0), float(od.get("avgPrice") or 0), raw=od,
        )

    # ── 下单 ──────────────────────────────────────────────────────────────────

    def _fill_px(self, sym: str, resp: dict, qty: float) -> tuple[float, float]:
        """从 RESULT 响应取成交价/量；缺则短延迟后 query 兜底（避开下单后异步落库）。"""
        price = float(resp.get("avgPrice") or 0)
        filled = float(resp.get("executedQty") or 0)
        if price == 0:
            time.sleep(0.3)
            try:
                od = self.client.query_order(symbol=sym, orderId=resp["orderId"])
                price = float(od.get("avgPrice") or 0)
                filled = float(od.get("executedQty") or filled)
            except Exception:
                pass
        return price, (filled or qty)

    def market_open(self, symbol: str, pos_side: PosSide, qty: float, client_id: str) -> Fill:
        sym = _sym(symbol)
        side = open_side(pos_side)
        resp = self.client.new_order(
            symbol=sym, side=side.value, type="MARKET", quantity=qty,
            positionSide=pos_side.value, newClientOrderId=client_id,
            newOrderRespType="RESULT",
        )
        price, filled = self._fill_px(sym, resp, qty)
        return Fill(str(resp["orderId"]), client_id, symbol, pos_side, side, price, filled, raw=resp)

    def market_close(self, symbol: str, pos_side: PosSide, qty: float, client_id: str) -> Fill:
        sym = _sym(symbol)
        side = close_side(pos_side)
        resp = self.client.new_order(
            symbol=sym, side=side.value, type="MARKET", quantity=qty,
            positionSide=pos_side.value, newClientOrderId=client_id,
            newOrderRespType="RESULT",
        )
        price, filled = self._fill_px(sym, resp, qty)
        return Fill(str(resp["orderId"]), client_id, symbol, pos_side, side, price, filled, raw=resp)

    def place_reduce_limit(self, symbol: str, pos_side: PosSide, qty: float,
                           price: float, client_id: str) -> str:
        resp = self.client.new_order(
            symbol=_sym(symbol), side=close_side(pos_side).value, type="LIMIT",
            quantity=qty, price=price, timeInForce="GTC",
            positionSide=pos_side.value, newClientOrderId=client_id,
        )
        return str(resp["orderId"])

    def place_stop_market(self, symbol: str, pos_side: PosSide, qty: float,
                          stop_price: float, client_id: str) -> str:
        # 条件单走 Algo Service（/fapi/v1/algoOrder，2025-12 迁移）。返回带 algo: 前缀。
        resp = self.client.sign_request("POST", "/fapi/v1/algoOrder", {
            "algoType": "CONDITIONAL", "symbol": _sym(symbol),
            "side": close_side(pos_side).value, "positionSide": pos_side.value,
            "type": "STOP_MARKET", "quantity": qty, "triggerPrice": stop_price,
            "workingType": "CONTRACT_PRICE", "clientAlgoId": client_id,
        })
        return "algo:" + str(resp["algoId"])

    def cancel_order(self, symbol: str, order_id: Optional[str] = None,
                     client_id: Optional[str] = None) -> None:
        sym = _sym(symbol)
        if order_id and order_id.startswith("algo:"):
            self.client.sign_request("DELETE", "/fapi/v1/algoOrder",
                                     {"symbol": sym, "algoId": int(order_id.split(":", 1)[1])})
        elif order_id is not None:
            oid = order_id.split(":", 1)[1] if order_id.startswith("ord:") else order_id
            self.client.cancel_order(symbol=sym, orderId=int(oid))
        else:
            self.client.cancel_order(symbol=sym, origClientOrderId=client_id)
