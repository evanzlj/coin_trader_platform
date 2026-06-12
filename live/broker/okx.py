"""
OKX Perp Swap adapter（#5），实现 Broker 接口。

SDK: python-okx 0.4.1（Trade/Account/PublicData API）。demo 走 flag="1"（§19）。
long/short 模式（§8.4）：posSide=long/short + 反向 side，**不传 reduceOnly**。
触发价口径 last（§6.2）：algo slTriggerPxType="last"，slOrdPx="-1"（市价）。

与 Binance 的差异（adapter 内部抹平）：
- 下单单位是**合约张数 sz**，sz × ctVal = 币量；接口对外仍用币量（§8.3）。
- SL 是 **algo（conditional）单**，与普通 order 查询/撤销接口不同 → order_id 加前缀
  "ord:" / "algo:" 区分（executor 透明传递）。
- clOrdId 只允许字母数字 → 去掉 client_id 里的下划线。
"""
from __future__ import annotations

import math
from typing import Optional

from okx.Trade import TradeAPI
from okx.Account import AccountAPI
from okx.PublicData import PublicAPI

from live.broker.base import (
    Broker, Side, PosSide, OrderState,
    Fill, Position, OrderStatus, SymbolSpec,
    open_side, close_side,
)

_ORD_STATE = {
    "live": OrderState.NEW,
    "partially_filled": OrderState.PARTIALLY_FILLED,
    "filled": OrderState.FILLED,
    "canceled": OrderState.CANCELED,
    "mmp_canceled": OrderState.CANCELED,
}
_ALGO_STATE = {
    "live": OrderState.NEW,
    "effective": OrderState.FILLED,        # 触发并成交
    "canceled": OrderState.CANCELED,
    "order_failed": OrderState.REJECTED,
}


def _inst(symbol: str) -> str:
    return symbol.replace("/", "-") + "-SWAP"     # BTC/USDT -> BTC-USDT-SWAP


def _cl(client_id: str) -> str:
    return client_id.replace("_", "")             # OKX clOrdId 仅字母数字


def _fmt(x: float) -> str:
    s = f"{x:.10f}".rstrip("0").rstrip(".")
    return s if s else "0"


# 撤单返回里「订单已不存在/已撤/已完成」= 等价撤成功（已无活跃单，drain 视为干净）。
_CANCEL_GONE = {"51400", "51401", "51402", "51410", "51503"}


def _check_cancel(resp: dict) -> None:
    """严格校验 OKX 撤单返回（§22.5：_drain 依赖 cancel 成功 = 交易所确认撤单）。
    有逐条 data → 看每条 sCode（外层 code 是批量级，单条失败就非0，不能据此判定）；
    sCode 非0 且非「订单已不存在」(51400 等) → 抛。无 data → 看请求级 code。"""
    data = resp.get("data") or []
    if data:
        for d in data:
            scode = str(d.get("sCode"))
            if scode in ("0", "None", "") or scode in _CANCEL_GONE:
                continue
            raise RuntimeError(f"okx cancel rejected {scode}: {d.get('sMsg')}")
        return
    if str(resp.get("code")) not in ("0", "None", ""):
        raise RuntimeError(f"okx cancel error {resp.get('code')}: {resp.get('msg')}")


class OKXBroker(Broker):
    def __init__(self, label: str, api_key: str, secret: str, passphrase: str,
                 flag: str) -> None:
        self.label = label
        self.exchange = "okx"
        self.trade = TradeAPI(api_key, secret, passphrase, flag=flag, debug=False)
        self.account = AccountAPI(api_key, secret, passphrase, flag=flag, debug=False)
        self.public = PublicAPI(api_key, secret, passphrase, flag=flag, debug=False)
        self._inst_cache: dict[str, dict] = {}     # symbol -> {ctVal,lotSz,minSz,tickSz}

    # ── 工具 ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _data(resp: dict) -> list:
        if str(resp.get("code")) != "0":
            raise RuntimeError(f"okx error {resp.get('code')}: {resp.get('msg')} {resp.get('data')}")
        return resp.get("data", [])

    def _inst_meta(self, symbol: str) -> dict:
        if symbol not in self._inst_cache:
            d = self._data(self.public.get_instruments(instType="SWAP", instId=_inst(symbol)))[0]
            self._inst_cache[symbol] = {
                "ctVal": float(d["ctVal"]), "lotSz": float(d["lotSz"]),
                "minSz": float(d["minSz"]), "tickSz": float(d["tickSz"]),
            }
        return self._inst_cache[symbol]

    def _qty_to_sz(self, symbol: str, qty_coin: float) -> str:
        m = self._inst_meta(symbol)
        contracts = qty_coin / m["ctVal"]
        sz = math.floor(contracts / m["lotSz"]) * m["lotSz"]
        return _fmt(sz)

    # ── 账户 / 规格 ────────────────────────────────────────────────────────────

    def get_available_balance(self) -> float:
        d = self._data(self.account.get_account_balance(ccy="USDT"))
        if not d:
            return 0.0
        for det in d[0].get("details", []):
            if det.get("ccy") == "USDT":
                return float(det.get("availBal") or 0)
        return 0.0

    def get_symbol_spec(self, symbol: str) -> SymbolSpec:
        m = self._inst_meta(symbol)
        return SymbolSpec(
            symbol=symbol,
            qty_step=m["lotSz"] * m["ctVal"],     # 币量步长
            price_tick=m["tickSz"],
            min_qty=m["minSz"] * m["ctVal"],
            min_notional=0.0,
        )

    def set_leverage(self, symbol: str, pos_side: PosSide, leverage: int) -> None:
        self.account.set_leverage(instId=_inst(symbol), lever=str(int(leverage)),
                                  mgnMode="isolated", posSide=pos_side.value.lower())

    # ── 查询 ──────────────────────────────────────────────────────────────────

    def get_position(self, symbol: str, pos_side: PosSide) -> Optional[Position]:
        m = self._inst_meta(symbol)
        for d in self._data(self.account.get_positions(instType="SWAP", instId=_inst(symbol))):
            if d.get("posSide") == pos_side.value.lower():
                pos = float(d.get("pos") or 0)
                if abs(pos) > 1e-12:
                    return Position(symbol, pos_side, abs(pos) * m["ctVal"],
                                    float(d.get("avgPx") or 0), raw=d)
        return None

    def get_order(self, symbol: str, order_id: Optional[str] = None,
                  client_id: Optional[str] = None) -> Optional[OrderStatus]:
        m = None
        try:
            inst = _inst(symbol)
            m = self._inst_meta(symbol)            # 纳入 try：public instruments 抖动 → UNKNOWN，不外抛（§22）
            if order_id and order_id.startswith("algo:"):
                oid = order_id.split(":", 1)[1]
                d = self._data(self.trade.get_algo_order_details(algoId=oid))[0]
                state = _ALGO_STATE.get(d.get("state"), OrderState.NEW)
                filled = float(d.get("actualSz") or 0) * m["ctVal"]
                return OrderStatus(order_id, d.get("algoClOrdId", ""), state, filled,
                                   float(d.get("actualPx") or 0), raw=d)
            else:
                oid = order_id.split(":", 1)[1] if order_id and ":" in order_id else order_id
                kw = {"instId": inst, "ordId": oid} if oid else {"instId": inst, "clOrdId": _cl(client_id)}
                d = self._data(self.trade.get_order(**kw))[0]
                state = _ORD_STATE.get(d.get("state"), OrderState.NEW)
                return OrderStatus("ord:" + d["ordId"], d.get("clOrdId", ""), state,
                                   float(d.get("accFillSz") or 0) * m["ctVal"],
                                   float(d.get("avgPx") or 0), raw=d)
        except Exception as e:
            msg = str(e).lower()
            if "not exist" in msg or "51603" in msg or "51000" in msg:
                if order_id is None and client_id:
                    # 普通单不存在 → 查 algo（SL 是 algo，by algoClOrdId，供 adopt 先查再挂 §18）
                    try:
                        d = self._data(self.trade.get_algo_order_details(algoClOrdId=_cl(client_id)))
                        if d:
                            a = d[0]
                            return OrderStatus("algo:" + a["algoId"], client_id,
                                               _ALGO_STATE.get(a.get("state"), OrderState.NEW),
                                               float(a.get("actualSz") or 0) * m["ctVal"],
                                               float(a.get("actualPx") or 0), raw=a)
                    except Exception:
                        pass
                return None                            # 订单确实不存在
            return OrderStatus(order_id or "", client_id or "", OrderState.UNKNOWN, 0.0, 0.0)

    # ── 下单 ──────────────────────────────────────────────────────────────────

    def _place(self, symbol, pos_side, side, ord_type, qty, client_id, px=None) -> dict:
        params = dict(instId=_inst(symbol), tdMode="isolated", side=side.value.lower(),
                      posSide=pos_side.value.lower(), ordType=ord_type,
                      sz=self._qty_to_sz(symbol, qty), clOrdId=_cl(client_id))
        if px is not None:
            params["px"] = _fmt(px)
        d = self._data(self.trade.place_order(**params))[0]
        if str(d.get("sCode")) not in ("0", "None", ""):
            raise RuntimeError(f"okx order rejected {d.get('sCode')}: {d.get('sMsg')}")
        return d

    def market_open(self, symbol, pos_side, qty, client_id) -> Fill:
        d = self._place(symbol, pos_side, open_side(pos_side), "market", qty, client_id)
        od = self.get_order(symbol, "ord:" + d["ordId"])
        price = od.avg_price if od else 0.0
        filled = od.filled_qty if od and od.filled_qty else qty
        return Fill("ord:" + d["ordId"], client_id, symbol, pos_side, open_side(pos_side),
                    price, filled, raw=d)

    def market_close(self, symbol, pos_side, qty, client_id) -> Fill:
        d = self._place(symbol, pos_side, close_side(pos_side), "market", qty, client_id)
        return Fill("ord:" + d["ordId"], client_id, symbol, pos_side, close_side(pos_side),
                    0.0, qty, raw=d)

    def place_reduce_limit(self, symbol, pos_side, qty, price, client_id) -> str:
        d = self._place(symbol, pos_side, close_side(pos_side), "limit", qty, client_id, px=price)
        return "ord:" + d["ordId"]

    def place_stop_market(self, symbol, pos_side, qty, stop_price, client_id) -> str:
        resp = self.trade.place_algo_order(
            instId=_inst(symbol), tdMode="isolated", side=close_side(pos_side).value.lower(),
            posSide=pos_side.value.lower(), ordType="conditional",
            sz=self._qty_to_sz(symbol, qty),
            slTriggerPx=_fmt(stop_price), slOrdPx="-1", slTriggerPxType="last",
            algoClOrdId=_cl(client_id),
        )
        d = self._data(resp)[0]
        if str(d.get("sCode")) not in ("0", "None", ""):
            raise RuntimeError(f"okx algo rejected {d.get('sCode')}: {d.get('sMsg')}")
        return "algo:" + d["algoId"]

    def cancel_order(self, symbol, order_id=None, client_id=None) -> None:
        inst = _inst(symbol)
        if order_id and order_id.startswith("algo:"):
            resp = self.trade.cancel_algo_order([{"instId": inst, "algoId": order_id.split(":", 1)[1]}])
        else:
            oid = order_id.split(":", 1)[1] if order_id and ":" in order_id else order_id
            if oid:
                resp = self.trade.cancel_order(instId=inst, ordId=oid)
            else:
                resp = self.trade.cancel_order(instId=inst, clOrdId=_cl(client_id))
        _check_cancel(resp)                             # 严格校验：业务失败 → 抛，让 drain 知道没撤成功（§22.5）
