"""OKXBroker.get_position 尘埃残仓过滤回归测试（DUST_NOTIONAL_USD）。

背景：OKX 对冲净仓多次进出后会留 sub-$ 舍入残渣（如 0.01 张，其量 == minSz，size 阈值区分不了），
    导致 position_manager 的 SL/BE 终态判定（get_position() is None）被永久卡住 → state 冻结 +
    dashboard 幽灵浮亏。修复：notional < DUST_NOTIONAL_USD 的持仓视作 flat（返回 None）。
"""
import unittest

from live.broker.base import PosSide, Position

try:                                          # okx SDK 只在部署机(btc-ml)装了；Mac 上缺则跳过
    from live.broker.okx import OKXBroker, DUST_NOTIONAL_USD
    _OKX_OK = True
except ModuleNotFoundError:
    _OKX_OK = False
    DUST_NOTIONAL_USD = 5.0                    # 占位，跳过时用不到


class _FakeAccount:
    """account.get_positions 桩：返回预置的 OKX positions 响应。"""
    def __init__(self, rows):
        self._rows = rows

    def get_positions(self, **kw):
        return {"code": "0", "data": self._rows}


def _make_broker(rows):
    b = OKXBroker.__new__(OKXBroker)          # 绕过 __init__（不建真 API 连接）
    b.account = _FakeAccount(rows)
    b._inst_cache = {"SOL/USDT": {"ctVal": 1.0, "lotSz": 0.01, "minSz": 0.01, "tickSz": 0.01}}
    return b


def _row(pos, notional=None, markPx="76.8", avgPx="78.89", posSide="long"):
    r = {"posSide": posSide, "pos": str(pos), "avgPx": avgPx, "markPx": markPx, "last": markPx}
    if notional is not None:
        r["notionalUsd"] = str(notional)
    return r


@unittest.skipUnless(_OKX_OK, "okx SDK not installed (run on btc-ml)")
class TestOKXDustFilter(unittest.TestCase):
    def test_dust_residual_treated_as_flat(self):
        # 0.01 张 × ctVal 1 × 76.8 ≈ $0.77 notional → 视作无仓
        b = _make_broker([_row(0.01, notional="0.7675")])
        self.assertIsNone(b.get_position("SOL/USDT", PosSide.LONG))

    def test_real_position_returned(self):
        # 真实半仓：notional 远超阈值 → 正常返回
        b = _make_broker([_row(3.85, notional="295.7")])
        pos = b.get_position("SOL/USDT", PosSide.LONG)
        self.assertIsInstance(pos, Position)
        self.assertAlmostEqual(pos.qty, 3.85)

    def test_no_notional_falls_back_to_computed(self):
        # OKX 未给 notionalUsd → 用 pos×ctVal×markPx 兜底，dust 仍被过滤
        b = _make_broker([_row(0.01, notional=None, markPx="76.8")])
        self.assertIsNone(b.get_position("SOL/USDT", PosSide.LONG))
        # 兜底下的真实仓仍返回
        b2 = _make_broker([_row(3.85, notional=None, markPx="76.8")])
        self.assertIsNotNone(b2.get_position("SOL/USDT", PosSide.LONG))

    def test_zero_position_is_none(self):
        b = _make_broker([_row(0.0, notional="0")])
        self.assertIsNone(b.get_position("SOL/USDT", PosSide.LONG))

    def test_threshold_boundary(self):
        # 恰好低于阈值 → flat；恰好高于 → 保留
        b_lo = _make_broker([_row(0.1, notional=str(DUST_NOTIONAL_USD - 0.01))])
        self.assertIsNone(b_lo.get_position("SOL/USDT", PosSide.LONG))
        b_hi = _make_broker([_row(0.1, notional=str(DUST_NOTIONAL_USD + 0.01))])
        self.assertIsNotNone(b_hi.get_position("SOL/USDT", PosSide.LONG))

    def test_posside_filter(self):
        # 只匹配请求的 posSide
        b = _make_broker([_row(3.85, notional="295.7", posSide="short")])
        self.assertIsNone(b.get_position("SOL/USDT", PosSide.LONG))


if __name__ == "__main__":
    unittest.main()
