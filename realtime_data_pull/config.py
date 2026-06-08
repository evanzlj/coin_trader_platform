SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]

SYMBOL_DISPLAY = {
    "BTCUSDT": "BTC/USDT",
    "ETHUSDT": "ETH/USDT",
    "BNBUSDT": "BNB/USDT",
    "SOLUSDT": "SOL/USDT",
}

# slug used in historical CSV filenames
SYMBOL_SLUG = {
    "BTC/USDT": "btcusdt",
    "ETH/USDT": "ethusdt",
    "BNB/USDT": "bnbusdt",
    "SOL/USDT": "solusdt",
}

BINANCE_FUTURES_WS   = "wss://fstream.binance.com/stream"
BINANCE_FUTURES_REST = "https://fapi.binance.com"

# kline timeframes to subscribe (weekly via 1w stream)
KLINE_TIMEFRAMES = ["15m", "4h", "1w"]

# how many closed bars to pre-fetch via REST on startup
WARMUP_BARS = {
    "15m": 250,   # 192 for QRC, 20 for MA, 14 for ATR
    "4h":  60,    # 20 for structure lookback
    "1w":  60,    # 50 for weekly MA50
}
