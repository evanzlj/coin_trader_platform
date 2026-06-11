"""
加载 API key（§12.1）。

按 ENV 读 live/keys_{env}.json（gitignored），校验结构。代码里不出现 key。
  - testnet：各 1 个（binance_tn / okx_demo）
  - live：5 + 5
"""
from __future__ import annotations

import json
from pathlib import Path

from live import exec_config as cfg


def load_keys(env: str | None = None) -> dict:
    """Return {"binance": [...], "okx": [...]}，每个 account 含 label/api_key/secret(/passphrase)。"""
    env = env or cfg.ENV
    path = cfg.ROOT / "live" / f"keys_{env}.json"
    if not path.exists():
        raise FileNotFoundError(
            f"keys file not found: {path}\n"
            f"  → 复制 live/keys_{env}.json.example 为 live/keys_{env}.json 并填入"
        )
    data = json.loads(path.read_text(encoding="utf-8"))
    _validate(data, path)
    return data


def _validate(data: dict, path: Path) -> None:
    for ex in ("binance", "okx"):
        accs = data.get(ex)
        if not isinstance(accs, list) or not accs:
            raise ValueError(f"{path}: '{ex}' 缺失或为空")
        for acc in accs:
            for field in ("label", "api_key", "secret"):
                if not acc.get(field):
                    raise ValueError(f"{path}: {ex} 账户 {acc.get('label', '?')} 缺 '{field}'")
            if ex == "okx" and not acc.get("passphrase"):
                raise ValueError(f"{path}: okx 账户 {acc.get('label')} 缺 'passphrase'")
