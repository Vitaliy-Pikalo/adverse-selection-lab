"""Environment + YAML config loading."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]

KALSHI_HOSTS = {
    "prod": {
        "rest": "https://api.elections.kalshi.com/trade-api/v2",
        "ws": "wss://api.elections.kalshi.com/trade-api/ws/v2",
    },
    "demo": {
        "rest": "https://demo-api.kalshi.co/trade-api/v2",
        "ws": "wss://demo-api.kalshi.co/trade-api/ws/v2",
    },
}
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"
COINBASE_REST = "https://api.exchange.coinbase.com"


@dataclass
class KalshiCreds:
    key_id: str
    private_key_path: str
    env: str = "prod"

    @property
    def rest_url(self) -> str:
        return KALSHI_HOSTS[self.env]["rest"]

    @property
    def ws_url(self) -> str:
        return KALSHI_HOSTS[self.env]["ws"]


def load_creds(env_file: str | Path | None = None) -> KalshiCreds:
    load_dotenv(env_file or REPO_ROOT / ".env")
    env = os.getenv("KALSHI_ENV", "prod").strip() or "prod"
    if env not in KALSHI_HOSTS:
        raise ValueError(f"KALSHI_ENV must be one of {list(KALSHI_HOSTS)}, got {env!r}")
    return KalshiCreds(
        key_id=os.getenv("KALSHI_KEY_ID", "").strip(),
        private_key_path=os.getenv("KALSHI_PRIVATE_KEY_PATH", "").strip(),
        env=env,
    )


@dataclass
class CollectorConfig:
    series: list[str] = field(default_factory=lambda: ["KXBTC", "KXETH"])
    max_hours_to_close: float = 6
    max_markets: int = 400
    refresh_seconds: int = 120
    priority_series: list[str] = field(default_factory=list)
    min_mid: float = 0.02
    max_mid: float = 0.98
    channels: list[str] = field(default_factory=lambda: ["orderbook_delta", "trade"])
    coinbase_products: list[str] = field(default_factory=lambda: ["BTC-USD", "ETH-USD"])
    coinbase_channels: list[str] = field(default_factory=lambda: ["ticker"])
    out_dir: Path = REPO_ROOT / "data" / "raw"
    flush_seconds: float = 2.0


def load_collector_config(path: str | Path) -> CollectorConfig:
    raw = yaml.safe_load(Path(path).read_text())
    k, cb, out = raw.get("kalshi", {}), raw.get("coinbase", {}), raw.get("output", {})
    out_dir = Path(out.get("dir", "data/raw"))
    if not out_dir.is_absolute():
        out_dir = REPO_ROOT / out_dir
    d = CollectorConfig()
    return CollectorConfig(
        series=k.get("series", d.series),
        max_hours_to_close=k.get("max_hours_to_close", d.max_hours_to_close),
        max_markets=k.get("max_markets", d.max_markets),
        refresh_seconds=k.get("refresh_seconds", d.refresh_seconds),
        priority_series=k.get("priority_series", d.priority_series),
        min_mid=k.get("min_mid", d.min_mid),
        max_mid=k.get("max_mid", d.max_mid),
        channels=k.get("channels", d.channels),
        coinbase_products=cb.get("products", d.coinbase_products),
        coinbase_channels=cb.get("channels", d.coinbase_channels),
        out_dir=out_dir,
        flush_seconds=out.get("flush_seconds", d.flush_seconds),
    )
