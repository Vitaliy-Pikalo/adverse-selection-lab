# Adverse Selection Lab

Research question: on Kalshi BTC/ETH binary markets, is maker adverse selection driven mainly
by quotes going stale relative to the underlying spot price, and does a quoting policy that
reacts to that risk beat static and inventory-aware quoting under conservative fill assumptions?

Status: **Milestone 1 — data infrastructure.** No results yet. See `PROJECT_STATUS.md`.

This project is research and paper simulation only. The code has no order-placement path.

## Setup (Windows PowerShell)

```powershell
cd adverse-selection-lab
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
copy .env.example .env      # then fill in KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH
pytest -q                   # should print: 29 passed
```

## Commands

| Task | Command |
|---|---|
| 10-minute collector smoke test | `python -m asl.recorder.run --duration 600` |
| Run collector continuously | `python -m asl.recorder.run` |
| Rebuild books + quality report | `python -m asl.replay` |
| Quick backfill check | `python -m asl.backfill --series KXBTC --days 2 --max-markets 50` |
| Full backfill (30 days) | `python -m asl.backfill --series KXBTC KXETH --days 30` |

## Layout

```
src/asl/
  fixed.py            exact integer parsing of Kalshi price/size strings
  book.py             L2 book (bids-only YES/NO legs), per-subscription seq-gap detection
  kalshi/auth.py      RSA-PSS request signing
  kalshi/rest.py      read-only REST client (no write methods by design)
  recorder/           WebSocket recorders (Kalshi orderbook_delta + trade, Coinbase ticker)
  replay.py           raw logs -> top-of-book / trades / spot parquet + quality report
  backfill.py         settled markets, public trades, Coinbase 1-min candles
configs/collector.yaml
data/                 raw + derived data (gitignored, reproducible)
```

## Data lineage

Raw WebSocket messages are logged before any processing, with local receive timestamps.
Every derived table is rebuilt from raw logs by `asl.replay`, so a bug in book logic can be
fixed and replayed without recollecting.
