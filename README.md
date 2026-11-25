# Adverse Selection Lab

## Interactive execution replay

[Download the BTC replay](https://raw.githubusercontent.com/Vitaliy-Pikalo/adverse-selection-lab/main/examples/btc-real-replay.html) and open the saved HTML in a browser. Choose a submission time or select **First fill** to see the displayed queue, modeled execution and midpoint on the same clock.

On October 25, 2026, 13,999 overlapping paper orders across 92 BTC markets produced a fill on 53.4% of resting orders. At the 60-second deadline, mean value per submitted contract was −0.64¢ for an immediate buy and −0.15¢ for joining the bid. A resting order earned zero on unfilled quantity. Four markets in the recorder incident hour were excluded. These one-day figures exclude fees and latency.

The interactive page shows the noon Eastern market. The simulation credits only trades at the original bid that consume the displayed queue. [Replay builder](examples/build_btc_replay.py) · [Kalshi Eye source data](https://huggingface.co/datasets/KalshiEye/kalshi-order-book-sample)

Research question: on Kalshi BTC/ETH binary markets, is maker adverse selection driven mainly
by quotes going stale relative to the underlying spot price, and does a quoting policy that
reacts to that risk beat static and inventory-aware quoting under conservative fill assumptions?

Status: **Milestone 2 — markout labels done.** First result (EXPERIMENT_LOG A0): on 1M KXBTC trades, makers fill 2.55¢ better than a log-normal model, then the value moves 1.9–2.9¢ against them; net −0.73¢ per contract at settlement (95% CI −1.39 to −0.07). See `PROJECT_STATUS.md`.

This project is research and paper simulation only. The code has no order-placement path.

## Setup (Windows PowerShell)

```powershell
cd adverse-selection-lab
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[dev]"
copy .env.example .env      # then fill in KALSHI_KEY_ID and KALSHI_PRIVATE_KEY_PATH
pytest -q                   # should print: 59 passed
```

## 24/7 collector on Linux (systemd)

```bash
git clone https://github.com/Vitaliy-Pikalo/adverse-selection-lab.git
cd adverse-selection-lab
nano .env                              # KALSHI_KEY_ID, KALSHI_PRIVATE_KEY_PATH, KALSHI_ENV=prod
bash deploy/setup_collector.sh         # venv, tests, systemd service, no-sleep, NTP
sudo systemctl start asl-recorder
bash deploy/check.sh                   # health: service, file freshness, disk, clock
bash deploy/update.sh                  # pull new code, test, restart
```

## Commands

| Task | Command |
|---|---|
| 10-minute collector smoke test | `python -m asl.recorder.run --duration 600` |
| Run collector continuously | `python -m asl.recorder.run` |
| Rebuild books + quality report | `python -m asl.replay` |
| Quick backfill check | `python -m asl.backfill --series KXBTC --days 2 --max-markets 50` |
| Full backfill (30 days) | `python -m asl.backfill --series KXBTC KXETH --days 30` |
| Markout labels + summary (M2) | `python -m asl.markouts --series KXBTC KXETH` |

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
  pricer.py           log-normal binary pricer (greater / less / between strikes)
  markouts.py         M2: maker markout labels, stale-move feature, event-block bootstrap
configs/collector.yaml
data/                 raw + derived data (gitignored, reproducible)
```

## Data lineage

Raw WebSocket messages are logged before any processing, with local receive timestamps.
Every derived table is rebuilt from raw logs by `asl.replay`, so a bug in book logic can be
fixed and replayed without recollecting.
