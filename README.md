# Adverse Selection Lab

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
pytest -q                   # Spark integration skips when PySpark is absent
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

## PySpark port: parity before performance

An additive PySpark DataFrame implementation labels the same **1,121,544 public
hourly trades** across KXBTC/KXETH. It uses minute-key broadcast joins, ordered
volatility windows, and the same no-lookahead label definitions. The original
pandas pipeline, stored research outputs, collector, and deployment are unchanged.
The A0 maker-loss result and unsupported pre-set directional hypothesis remain
research findings; this port adds an engineering validation of the pipeline.

**Full-data pytest parity passed for both series:** 27 columns, exact trade keys,
row counts, timestamps, missing-value masks, signs, settlements, prices, quantities,
and spot; remaining floats use `rtol=1e-10, atol=1e-12`. Settlement markouts matched
exactly; the largest horizon-markout difference was 5.70e-13 dollars/contract.

| Labeling engine | Run 1 (s) | Run 2 (s) | Run 3 (s) | Median (s) |
|---|---:|---:|---:|---:|
| pandas | 55.98 | 36.54 | 35.83 | **36.54** |
| Spark `local[*]` | 175.04 | 192.47 | 160.71 | **175.04** |

Spark took **4.79 times as long** by the median on this laptop. The data fits in
memory, and this implementation pays JVM/worker startup, scheduling/shuffle,
small-file, validation-scan, and Arrow/Python CDF costs. These are plausible
contributors, not individually profiled measurements. No Spark speedup is claimed.

Measured November 19, 2026 (Eastern), on an Intel i7-10510U (4 cores / 8 logical
CPUs), about 11.8 GiB host RAM, Ubuntu 26.04.1/WSL2 with 5.68 GiB RAM and 2 GiB
swap, Python 3.11.9, pandas 3.1.1, PySpark 4.0.1, and JDK 17. Spark used all 8
available CPUs and a 1 GiB JVM heap. Each trial was a fresh process and included
input reads, validation, labels, Snappy Parquet output, and startup/shutdown.
Inputs were pre-read for hashing; OS caches and laptop background load were not
controlled. Input storage was Windows-mounted 9p; output was Linux ext4. Native
output layouts were retained. Bootstrap summaries and parity comparison were
excluded. These are local batch timings, not distributed-cluster throughput.

Set up the isolated environment using [the WSL instructions](spark/README.md).
From this repository's Windows directory:

```powershell
# Full audited hourly-data parity (requires the existing local backfill):
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m pytest tests/test_spark_parity.py -m spark_full --spark-full-data --spark-backfill data/backfill -q -s -p no:cacheprovider

# Reproduce the six-run benchmark:
wsl -d Ubuntu -u root --cd /mnt/c/Users/pikal/adverse-selection-lab --exec /opt/asl-spark/venv/bin/python -B -m spark.benchmark run --backfill data/backfill
```

The [Spark parity workflow](.github/workflows/spark-parity.yml) installs a separate
locked Python 3.11/JDK 17 environment and runs ingestion, no-lookahead, and
synthetic sample parity tests. The deterministic 49-trade sample needs no local
research files, credentials, or data API access. Full-data tests are explicit
local opt-ins and fail if the audited event-file/row counts are missing.

[Detailed protocol, caveats, and interview concepts](spark/README.md) ·
[Raw benchmark receipts and commands](spark/benchmarks/20261120T030922Z/results.json) ·
[Input hashes](spark/benchmarks/20261120T030922Z/inputs.json)

Portfolio/resume bullet:

> Ported a 1.12M-trade labeling pipeline to PySpark using window functions and
> broadcast joins; verified 27-column parity with pytest and automated sample
> parity checks in GitHub Actions.
