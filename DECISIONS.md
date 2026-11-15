# DECISIONS.md

Append-only. Each entry: date, decision, reason, alternatives rejected.

## D1 — 2026-11-14 — Path C; old DB treated as lost
- **Decision:** Proceed on Path C (new collection + historical backfill). `poker_quant.db` is treated as lost.
- **Reason:** DB not found on the user's machine; user confirmed it is likely lost. Even if found, its schema (top-of-book prices at 60 s, no sizes or trades) cannot support fill simulation. See DATA_AUDIT.md §5.
- **Rejected:** Path A/B on old data.

## D2 — 2026-11-14 — Research question narrowed to stale-quote adverse selection
- **Decision:** Main hypothesis = maker adverse selection on Kalshi BTC/ETH is driven mainly by the underlying moving before the Kalshi book reprices.
- **Reason:** Gives a mechanistic, testable hypothesis; reuses the log-normal pricer (binary delta ∂P/∂S); measurable historically from public trades + spot, and forward from L2.
- **Rejected:** Generic microstructure feature model with no prior on what informs traders.

## D3 — 2026-11-14 — Underlying reference: Coinbase as proxy for CF Benchmarks BRTI/ETH RTI
- **Decision:** Record Coinbase BTC-USD / ETH-USD ticker as the high-frequency underlying.
- **Reason:** KXBTC settles on the 60-second simple average of the CF Bitcoin Real-Time Index before expiry; the index aggregates several exchanges. Coinbase is a free, public, sub-second constituent.
- **Known limitation:** proxy error vs the index; settlement averaging over 60 s (and trimmed mean on KXBTCD) changes payoff near expiry. Revisit if Kalshi's CF Benchmarks pass-through endpoint is accessible.

## D4 — 2026-11-14 — Raw-first storage
- **Decision:** Log every raw WS message (JSONL, hourly files, gzip on close) with wall-clock and monotonic receive timestamps; derive everything by replay.
- **Reason:** Reproducibility; book bugs can be fixed without recollection.
- **Rejected:** Writing parsed top-of-book directly to SQLite (loses information, not replayable).

## D5 — 2026-11-14 — Integer fixed-point prices/sizes
- **Decision:** price ticks = $0.0001, size = 1/100 contract, integers throughout.
- **Reason:** Summing millions of float deltas drifts; Kalshi sends exact decimal strings.

## D6 — 2026-11-14 — Seq gap ⇒ full resubscribe
- **Decision:** On an orderbook seq gap (seq is per subscription, not per market) the recorder drops the connection and resubscribes; replay invalidates every book on that subscription until it gets a fresh snapshot.
- **Reason:** Simplest correct recovery. Cost: a few seconds of missing data per gap, visible in the quality report.

## D7 — 2026-11-14 — Collected markets
- **Decision:** KXBTC, KXETH (hourly range) and KXBTCD, KXETHD (hourly above/below), closing within 6 h, cap 400 markets.
- **Reason:** Hourly expiries give many independent events per day (the unit of statistical independence). Cap avoids an undocumented per-subscription market limit (Kalshi error code 26).
- **Open:** confirm real message volume and whether 400 trips the limit during the smoke test.

## D8 — 2026-11-14 — 24/7 collector on AWS Lightsail (Ohio), not a home machine
- **Decision:** Run the recorder on a Lightsail instance `asl-collector`, us-east-2a, Ubuntu 24.04, 1 GB RAM / 2 vCPU / 40 GB SSD, dual-stack, $7/month (Levee AWS account). 1 GB swap added. Separate read-only Kalshi API key for the VM.
- **Reason:** A laptop or home PC sleeps, reboots and has no one to restart it; residential latency is higher and noisier, which contaminates recv-vs-exchange timing. Kalshi's API origin is reported to be AWS us-east-2.
- **Rejected:** overnight-only laptop recording (hour-of-day sampling bias); home PC (needs in-person setup, no remote restart); 512 MB plan (OOM risk during replay).
- **Cost to deadline:** ~$7–8. Delete the instance after Dec 14.
