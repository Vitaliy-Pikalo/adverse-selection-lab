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

## D9 — 2026-11-15 — Ed25519 key support
- **Decision:** Auth accepts Ed25519 (Kalshi's current default) and RSA keys; signature algorithm follows the key type.
- **Reason:** The VM's newly created key was Ed25519; the RSA-only loader would have failed at connect time.

## D10 — 2026-11-15 — Center the study on 15-minute up/down markets
- **Evidence (public API, settled markets in the 24 h to 2026-11-15 05:00 UTC):** KXBTC15M 96 events, 96/96 traded, 395.3M contracts; KXETH15M 25.9M; KXBTCD 44.7M (820 traded strikes); KXBTC 0.88M (526 traded of 4,404). KXBTC15M open market quoted 0.9760/0.9770 (sub-cent ticks).
- **Decision:** Always subscribe KXBTC15M + KXETH15M; hourly series fill the remaining slots, ranked by volume, keeping already-subscribed live markets to avoid churn. Untraded markets must be quoted with mid in [0.02, 0.98]. Refresh every 15 s so each new 15-min market is captured from its first seconds.
- **Reason:** 4× more independent events/day than hourly series and far more trades per event → more statistical power for Experiment B; a single strike per event that is a pure bet on short-horizon spot direction fits the stale-quote hypothesis (D2) directly.
- **Rejected:** soonest-closing-first selection (filled 400 slots with dead strikes).
- **Data boundary:** recordings before this deploy lack 15-min markets; the forward dataset for 15-min analysis starts at the deploy time logged in EXPERIMENT_LOG/PROJECT_STATUS.

## D11 — 2026-11-16 — Compression moved out of the recorder; auto-restarts blocked
- **Evidence (VM check 2026-11-16 ~08:55 UTC):** hourly .gz files finished at :05–:07 → in-process gzip of a ~2.4 GB hour froze the asyncio loop for 5–7 min every hour (data loss at the start of each hour; 140 Kalshi reconnects in ~26 h). On 2026-11-15 06:46:55 unattended-upgrades/needrestart restarted the service; shutdown blocked in gzip past TimeoutStopSec=60 → SIGKILL → `kalshi_20261115T06.jsonl.gz` corrupt ("invalid compressed data"); the restart then appended to the truncated gzip.
- **Decision:** writer only appends plain .jsonl and never compresses; `asl.compress` (systemd timer, every 10 min, Nice=19, idle IO, CPUQuota 50%) compresses closed hours to zstd level 9 via .tmp + fsync + full verify + atomic rename, then deletes the source; late data for an already-compressed hour becomes `.pN`. needrestart may not restart asl-recorder. Replay reads .gz/.zst/parts and reports corrupt tails.
- **Data-quality consequence:** hours 2026-11-15 05:00 → 2026-11-16 deploy have a ~5–7 min hole after each hour boundary; the 06:00 UTC 2026-11-15 file is readable only up to the corruption point. Both are flagged, not hidden: analysis must exclude or mark these windows.
- **Disk:** ~130–210 MB/h compressed (gzip) ≈ 4.5 GB/day; 27 GB free on 2026-11-16 ≈ 6 days. Open decision (D12).

## D13 — 2026-11-17 — Markout label definitions (M2)
- **Decision:** Labels are from the maker's side (opposite the taker). Report three quantities: `maker_edge` = model value − price at the trade; `move_h` = model value change from t to t+h (h = 1, 5, 15 min); `mo_settle` = settlement value − price. Values past close pin to the settlement value. Model = log-normal binary pricer, per-minute σ from 60 min of 1-min log returns ending before the trade's minute; spot = open of the trade's minute. Block trades and trades at/after close are dropped. CIs are 95% event-block bootstrap, contract-weighted.
- **Reason:** Splitting `mo_h = maker_edge + move_h` separates "the model's level is off" from "the value moved after the fill"; only the second is adverse selection. Event blocks because trades in one event share one spot path (DATA_AUDIT.md §6). `mo_settle` gives a model-free check.
- **Rejected:** trade-price markouts (next trade in the same market) — most strikes trade too rarely for a 1-minute horizon; forward-filling spot across candle gaps (hides missing data).
- **Known limitation:** 1-min spot cannot resolve second-scale stale quotes; Kalshi settles on a 60-s index average, ignored except via pinning at close.
