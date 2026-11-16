# PROJECT_STATUS.md

**Deadline:** 2026-12-14 · **Last updated:** 2026-11-14 · **Data freeze target:** 2026-12-07

## Status map

| Phase | Days | Status |
|---|---|---|
| 1. Repo + data audit | 1–3 | ✅ done (DATA_AUDIT.md; old DB lost → Path C) |
| M1. Data infrastructure | 1–4 | ✅ live recorder on Lightsail since 2026-11-15 04:51 UTC; replay verified on live data; 30-day backfill running |
| M2. Backfill markouts + labels | 5–9 | ⏳ |
| M3. Experiment A: toxicity prediction | 10–15 | ⏳ |
| M4. Replay simulator + 3 strategies | 16–21 | ⏳ |
| M5. Robustness | 22–25 | ⏳ |
| M6. Report, dashboard, README | 26–28 | ⏳ |
| M7. Final verification | 29–30 | ⏳ |

## M1 — implemented
- `asl.book`: L2 book from snapshot/delta, YES-ask derived from NO bids, per-sid seq gap → invalidate, crossed-book flag, microprice/imbalance.
- `asl.recorder`: Kalshi WS (auth, orderbook_delta + trade, market rollover via update_subscription, gap → resync, backoff) + Coinbase ticker; hourly raw JSONL, gzip on close, crash-safe.
- `asl.replay`: raw → tob / trades / spot parquet + `quality_report.json` (gaps, disconnects, downtime, crossed updates, recv−exchange latency).
- `asl.backfill`: settled markets (live + historical endpoints), public trades per market, Coinbase 1-min candles; resumable.

## M1 — tested
- 29 tests: fixed-point parsing, book mechanics, seq gaps, writer rotation/crash recovery, replay end-to-end, RSA signature verification, REST pagination/retry, recorder vs local fake WS server (gap → reconnect), backfill fallbacks.
- **Not tested against real Kalshi/Coinbase** (sandbox network blocks both). The live smoke test is the M1 verification gate.

## Live API facts (2026-11-14, from probe + 1-day backfill)
- All public REST endpoints answer in < 1 s without auth. Historical cutoff: markets/trades settled before 2026-09-15 are archive-only; a 30-day window needs only live endpoints.
- `/historical/markets` did not apply close-time filters (paged the whole archive) → fixed in `61c3260`.
- KXBTC: 24 hourly events/day × ~188 strikes; 533 / 4,404 markets (12%) had any volume in one day. 30-day window = 128,962 KXBTC markets.
- Trades: microsecond `created_time`, `taker_outcome_side` on every trade, fractional sizes exist (min 0.01). One event sample: 2,169 trades over 20 markets, YES/NO takers ~50/50, trading concentrated in the final hour.
- Coinbase 1-min candles complete (1,440/1,440). BTC fell ~8% intraday in the sample day: the dataset spans a high-vol regime.

## Live recorder verification (2026-11-15 04:51–04:55 UTC, VM asl-collector)
- Service active, Ed25519 key auth OK, NTP synced.
- First minute: 15,688 Kalshi msgs, 3,022 Coinbase msgs, 400 markets subscribed (= cap), 0 gaps, 0 reconnects.
- Replay on ~4 min of live data: 0 seq gaps, 0 invalid books, 0 crossed updates, 0 server errors, 45 public trades, 7,557 Coinbase ticks.
- recv − exchange timestamp: p01 4.3 ms, p50 6.7 ms, p99 13.8 ms (includes VM↔exchange clock offset).
- Raw volume ≈ 7 MB/min uncompressed (≈ 10 GB/day before gzip). 34 GB free. Compression ratio to be measured at first hourly rollover.

## Deploys 2026-11-15 (UTC)
- 05:01 `373a31f`: 15-min markets prioritized, liveness filter, 15 s refresh. Before this, selection had collapsed at 05:00 (all 400 slots were markets closing at 05:00).
- 05:04 `aec4426`: fix false seq gaps on update_subscription `ok` replies (confirmed in raw log: `ok` seq N then snapshot N+1 flagged as gap). After deploy: 2 min, 0 gaps, 0 reconnects, 84 live markets.
- **15-min forward dataset starts 2026-11-15 05:04 UTC.** Earlier raw data (04:51–05:04) is hourly-series only.

## Check 2026-11-16 ~09:00 UTC (≈ 28 h of 15-min data)
- Found: in-process gzip froze the recorder 5–7 min every hour; `kalshi_20261115T06.jsonl.gz` corrupted by a SIGKILL during a needrestart-triggered shutdown; 140 reconnects. Fixed in `4bcbadc` (D11).
- Found: **CPU throttling.** Lightsail $7 baseline is 10% sustained; recorder needs ~70–100% of a core at ~2,300–2,600 msg/s → CPU steal 31–57%, keepalive ping timeouts. Hot path optimized in `e2d5c27` (3.4× on own code), but py-spy shows the cost is mostly websockets/TLS/asyncio, not our code. Still ~75% of a core after optimization.
- Deploy boundary for analysis: 2026-11-16 09:01–09:02 UTC (old process killed; `kalshi_20261116T08.jsonl.gz.partial-killed` set aside; complete hour kept as plain .jsonl → compressed by timer).
- Disk: ~4.5 GB/day compressed; 27 GB free → full ~Nov 21–22.

## Open issues
- **D12 (needs user decision): move recorder to a non-burstable host with a larger disk.** Recommended: EC2 c7g.medium (1 Graviton vCPU, non-burstable) in us-east-2 + ~100 GB gp3. Until then data quality is degraded under throttling.
- Small CPU fix pending: cache the hour boundary in `RawWriter` (`hour_key` was 5% of samples).

## Uncertain
- Real message rate, per-subscription market limit, whether `/markets` accepts `min_close_ts`/`max_close_ts` (fallback implemented).
- Where the collector runs 24/7.

## Next
1. Measure gzip ratio on the 05:00 hour; pick the disk fix before ~Nov 20.
2. Check the 30-day backfill completed; nightly processing job on the VM.
3. M2: trade markout labels + Experiment A on the backfill (add KXBTC15M/KXETH15M to the backfill).
