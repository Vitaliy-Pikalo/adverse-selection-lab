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

## Open issues
- **Market cap binding:** 400 subscribed = `max_markets`. Soonest-closing markets win, so far-from-money dead strikes take slots. Fix: rank by proximity to spot / recent volume (M1.1).
- **Disk budget:** verify gzip ratio; if < 5×, add pruning or move closed hours to cheaper storage.

## Uncertain
- Real message rate, per-subscription market limit, whether `/markets` accepts `min_close_ts`/`max_close_ts` (fallback implemented).
- Where the collector runs 24/7.

## Next
1. Measure gzip ratio after the 05:00 UTC rollover; fix market selection (near-money first).
2. When the 30-day backfill finishes: M2 trade markout labels.
