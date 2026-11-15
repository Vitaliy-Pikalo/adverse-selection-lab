# PROJECT_STATUS.md

**Deadline:** 2026-12-14 · **Last updated:** 2026-11-14 · **Data freeze target:** 2026-12-07

## Status map

| Phase | Days | Status |
|---|---|---|
| 1. Repo + data audit | 1–3 | ✅ done (DATA_AUDIT.md; old DB lost → Path C) |
| M1. Data infrastructure | 1–4 | 🟡 backfill verified live; 30-day backfill running; **WS recorder smoke test pending (.env)** |
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

## Uncertain
- Real message rate, per-subscription market limit, whether `/markets` accepts `min_close_ts`/`max_close_ts` (fallback implemented).
- Where the collector runs 24/7.

## Next
1. User: add `.env` (read-only Kalshi key) → 10-min WS recorder smoke test → check `quality_report.json`.
2. Fix anything the live feed breaks; start 24/7 collection.
3. When the 30-day backfill finishes: M2 trade markout labels.
