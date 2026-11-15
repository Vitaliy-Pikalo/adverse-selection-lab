# PROJECT_STATUS.md

**Deadline:** 2026-12-14 · **Last updated:** 2026-11-14 · **Data freeze target:** 2026-12-07

## Status map

| Phase | Days | Status |
|---|---|---|
| 1. Repo + data audit | 1–3 | ✅ done (DATA_AUDIT.md; old DB lost → Path C) |
| M1. Data infrastructure | 1–4 | 🟡 code done + tested offline; **needs live smoke test on user machine** |
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

## Uncertain
- Real message rate, per-subscription market limit, whether `/markets` accepts `min_close_ts`/`max_close_ts` (fallback implemented).
- How far back `/historical/trades` goes for KXBTC/KXETH.
- Where the collector runs 24/7.

## Next
1. User: 10-min smoke test + quick backfill on own machine; send `quality_report.json` and `backfill_log.json`.
2. Fix anything the live feed breaks; start 24/7 collection.
3. M2: trade markout labels on backfilled data.
