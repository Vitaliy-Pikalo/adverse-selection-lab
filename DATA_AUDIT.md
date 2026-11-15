# DATA_AUDIT.md — Adverse Selection Lab, Phase 1

**Audit date:** 2026-11-14
**Auditor scope:** read-only. No databases, production code or services were modified.
**Sources inspected:**
- GitHub `Vitaliy-Pikalo/kalshi-trading-bot` @ `5709eed` (2026-05-21, 8 commits, ~4,100 LOC Python), cloned read-only
- `HANDOFF.md`, `PROJECT_STORY.md`, `README.md` in that repo
- Local Windows machine (`C:\Users\pikal`) via file search

---

## 0. Headline findings

| # | Finding | Impact |
|---|---|---|
| 1 | **The SQLite database was not found.** The documented local path `C:\Users\pikal\Downloads\claude project\` no longer exists. No project `*.db` / `poker_quant*` file exists under `C:\Users\pikal`. A full-drive search of `C:\` was inconclusive (permission errors on system folders, zero hits before failure). The DB is gitignored, so it is not on GitHub. | Row-level checks (coverage, gaps, duplicates, counts) **could not be run**. Everything below is from code and docs. |
| 2 | **Snapshots hold top-of-book prices only.** No bid/ask sizes, no depth, no public trades, no order IDs. | Order-book imbalance, microprice, queue position and fill simulation are **impossible** from this data. |
| 3 | **Timestamps are local poll times, not exchange times**, shared by every ticker in a poll, ~60 s apart. | Too coarse for any adverse-selection measurement on a market whose underlying (BTC/ETH) moves every millisecond. |
| 4 | **`markets.settled_outcome` is never written** by any code path. The settler only fetches results for markets with paper fills. | The 270k snapshots have **no outcome labels in the DB**. Labels must be re-fetched from the Kalshi API. |
| 5 | **Paper fills ignore fees and use a stale price.** Fills cross at the snapshot ask (up to ~60 s old), 1-contract minimum, no size check, no fees. Edge was computed vs **mid** but executed at **ask**. | v1/v2/v3 ROI figures are biased upward by fees + half-spread. Taker fee at 22¢ for 1 contract rounds up to 2¢ ≈ 9% of cost. |
| 6 | **Model inputs were not persisted.** Spot price and σ used for each prediction are not stored. σ = 30-day **daily** realized vol, held flat intraday, applied to hourly markets. | Predictions can't be reproduced or decomposed. The σ choice is a likely cause of the documented [0.20, 0.40) miscalibration. |
| 7 | **No backtester, minimal tests.** `src/backtest/` is empty. `tests/` covers config loading only. | Nothing reusable for an execution simulator. |

**Supported path: C (for the market-making question).** See §5.

---

## 1. Repository architecture

```
src/
  kalshi/client.py        RSA-signed REST client (markets, events, series, orderbook endpoint defined but unused by the poller)
  ingest/discover.py      market discovery → markets table (every 6 h)
  ingest/poller.py        bulk GET /markets?tickers=... → snapshots table (every 60 s)
  ingest/settler.py       fetch `result` for markets with fills → fills.realized_pnl (every 30 min)
  spot/coinbase.py        Coinbase spot + candles (not persisted)
  spot/tennis_data.py     tennis Elo inputs
  strategy/crypto_vol.py  log-normal pricer for greater/less/between strikes
  strategy/tennis_elo.py  surface Elo
  strategy/paper_sim.py   gates + quarter-Kelly sizing → fills table (every 5 min)
  run/daemon.py           single-process scheduler; presets conservative/balanced/aggressive/range_v1
  analyze.py, diagnose*.py, show_edges.py, investigate_loss.py   ad-hoc analysis
  backtest/, models/, live/   empty packages
tests/test_config.py      2 tests, config only
```

Stack: Python, SQLAlchemy 2, SQLite (WAL), httpx, pandas. `requirements.txt` lists xgboost/streamlit but neither is used.

---

## 2. Database schema (from `src/db.py`)

DB URL default: `sqlite:///data/poker_quant.db`

| Table | Columns | Notes |
|---|---|---|
| `markets` | ticker (PK), title, series_ticker, event_ticker, open_ts, close_ts, settled_outcome, first_seen, strike_type, floor_strike, cap_strike | `settled_outcome` is always NULL (never written). |
| `snapshots` | id, ticker, ts, yes_bid, yes_ask, no_bid, no_ask, last_price, volume, open_interest | Prices are **integer cents**. `volume` is cumulative. No sizes. |
| `predictions` | id, ticker, ts, model_version, predicted_prob, market_implied_prob, edge | `market_implied_prob` = mid (or ask if no bid). No spot/σ/T stored. |
| `fills` | id, ticker, ts, side, price_cents, contracts, cost_usd, is_paper, realized_pnl | All paper. No fee column. |
| `bankroll` | id, ts, total_usd, open_position_value, realized_pnl_today, note | |

---

## 3. Field-by-field availability

| Required field | Available? | Detail |
|---|---|---|
| Market identifiers | ✅ | ticker, series, event, strike type/levels (from Kalshi fields, post-v1 fix) |
| Event timestamps | ⚠️ | `snapshots.ts` = poller start time (naive UTC, local clock), identical across a poll batch. No exchange timestamps. |
| Sampling frequency | ⚠️ | Nominal 60 s; actual depends on cycle time (bulk 100 tickers/request, client throttle, up to 1,500 tickers). Real cadence unknown without the DB. |
| Coverage | ⚠️ | Only markets closing within 48 h. Series: KXBTC, KXBTCD, KXBTCDB, KXBTC15M, KXETH, KXETHD, KXETH15M + 6 tennis series. Run window ≈ 2026-05-14 → 05-21 (~1 week per docs). |
| Best bid/ask | ✅ | Integer cents. On Kalshi, yes_ask ≡ 100 − no_bid and no_ask ≡ 100 − yes_bid, so the 4 columns carry 2 independent numbers (verify on data). Any sub-cent pricing is rounded away. |
| Bid/ask quantities | ❌ | Not collected. |
| Order-book depth | ❌ | Not collected. `get_orderbook()` exists in the client but is never called by the poller. |
| Public trade prints | ❌ | Not collected. Only proxy: Δ`volume` between snapshots (unsigned contracts per ~minute) and `last_price` changes. |
| Prediction records | ✅ | ~13k rows (per docs). Written every 5-min trade cycle for every eligible market → heavily autocorrelated. Effective sample size ≈ number of settled **events**, not rows. |
| Model outputs | ⚠️ | Probability + edge only. Inputs (spot, σ, T) not stored. |
| Expiration | ✅ | `markets.close_ts` |
| Settlement | ❌ in DB / ✅ recoverable | Not stored for snapshot markets. Recoverable via `GET /markets/{ticker}` or the historical markets endpoint. |
| Missing values / duplicates / gaps | ❓ | Not checkable without the DB. Known risks: (a) two-daemon period before WAL fix may have written duplicate snapshots; (b) daemon restarts on 05-19 leave gaps; (c) `ts` is not unique per ticker if a poll retries. |
| Backtest / execution logic | ❌ | `paper_sim.py` = taker-only fill at snapshot ask, no fees, no latency, no size constraint. No replay engine. |

---

## 4. Methodology issues in the existing work

These matter for the new project's credibility; they should be stated, not hidden.

1. **Fees omitted.** Kalshi taker fee = round_up(0.07 × C × P × (1 − P)). For 1 contract at 22¢ that is 1.2¢ → rounded to 2¢, ≈ 9% of cost. On a strategy with ~22¢ average entry this alone moves ROI by several percentage points.
2. **Edge vs mid, fill at ask.** Gate `edge = predicted − mid` but execution at the ask means realized edge is overstated by the half-spread on every trade.
3. **Stale-quote fills.** Decisions use a snapshot up to ~60 s old (plus a 5-min trade cycle). In live trading that ask may not exist anymore — this is exactly the adverse-selection problem the new project studies, in reverse.
4. **σ mismatch.** 30-day daily-close realized vol, annualized, cached 1 h, applied to markets expiring in < 1 h to 48 h. Intraday vol seasonality and short-horizon vol are ignored.
5. **Sample sizes.** v2 n=104 fills, v3 n=22. Fills in the same hourly event are correlated (same underlying draw). The v3 negative result is directionally informative but not statistically decisive either.
6. **Calibration buckets on fills only.** Calibration was measured on traded markets (selected on edge), not on all predictions — a selection effect.

---

## 5. Path determination

| Path | Requirement | Status |
|---|---|---|
| **A — Full replay** | L2 book + trade prints with timestamps fine enough to model queue and latency | ❌ No sizes, no depth, no trades, 60 s poll clock |
| **B — Top-of-book research** | Bid/ask observations sufficient to build a toxicity predictor | ⚠️ Prices exist, but no sizes (no imbalance/microprice), no signed flow, 60 s resolution, no labels in DB, and the DB is currently missing |
| **C — Limited historical data** | Prices + predictions; start new collection | ✅ **This is what the evidence supports** |

**Verdict: Path C.** Even if the DB is recovered, it cannot support Experiment B (strategy comparison with fills) and supports Experiment A only weakly (60-s mid-move labels without book features). Its best use is a **separate forecasting/calibration benchmark** (log-normal pricer vs market mid vs recalibrated model), after re-fetching outcome labels.

### 5.1 What changes the picture: data we can still get

| Source | What it gives | Status |
|---|---|---|
| Kalshi `GET /markets/trades` and `GET /historical/trades` | Public trade prints: ticker, price, count, `taker_outcome_side` / `taker_book_side`, `created_time`. Public endpoint (no auth in spec). | Not tested from this sandbox (network allowlist blocked Kalshi). Must verify history depth for KXBTC/KXETH from your machine. |
| Kalshi historical markets / candlesticks | Settlement results, OHLC of prices | Same — verify. |
| Kalshi WebSocket `orderbook_delta` (+ `orderbook_snapshot`) and `trade` channels | L2 aggregated price levels with `seq` numbers, trades in real time | Forward-only. Requires API-key auth (you have one). No order IDs → queue position must be modeled, not observed. |
| Coinbase 1-min candles (historical) + WebSocket ticker (forward) | Underlying spot path | Historical 1-min is coarse; forward WebSocket gives sub-second. |

**Signed public trades are richer for this research question than the old DB.** They allow trade markouts (price move after each trade, from the maker's side) — the standard empirical measure of adverse selection — without any order book.

---

## 6. Recommended research direction

**Change the question slightly:**

> On Kalshi BTC/ETH binary markets, is maker adverse selection driven mainly by **stale quotes relative to the underlying spot** (latency/pick-off risk), and does a quoting policy that re-prices on spot moves outperform static and inventory-aware quoting under conservative fill assumptions?

Why this is stronger than generic microstructure toxicity:

- Kalshi crypto contracts are derivatives of a continuously traded underlying. The obvious informed trader is someone who saw Coinbase/Binance move before the Kalshi book did. That gives a **mechanistic hypothesis**, not a feature-soup model.
- The key feature — spot move since the maker's last quote update × binary delta ∂P/∂S — comes straight from the log-normal pricer you already built. Your old work becomes a component, not a dead end.
- It is testable in stages: Experiment A on **historical** trades + 1-min spot (backfill, available now); Experiment B on **forward-collected** L2 + trades + sub-second spot.
- A negative result ("book imbalance adds nothing beyond spot delta" or "toxicity filter doesn't survive fees") is still a clean, explainable finding.

Settlement reference (verified 2026-11-14): KXBTC resolves on the simple average of the 60 seconds of CF Benchmarks' Bitcoin Real-Time Index (BRTI) before expiry; KXBTCD uses the same window with the top and bottom 20% of values excluded. Coinbase is one index constituent and is used as a proxy (DECISIONS.md D3).

---

## 7. Feasibility vs the 30-day deadline (2026-12-14)

The binding constraint is **forward data**. Every day the collector isn't running is a day of test data lost.

| If collector starts | L2 data by Dec 7 (freeze) | Realistic train/val/test |
|---|---|---|
| Nov 17 | ~20 days | ~12 / 4 / 4 days |
| Nov 21 | ~16 days | ~10 / 3 / 3 days |

At ~24 hourly events/day/asset, 4 test days ≈ 190 events. Strikes within an event are correlated, so the effective n for P&L comparisons is events, not markets or fills. **Expect wide confidence intervals on strategy P&L differences.** Experiment A (markout prediction on backfilled trades) will have far more power and should carry the main finding.

**Narrower credible deliverable:**
1. Historical: markout analysis + toxicity classifier on backfilled public trades (main result).
2. Forward: ~2–3 weeks of L2 + trades; replay simulator with conservative queue model; 3 strategies; event-block bootstrap CIs.
3. Old DB (if recovered): calibration benchmark appendix only.
4. Report, dashboard, README. Skip LightGBM unless logistic regression shows non-trivial signal.

Infrastructure question: the collector must run 24/7. A laptop that sleeps will create gaps. An always-on host (cheap VPS, free-tier cloud VM, or a spare machine) is needed.

---

## 8. Open items blocking a complete audit

- [x] ~~Locate `poker_quant.db`~~ — user confirmed likely lost (2026-11-14). Row-level audit will not be performed. Old results stand only as documented in the old repo's PROJECT_STORY.md.
- [ ] Verify Kalshi historical trade depth for KXBTC/KXETH (how far back, rate limits).
- [x] Confirm settlement index for KXBTC/KXETH — CF Benchmarks RTI, 60 s average (see §6).
- [ ] Decide collector host.

## 9. First milestone (M1, Days 1–4) — approved 2026-11-14, implemented (see PROJECT_STATUS.md)

1. New repo `adverse-selection-lab` (do not modify `kalshi-trading-bot`).
2. **Forward collector:** Kalshi WebSocket `orderbook_delta` + `trade` for KXBTC/KXETH near-expiry markets, Coinbase BTC/ETH ticker; raw messages appended to daily Parquet/JSONL with receive timestamps; `seq`-gap detection, auto-resubscribe with fresh snapshot, heartbeat log.
3. **Book reconstruction** module with tests: snapshot + deltas → L2 state; seq gap → invalidate; replay from raw log must reproduce live state.
4. **Historical backfill:** public trades + market results + 1-min spot for the last N weeks → Parquet.
5. **Verification gate:** 24 h of collection with a gap report and a reconstruction test passing on real data.

No live orders. No changes to existing DBs or the old bot.
