# EXPERIMENT_LOG.md

Every reported number must point to: dataset (raw file range or backfill run), config, git commit.

| ID | Date | Question | Data | Config | Commit | Result | Notes |
|---|---|---|---|---|---|---|---|
| A0 | 2026-11-17 | Do makers on KXBTC/KXETH lose to takers after the spread, and does pre-trade spot movement (D2 `stale_move`) predict it? | Backfill run 2026-11-15 03:38 UTC (`data/backfill`, 2026-10-16 → 2026-11-15): KXBTC 1,010,733 trades / 704 events, KXETH 110,811 / 693. Coinbase 1-min candles. | `python -m asl.markouts --series KXBTC KXETH` (horizons 1/5/15 min, σ = 60-min realized, 500 event-block bootstrap reps) | `c0dd96a` | See A0 below | Labels pre-fee. 1-min spot resolution. |

## A0 — Maker markouts on 30 days of public trades (2026-11-17)

Contract-weighted means, cents per contract, maker's side. 95% CIs resample whole events (trades in one event share one spot path). `mo_h` = markout vs model value at t+h; `maker_edge` = model value minus price at the trade, maker-signed; `move_h` = value change after the trade (`mo_h = maker_edge + move_h`).

| KXBTC (1.01M trades, 704 events) | mean | 95% CI | share < 0 |
|---|---|---|---|
| maker_edge (at trade) | +2.55 | [+2.28, +2.84] | 36.8% |
| move_1m | −1.92 | [−2.28, −1.62] | 59.0% |
| move_5m | −2.41 | [−2.97, −1.89] | 56.7% |
| move_15m | −2.90 | [−3.52, −2.34] | 54.9% |
| mo_settle (model-free) | −0.73 | [−1.39, −0.07] | 47.7% |

KXETH (111k trades, 693 events): maker_edge +1.78 [+1.12, +2.49], move_1m −1.02 [−1.66, −0.50], mo_settle −2.53 [−5.37, +0.45]. Same signs, wider intervals.

**Finding 1 — adverse selection is real and larger than the spread on KXBTC.** Makers fill 2.55¢ better than the model, then the value moves 1.9–2.9¢ against them; at settlement they lose 0.73¢ per contract on average (CI excludes 0). Most of the adverse move happens in the first minute.

**Finding 2 — the directional form of D2 is not supported at 1-min resolution.** KXBTC `move_1m` by `stale_move_1m` quintile (taker-signed model move over the minute before the trade):

| quintile | stale_move_1m range (¢) | move_1m | mo_settle |
|---|---|---|---|
| 0 (taker trades against the recent move) | [−100, −1.76] | −3.26 [−4.95, −2.10] | −2.29 [−3.92, −0.82] |
| 1 | [−1.76, −0.04] | −0.72 [−1.38, −0.18] | −0.13 [−1.61, +1.27] |
| 2 (no recent move) | [−0.04, 0.47] | −0.92 [−1.38, −0.54] | −0.45 [−2.11, +0.96] |
| 3 | [0.47, 3.15] | −1.59 [−2.08, −1.09] | −0.32 [−1.53, +1.04] |
| 4 (taker trades with the recent move) | [3.15, 100] | −2.77 [−3.93, −1.59] | +0.02 [−1.50, +1.87] |

D2 predicts losses rising monotonically from quintile 0 to 4. Instead the pattern is U-shaped: trades after large spot moves in either direction are the most toxic. The size of the recent move matters; its sign does not, at this resolution.

**Caveats.**
- Spot is the open of the trade's 1-min candle, so it can be up to 59 s stale. Stale-quote picking happens in seconds, so a null on direction here is weak evidence. The forward recorder's tick-level Coinbase data is the real test (M3).
- `move_h` uses the log-normal model with 60-min realized σ; near-expiry and deep-OTM strikes carry the most model error. `mo_settle` is model-free but noisy.
- Labels are pre-fee.

**Next.** Use |stale_move| and recent realized vol as features in Experiment A (M3); re-run A0 on KXBTC15M (backfill running) and on forward tick data.
