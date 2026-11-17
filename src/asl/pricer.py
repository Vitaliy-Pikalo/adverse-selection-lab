"""Log-normal binary pricer for Kalshi crypto contracts (vectorized).

Kalshi strike types (from the /markets payload):
  greater: YES if settle > floor_strike
  less:    YES if settle < cap_strike
  between: YES if floor_strike <= settle <= cap_strike

Under driftless GBM over tau minutes with per-minute vol sigma:
  P(S_T > K) = N(d2),  d2 = (ln(S/K) - sigma^2 tau / 2) / (sigma sqrt(tau))

Time is in minutes and sigma is per-minute, so no annualization choices leak in
(DATA_AUDIT.md §4 item 4: the old bot applied daily vol to hourly markets).
Settlement is a 60 s average of the index (D3); that is ignored here and only
matters in the last minute, where labels pin to the settlement value anyway.
"""

from __future__ import annotations

import math

import numpy as np

_erf = np.vectorize(math.erf, otypes=[float])
SIGMA_FLOOR = 1e-6  # per-minute; avoids divide-by-zero in flat minutes


def norm_cdf(x: np.ndarray) -> np.ndarray:
    return 0.5 * (1.0 + _erf(np.asarray(x, dtype=float) / math.sqrt(2.0)))


def prob_above(spot, strike, tau_min, sigma_min) -> np.ndarray:
    """P(S_T > K). tau <= 0 returns the indicator S > K. NaN strike -> NaN."""
    s = np.asarray(spot, dtype=float)
    k = np.asarray(strike, dtype=float)
    tau = np.asarray(tau_min, dtype=float)
    sig = np.maximum(np.asarray(sigma_min, dtype=float), SIGMA_FLOOR)
    s, k, tau, sig = np.broadcast_arrays(s, k, tau, sig)
    out = np.full(s.shape, np.nan)
    live = tau > 0
    ok = np.isfinite(s) & np.isfinite(k) & (s > 0) & (k > 0)
    m = ok & live
    sd = sig[m] * np.sqrt(tau[m])
    d2 = (np.log(s[m] / k[m]) - 0.5 * sd**2) / sd
    out[m] = norm_cdf(d2)
    e = ok & ~live
    out[e] = (s[e] > k[e]).astype(float)
    return out


def prob_yes(strike_type, floor, cap, spot, tau_min, sigma_min) -> np.ndarray:
    """Model P(YES) for arrays of contracts with mixed strike types."""
    st = np.asarray(strike_type, dtype=object)
    above_floor = prob_above(spot, floor, tau_min, sigma_min)
    above_cap = prob_above(spot, cap, tau_min, sigma_min)
    out = np.full(st.shape, np.nan)
    g, lo, b = st == "greater", st == "less", st == "between"
    out[g] = above_floor[g]
    out[lo] = 1.0 - above_cap[lo]
    # between [floor, cap]: P(S > floor) - P(S > cap). Point mass at the
    # boundaries is zero under GBM, so >= vs > doesn't matter.
    out[b] = above_floor[b] - above_cap[b]
    return np.clip(out, 0.0, 1.0)
