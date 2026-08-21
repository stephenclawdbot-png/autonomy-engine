# Trader Analysis: `6SHqkz…3obS`

On-chain behavioral study of Solana wallet
[`6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS`](https://solscan.io/account/6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS),
and the trading system built from it (`trader_profiler.py` +
`copy_signal_engine.py`).

**Data**: 500 transaction details (of ~1,000 signatures over 8 days)
parsed directly from Solana mainnet RPC (`getSignaturesForAddress` +
`getTransaction`), covering Aug 16–21, 2026. USD figures value SOL
legs at ~$185; stablecoin legs at face value. Residual holdings are
marked with DexScreener prices as of Aug 21.

---

## 1. Who this trader is

A **high-frequency memecoin scalper** operating around the clock with a
gap from roughly 00:00–05:00 UTC (sleep window). Activity peaks
20:00–22:00 UTC. Roughly 1,000 transactions in 8 days.

**Stack**: trades are routed through the **DFlow aggregator** (its
router and solver programs dominate the program mix), falling back to
**Jupiter v6**, with fills landing on PumpSwap, Meteora DLMM, Raydium
CLMM, Orca Whirlpool, and occasionally straight pump.fun bonding
curves. The quote currency is **USDC**, not SOL — the wallet keeps
only a small SOL float (~0.28 SOL) for fees and holds its bankroll in
stables. A platform-fee program appears in most swaps, consistent with
a trading terminal/bot front-end rather than manual DEX UIs.

**Token selection**: fresh pump.fun-ecosystem memecoins entered while
trending, normally held minutes. 63 unique tokens in ~4.7 days.

## 2. Measured behavior (Aug 16–21 window)

| Metric | Value |
|---|---|
| Transactions parsed | 500 |
| Unique tokens traded | 63 |
| Complete round trips observed | 46 |
| Win rate | 39% (18/46) |
| Capital deployed (round trips) | ~$98,500 |
| Realized PnL | **−$24,400** |
| PnL incl. residual bags at Aug-21 prices | **≈ −$6,600 (−6.7%)** |
| Median buy size | ~$500 (p25 $100, p75 $1,000, max $6,000) |
| Median hold | ~7 minutes (p25 3 min, p75 17 min) |
| PnL per trip | worst −$18,026 · median −$30 · best +$7,626 |

### Two traders in one wallet

The dataset cleanly splits into two behavior modes:

**Mode A — the disciplined scalper (44 of 46 trips): +$5,900.**
Wins 41% of trips. Median loss ≈ −$30 (about −5% of a typical
position, cut within minutes). Winners are scaled out in tranches:
the best trades turned $500 → $3,107 (+6.2×, 159 s, 9 partial sells)
and $3,000 → $10,626 (+2.5×, ~5 min). A few outlier wins pay for many
small losses.

**Mode B — the tilted gambler (2 trips): −$30,300 realized.**
- MANLET: **14 buys averaging down** to $22,500 in one token; only
  $4,474 sold out; ~87% of tokens still held (≈$13.5k at current
  price — a trapped swing position, not a scalp).
- LOOKSMAX: 13 buys, $18,500 in, held ~2 days (vs. a 7-minute median
  hold), exited/holding for a ≈−$8k mark-to-market loss.

Mode B happened on Aug 16–17, then sizes visibly shrank — a classic
blowup-then-recover arc. **Everything Mode A earned in a week, Mode B
burned in two positions.** This is the single most important finding:
the trader's edge is real but survives only when their own implicit
rules (small probes, fast stops, minutes-long holds) are obeyed.

### The shape of the edge (Mode A mechanics)

- **Probe-then-commit sizing.** Entries start ≈$100–500; size scales
  up only when the token confirms.
- **Losses cut small and fast.** Median realized loss ≈ −5% within
  minutes. Full −40% stops are rare in scalps.
- **Winners scaled out, never dumped.** Up to 9 partial sells while a
  token runs; continuous de-risking.
- **Time discipline.** p75 hold is ~17 minutes. A token that hasn't
  moved is dead inventory.
- **Expect to lose most trades.** ~40% win rate; the distribution's
  right tail is the profit engine.

## 3. The replicable system

Rules distilled from the data — Mode A's mechanics plus the guardrails
that would have blocked Mode B. Implemented in
`copy_signal_engine.SignalConfig`:

1. **Quote in stables.** Bankroll in USDC, only a fee float in SOL.
2. **Trade the flow window** (this trader: 05:00–24:00 UTC, peak
   evenings).
3. **Enter small, add only on confirmation** — probe ≈ p25 size
   ($100-scale), never add to a red position. **Averaging down is the
   documented account-killer here.**
4. **Hard per-position cap** (`max_position_usd`) and **total exposure
   cap** (`max_total_exposure_usd`). The engine emits
   `EXPOSURE_CAPPED` instead of following a Mode-B spiral — a copier
   running defaults would have skipped buys 3–14 of MANLET.
5. **Time-stop at 1 hour** (`max_hold_seconds`); median winning hold
   is minutes. LOOKSMAX sat ~42 hours.
6. **Soft-stop early, hard-stop at −40%** (`stop_loss_pct`); the
   median Mode-A loss is ~−5%.
7. **Scale out of winners in tranches**; never let a runner round-trip
   to red.
8. **Loss-streak circuit breaker** (5 consecutive losses → 30 min
   halt) and **daily loss cap** — the engine's analogue of "walk away
   when tilted," which is precisely what this trader failed to do on
   Aug 16–17.

## 4. Using the code

### Profile the wallet (research layer)

```python
from trader_profiler import get_trader_profiler

profile = get_trader_profiler().profile(
    "6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS",
    max_transactions=200,          # public-RPC friendly
)
print(profile.summarize())         # win rate, sizing, holds, venues
for rt in sorted(profile.round_trips, key=lambda r: -r.usd_in)[:10]:
    print(rt.mint, rt.realized_pnl, rt.hold_seconds)
```

Works on **any wallet** — use it to vet other traders before copying
them, and to re-audit this one over fresh windows.

### Watch it live (signal layer)

```bash
python copy_signal_engine.py 6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS
```

Pipeline: `WalletWatcher → SignalFilter → RiskManager → PaperBook`.
Signals are dropped when **stale** (>20 s old — a scalper's entry
copied late is a different, worse trade), **dust** (sub-$100 probes),
or blocked by the **risk manager** (exposure caps, daily loss limit,
loss-streak halt). Everything else books a paper position and fires
the `on_signal` callback — the seam where a real execution layer would
plug in.

## 5. Honest limitations — read before risking money

- **This trader was net negative over the sampled week** once the two
  blowups are included (≈ −$6.6k mark-to-market on ~$98.5k turnover).
  Copying them verbatim copies the blowups too; the engine's risk
  layer exists precisely because raw mirroring fails.
- **Copy latency eats scalper edge.** Median hold ~7 minutes, best
  trade peaked in 159 s. Even a 20-second delay materially degrades
  entries. Paper-trade the signal stream and measure slippage first.
- **PnL attribution is approximate.** Trips whose entries predate the
  window are excluded (`RoundTrip.complete`), SOL legs use a fixed USD
  price, residual bags are marked at a single Aug-21 price snapshot,
  and airdropped/transferred tokens can distort per-token numbers.
- **Public RPC limits.** The default endpoint is rate-limited; the
  2-second live poll wants a dedicated RPC (Helius/Triton/etc.).
- **Not financial advice.** Memecoin scalping is negative-sum after
  fees for most participants; the engine ships in paper mode on
  purpose.
