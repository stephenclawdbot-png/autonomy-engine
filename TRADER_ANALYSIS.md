# Trader Analysis: `6SHqkz…3obS`

On-chain behavioral study of Solana wallet
[`6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS`](https://solscan.io/account/6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS),
and the trading system built from it (`trader_profiler.py` +
`copy_signal_engine.py`).

**Data**: transaction details parsed directly from Solana mainnet RPC
(`getSignaturesForAddress` + `getTransaction`), August 2026 window.
USD figures value SOL legs at ~$185; stablecoin legs are face value.

---

## 1. Who this trader is

A **high-frequency memecoin scalper** operating around the clock with a
gap from roughly 00:00–05:00 UTC (sleep window). Activity peaks
20:00–22:00 UTC. Roughly 1,000 transactions in 8 days.

**Stack**: trades are routed through the **DFlow aggregator** (router +
solver programs dominate the program mix), falling back to **Jupiter
v6**, with fills landing on PumpSwap, Meteora DLMM, Raydium CLMM, Orca
Whirlpool, and occasionally straight pump.fun bonding curves. The
quote currency is **USDC**, not SOL — the wallet keeps only a small
SOL float (~0.28 SOL) for fees and holds its bankroll in stables.
A dedicated platform-fee program appears in most swaps, consistent
with using a trading front-end (bot/terminal) rather than manual DEX
UIs.

**Token selection**: fresh pump.fun-ecosystem memecoins, entered
minutes-to-hours after they start trending, never held long. In the
sampled window: 28 unique tokens over ~66 hours.

## 2. Measured behavior (sampled window)

| Metric | Value |
|---|---|
| Transactions sampled | 220 (of ~1,000 in 8 days) |
| Window | ~66 h (Aug 18–21) |
| Unique tokens traded | 28 |
| Complete round trips observed | 20 |
| **Win rate** | **35%** (7/20) |
| Capital deployed (round trips) | ~$32,900 |
| **Net realized PnL** | **≈ +$604 (+1.8% on turnover)** |
| Median buy size | ~$440 (p25 $100, p75 $1,000, max $5,000) |
| Median hold | **~6 minutes** (min 5 s, p75 ~18 min) |
| PnL per trip | worst −$1,091 · median −$22 · best +$2,607 |

### The shape of the edge

The win rate is *only 35%* — this trader loses most trades. The money
is made on **asymmetry**, not accuracy:

- **Losses are cut small and fast.** Median losing trip is ≈ −$22 on a
  ~$450 position (≈ −5%), usually within minutes. Full-size stops
  (−40%) are rare.
- **Winners are scaled out, not dumped.** The best observed trade
  turned $500 → $3,107 (+6.2×) in 159 seconds across **9 partial
  sells**. Selling in tranches rides momentum while continuously
  de-risking.
- **Probe-then-commit sizing.** Entries range $100 → $5,000. Small
  probes test a token; size follows only when the token confirms.
- **A few trades pay for everything.** One +$2,607 trip covered all 13
  losing trips combined. Remove the top two winners and the window is
  net negative — the entire strategy is "survive cheaply until the
  outlier hits."

### Failure modes observed

Not everything works: the window includes a ~$6,000 position with no
exit observed (likely trapped/rugged or still held) and a −$1,091 trip
where the stop was slow (−40%). Even skilled scalpers absorb these;
the sizing discipline is what keeps them survivable.

## 3. The replicable system

Rules distilled from the measured behavior — implemented in
`copy_signal_engine.SignalConfig`:

1. **Trade the trend window, not the chart.** Only be active when flow
   exists (this trader: 05:00–24:00 UTC, peak evenings UTC).
2. **Quote in stables.** Keep the bankroll in USDC; keep only a fee
   float in SOL. PnL is then measured in dollars, not in a volatile
   quote.
3. **Enter small, add on confirmation.** Probe ≈ p25 size; scale to
   full size only when the position is green and the token still
   trends.
4. **Time-stop everything.** If a token hasn't moved in ~1 hour it is
   dead inventory — exit. Median winning hold is minutes.
5. **Hard stop at −40%, soft stop much earlier.** Median realized loss
   is ~−5%: exit as soon as momentum stalls, don't wait for the hard
   stop.
6. **Scale out of winners in tranches** (this trader: up to 9 partial
   sells). Never sell a running winner all at once; never let a
   tranche-out position round-trip to red.
7. **Expect a 35% win rate.** Position sizing must survive 5+
   consecutive losses without emotional or financial damage — the
   engine's loss-streak circuit breaker halts after 5 in a row.
8. **Cap the day.** Worst observed trader day ≈ −$1,150 on a ~$33k/3d
   turnover. The engine defaults to a $100/day loss cap at 1/10th copy
   scale.

## 4. Using the code

### Profile the wallet (research layer)

```python
from trader_profiler import get_trader_profiler

profile = get_trader_profiler().profile(
    "6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS",
    max_transactions=200,          # public RPC friendly
)
print(profile.summarize())         # win rate, sizing, holds, venues
for rt in sorted(profile.round_trips, key=lambda r: -r.usd_in)[:10]:
    print(rt.mint, rt.realized_pnl, rt.hold_seconds)
```

Works on **any wallet** — use it to vet other traders before copying
them.

### Watch it live (signal layer)

```bash
python copy_signal_engine.py 6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS
```

Pipeline: `WalletWatcher → SignalFilter → RiskManager → PaperBook`.
Signals are dropped when **stale** (>20 s old — a scalper's entry
copied late is a different, worse trade), **dust** (trader probing
below $100), or blocked by the **risk manager** (exposure caps, daily
loss limit, loss-streak halt). Everything else books a paper position
and fires the `on_signal` callback, which is where a real execution
layer would plug in.

## 5. Honest limitations — read before risking money

- **Copy latency eats scalper edge.** This trader's median hold is 6
  minutes and best trade peaked in 159 seconds. Even a 20-second copy
  delay materially degrades entries. Paper-trade the signal stream and
  measure slippage before considering live execution.
- **Survivorship risk.** The sampled window is net +1.8% on turnover —
  thin. A different week could be net negative; nothing here proves
  durable edge.
- **PnL attribution is approximate.** Positions opened before the
  sample window are excluded from win-rate math (`RoundTrip.complete`),
  SOL legs use a fixed USD price, and airdropped/transferred tokens can
  distort per-token numbers.
- **Public RPC limits.** The default endpoint is rate-limited; for the
  2-second live poll you want a dedicated RPC (Helius/Triton/etc.).
- **This is not financial advice.** Memecoin scalping is a
  negative-sum game after fees for most participants; the engine ships
  in paper mode on purpose.
