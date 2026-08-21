"""
Backtester - Replay a trader's recorded history through the copy pipeline.

Answers the question the live paper book cannot: "what would copying this
wallet actually have returned?" — by replaying the trader's real fills
in timestamp order through the same filters and risk rules the live
engine uses (SignalConfig), under configurable copy latency and slippage
assumptions.

Model (deliberately conservative):
- Copier entries/exits are priced off the trader's own fill prices
  (quote_usd / token_amount per trade), degraded by `slippage_pct` in
  the adverse direction on BOTH legs. For a memecoin scalper the trader
  IS a large part of the price action, so copying behind them fills
  worse than they did — never better.
- The copier only exits when the trader exits (pro-rata to the trader's
  exit fraction). Tokens the trader never sold in the window are marked
  at `abandoned_recovery` (default 0.25 = 75% loss) — the MANLET/
  LOOKSMAX failure mode, priced in rather than ignored.
- Buys the live engine would have dropped (dust, exposure caps, loss
  streak, daily loss limit) are dropped here too, with the same
  RiskManager.
- `latency_seconds` models the copy delay; trades where the trader's
  next same-token action happened within the latency window are
  entered at that NEXT price (you were late; you got the later price).

Usage:
    from backtester import Backtester
    bt = Backtester()
    result = bt.run(trades)                    # trades from TraderProfiler
    print(result.report())

    # or over saved raw transactions:
    python backtester.py wallet_data.json [more.json ...]

No external dependencies - standard library only.
"""

import json
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import logging

from trader_profiler import TraderProfiler, Trade, TradeSide
from copy_signal_engine import SignalConfig, RiskManager, SignalVerdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Backtester")


@dataclass
class BacktestPosition:
    mint: str
    usd_in: float = 0.0
    tokens: float = 0.0          # copier's token inventory (scaled units)
    usd_out: float = 0.0
    opened_ts: int = 0
    closed: bool = False
    abandoned: bool = False

    @property
    def pnl(self) -> float:
        return self.usd_out - self.usd_in


@dataclass
class BacktestResult:
    config: SignalConfig
    latency_seconds: float
    slippage_pct: float
    abandoned_recovery: float
    positions: List[BacktestPosition] = field(default_factory=list)
    dropped: Dict[str, int] = field(default_factory=dict)
    copied_buys: int = 0

    @property
    def closed_positions(self) -> List[BacktestPosition]:
        return [p for p in self.positions if p.closed]

    def summary(self) -> Dict:
        closed = self.closed_positions
        wins = [p for p in closed if p.pnl > 0]
        total_in = sum(p.usd_in for p in closed)
        total_pnl = sum(p.pnl for p in closed)
        return {
            "latency_seconds": self.latency_seconds,
            "slippage_pct": self.slippage_pct,
            "copied_buys": self.copied_buys,
            "round_trips": len(closed),
            "abandoned": sum(1 for p in closed if p.abandoned),
            "win_rate": round(len(wins) / len(closed), 3) if closed else 0.0,
            "usd_deployed": round(total_in, 2),
            "net_pnl_usd": round(total_pnl, 2),
            "return_on_turnover": round(total_pnl / total_in, 4)
                                  if total_in else 0.0,
            "dropped": dict(self.dropped),
        }

    def report(self) -> str:
        s = self.summary()
        return (f"latency={s['latency_seconds']:.0f}s "
                f"slippage={100 * self.slippage_pct:.0f}%: "
                f"{s['round_trips']} trips ({s['abandoned']} abandoned), "
                f"WR {s['win_rate']:.0%}, "
                f"${s['usd_deployed']:,.0f} deployed -> "
                f"${s['net_pnl_usd']:+,.0f} "
                f"({100 * s['return_on_turnover']:+.1f}% on turnover), "
                f"dropped {s['dropped']}")


class Backtester:
    """Replays trader fills through the copy filters and risk rules."""

    def __init__(self, config: Optional[SignalConfig] = None,
                 latency_seconds: float = 5.0,
                 slippage_pct: float = 0.03,
                 abandoned_recovery: float = 0.25):
        self.config = config or SignalConfig()
        self.latency_seconds = latency_seconds
        self.slippage_pct = slippage_pct
        self.abandoned_recovery = abandoned_recovery

    # -- price helpers -------------------------------------------------------

    @staticmethod
    def _price(trade: Trade) -> Optional[float]:
        if trade.token_amount > 0 and trade.quote_usd > 0:
            return trade.quote_usd / trade.token_amount
        return None

    def _entry_price(self, trade: Trade, later: List[Trade]) -> Optional[float]:
        """Price the copier's entry: the trader's fill price, unless the
        trader traded the same token again within the latency window —
        then the copier got that later price (they were behind)."""
        price = self._price(trade)
        for nxt in later:
            if nxt.timestamp - trade.timestamp > self.latency_seconds:
                break
            p = self._price(nxt)
            if p is not None:
                price = p
        if price is None:
            return None
        return price * (1 + self.slippage_pct)

    # -- main loop -----------------------------------------------------------

    def run(self, trades: List[Trade]) -> BacktestResult:
        trades = sorted(trades, key=lambda t: t.timestamp)
        cfg = self.config
        risk = RiskManager(cfg)
        # The RiskManager is wall-clock based; neutralize its day-roll and
        # cooldown clocks so replay speed doesn't matter.
        risk._roll_day = lambda: None

        result = BacktestResult(cfg, self.latency_seconds,
                                self.slippage_pct, self.abandoned_recovery)
        open_pos: Dict[str, BacktestPosition] = {}
        # Trader's outstanding token inventory per mint, to compute what
        # fraction of the position each of their sells closes.
        trader_inventory: Dict[str, float] = {}
        by_mint: Dict[str, List[Trade]] = {}
        for t in trades:
            by_mint.setdefault(t.mint, []).append(t)

        def drop(verdict: str):
            result.dropped[verdict] = result.dropped.get(verdict, 0) + 1

        for i, t in enumerate(trades):
            if t.side == TradeSide.BUY:
                trader_inventory[t.mint] = \
                    trader_inventory.get(t.mint, 0) + t.token_amount

                if t.quote_usd < cfg.min_trader_usd:
                    drop("dust")
                    continue
                exposure = sum(p.usd_in - p.usd_out
                               for p in open_pos.values())
                blocked = risk.check_buy(max(exposure, 0), len(open_pos))
                if blocked:
                    drop(blocked.value)
                    continue
                later = [x for x in by_mint[t.mint]
                         if x.timestamp >= t.timestamp and x is not t]
                entry = self._entry_price(t, later)
                if entry is None:
                    drop("unpriced")
                    continue
                copy_usd = min(t.quote_usd * cfg.copy_fraction,
                               cfg.max_position_usd)
                pos = open_pos.get(t.mint)
                if pos is None:
                    pos = open_pos[t.mint] = BacktestPosition(
                        mint=t.mint, opened_ts=t.timestamp)
                elif pos.usd_in - pos.usd_out >= cfg.max_position_usd:
                    drop("position_capped")
                    continue
                pos.usd_in += copy_usd
                pos.tokens += copy_usd / entry
                result.copied_buys += 1

            else:  # trader SELL
                inv = trader_inventory.get(t.mint, 0)
                sell_fraction = (min(t.token_amount / inv, 1.0)
                                 if inv > 0 else 1.0)
                trader_inventory[t.mint] = max(inv - t.token_amount, 0)

                pos = open_pos.get(t.mint)
                if pos is None or pos.tokens <= 0:
                    continue
                exit_price = self._price(t)
                if exit_price is None:
                    continue
                exit_price *= (1 - self.slippage_pct)
                sell_tokens = pos.tokens * sell_fraction
                pos.usd_out += sell_tokens * exit_price
                pos.tokens -= sell_tokens
                if sell_fraction >= 1.0 or pos.tokens < 1e-12:
                    pos.closed = True
                    result.positions.append(open_pos.pop(t.mint))
                    risk.record_close(pos.pnl)

        # Anything the trader never fully exited: the copier is stuck too.
        for mint, pos in open_pos.items():
            pos.usd_out += (pos.usd_in - pos.usd_out) * self.abandoned_recovery
            pos.closed = True
            pos.abandoned = True
            result.positions.append(pos)
        return result

    def sweep(self, trades: List[Trade],
              latencies=(0.0, 5.0, 15.0),
              slippages=(0.0, 0.03, 0.08)) -> List[BacktestResult]:
        """Grid of scenarios from optimistic to harsh."""
        results = []
        for lat in latencies:
            for slip in slippages:
                bt = Backtester(self.config, latency_seconds=lat,
                                slippage_pct=slip,
                                abandoned_recovery=self.abandoned_recovery)
                results.append(bt.run(trades))
        return results


def load_trades_from_files(paths: List[str], wallet: str) -> List[Trade]:
    txs, seen = [], set()
    for path in paths:
        for tx in json.load(open(path)):
            if tx is None:
                continue
            sig = tx["transaction"]["signatures"][0]
            if sig not in seen:
                seen.add(sig)
                txs.append(tx)
    profile = TraderProfiler().profile(wallet, raw_transactions=txs)
    return profile.trades


if __name__ == "__main__":
    wallet = "6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS"
    paths = sys.argv[1:] or ["wallet_data.json"]
    trades = load_trades_from_files(paths, wallet)
    print(f"{len(trades)} trades loaded\n")
    print("Scenario sweep (latency x slippage):")
    for res in Backtester().sweep(trades):
        print(" ", res.report())
