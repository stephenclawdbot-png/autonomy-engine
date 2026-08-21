"""
Copy Signal Engine - Real-time mirror-trading system for a tracked wallet.

Polls a target trader's wallet for new transactions, converts them into
BUY/SELL signals, and runs them through a risk pipeline before emitting:

    WalletWatcher -> SignalFilter -> RiskManager -> PaperBook / callback

Design notes (derived from profiling wallet 6SHqkz..3obS, see
TRADER_ANALYSIS.md):
- The tracked trader is a high-frequency memecoin scalper. Copying requires
  entering within seconds, so the watcher polls aggressively and drops
  signals that are already stale.
- Most of this style's edge dies in the copy latency. The engine therefore
  defaults to PAPER mode; live execution is deliberately out of scope and
  must be wired in by the operator behind their own execution layer.
- Risk pipeline mirrors the repo's resilience philosophy: a CircuitBreaker
  halts signal emission after a losing streak instead of averaging into a
  drawdown.

No external dependencies - standard library only.
"""

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Deque, Dict, List, Optional
import logging

from trader_profiler import SolanaRpcClient, TraderProfiler, TradeSide, Trade

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CopySignalEngine")


class SignalAction(Enum):
    BUY = "buy"
    SELL = "sell"


class SignalVerdict(Enum):
    EMITTED = "emitted"
    STALE = "stale"                  # too old to copy profitably
    DUST = "dust"                    # trader's size too small to be conviction
    RISK_HALTED = "risk_halted"      # loss-streak breaker is open
    EXPOSURE_CAPPED = "exposure_capped"
    DAILY_LOSS_LIMIT = "daily_loss_limit"


@dataclass
class SignalConfig:
    """Tunables. Defaults calibrated from the tracked trader's measured
    profile (see TRADER_ANALYSIS.md): median buy $500 (p25 $100 / p75
    $1000), median hold ~7 min (p75 ~17 min), win rate ~39%, and two
    account-scale blowups caused by averaging down past any cap."""
    poll_seconds: float = 2.0
    max_signal_age_seconds: float = 20.0   # scalper edge decays in seconds
    min_trader_usd: float = 100.0          # below trader's p25 = probe, skip
    copy_fraction: float = 0.10            # copy at 10% of trader's size...
    max_position_usd: float = 100.0        # ...capped per position
    max_concurrent_positions: int = 3
    max_total_exposure_usd: float = 250.0
    daily_loss_limit_usd: float = 100.0    # hard stop for the day
    loss_streak_halt: int = 5              # at 35% WR, 5 losses = cold streak
    halt_cooldown_seconds: float = 1800.0  # 30 min timeout after halt
    stop_loss_pct: float = 0.40            # exit if down 40% and no trader sell
    max_hold_seconds: float = 3600.0       # time-stop: p75 hold is ~18 min


@dataclass
class Signal:
    action: SignalAction
    mint: str
    trader_usd: float
    copy_usd: float
    timestamp: float
    source_signature: str
    verdict: SignalVerdict = SignalVerdict.EMITTED
    reason: str = ""


@dataclass
class PaperPosition:
    mint: str
    usd_spent: float
    opened_at: float
    token_amount: float = 0.0
    usd_returned: float = 0.0
    closed: bool = False

    @property
    def pnl(self) -> float:
        return self.usd_returned - self.usd_spent


class RiskManager:
    """Loss-streak breaker + exposure and daily-loss caps.

    Same philosophy as circuit_breaker.CircuitBreaker but tracks realized
    PnL rather than call failures: after `loss_streak_halt` consecutive
    losing round trips, or once the daily loss limit is hit, no new BUY
    signals are emitted until cooldown / next UTC day.
    """

    def __init__(self, config: SignalConfig):
        self.config = config
        self.consecutive_losses = 0
        self.halted_until: float = 0.0
        self.daily_pnl: float = 0.0
        self._day_start: float = time.time()

    def _roll_day(self) -> None:
        if time.time() - self._day_start >= 86400:
            self._day_start = time.time()
            self.daily_pnl = 0.0

    def record_close(self, pnl: float) -> None:
        self._roll_day()
        self.daily_pnl += pnl
        if pnl < 0:
            self.consecutive_losses += 1
            if self.consecutive_losses >= self.config.loss_streak_halt:
                self.halted_until = time.time() + self.config.halt_cooldown_seconds
                logger.warning("RiskManager HALT: %d consecutive losses, "
                               "cooling down %.0fs", self.consecutive_losses,
                               self.config.halt_cooldown_seconds)
        else:
            self.consecutive_losses = 0

    def check_buy(self, open_exposure: float, open_positions: int) -> Optional[SignalVerdict]:
        self._roll_day()
        if time.time() < self.halted_until:
            return SignalVerdict.RISK_HALTED
        if self.daily_pnl <= -self.config.daily_loss_limit_usd:
            return SignalVerdict.DAILY_LOSS_LIMIT
        if (open_positions >= self.config.max_concurrent_positions
                or open_exposure >= self.config.max_total_exposure_usd):
            return SignalVerdict.EXPOSURE_CAPPED
        return None


class PaperBook:
    """Tracks simulated copy positions and realized PnL."""

    def __init__(self, risk: RiskManager):
        self.risk = risk
        self.positions: Dict[str, PaperPosition] = {}
        self.closed: List[PaperPosition] = []

    @property
    def open_exposure(self) -> float:
        return sum(p.usd_spent for p in self.positions.values())

    def open(self, signal: Signal) -> None:
        pos = self.positions.get(signal.mint)
        if pos:
            pos.usd_spent += signal.copy_usd
        else:
            self.positions[signal.mint] = PaperPosition(
                mint=signal.mint, usd_spent=signal.copy_usd,
                opened_at=signal.timestamp)

    def close(self, mint: str, proceeds_ratio: float = 1.0) -> Optional[float]:
        """Close a position. proceeds_ratio approximates exit quality:
        1.0 = exited at the same multiple the trader did (paper idealization)."""
        pos = self.positions.pop(mint, None)
        if pos is None:
            return None
        pos.usd_returned = pos.usd_spent * proceeds_ratio
        pos.closed = True
        self.closed.append(pos)
        self.risk.record_close(pos.pnl)
        return pos.pnl

    def summary(self) -> Dict:
        realized = sum(p.pnl for p in self.closed)
        wins = sum(1 for p in self.closed if p.pnl > 0)
        return {
            "open_positions": len(self.positions),
            "open_exposure_usd": round(self.open_exposure, 2),
            "closed_trades": len(self.closed),
            "wins": wins,
            "realized_pnl_usd": round(realized, 2),
            "daily_pnl_usd": round(self.risk.daily_pnl, 2),
            "risk_halted": time.time() < self.risk.halted_until,
        }


class WalletWatcher:
    """Polls the target wallet and converts fresh transactions to Signals."""

    def __init__(self, wallet: str, config: Optional[SignalConfig] = None,
                 rpc: Optional[SolanaRpcClient] = None,
                 on_signal: Optional[Callable[[Signal], None]] = None):
        self.wallet = wallet
        self.config = config or SignalConfig()
        self.rpc = rpc or SolanaRpcClient(throttle_seconds=0.0)
        self.profiler = TraderProfiler(self.rpc)
        self.on_signal = on_signal
        self.risk = RiskManager(self.config)
        self.book = PaperBook(self.risk)
        self.seen_signatures: Deque[str] = deque(maxlen=2000)
        self._seen_set: set = set()
        self._stop = threading.Event()
        self.signal_log: List[Signal] = []

    # -- signal construction -------------------------------------------------

    def _evaluate(self, trade: Trade) -> Signal:
        cfg = self.config
        action = (SignalAction.BUY if trade.side == TradeSide.BUY
                  else SignalAction.SELL)
        copy_usd = min(trade.quote_usd * cfg.copy_fraction, cfg.max_position_usd)
        signal = Signal(action=action, mint=trade.mint,
                        trader_usd=trade.quote_usd, copy_usd=copy_usd,
                        timestamp=time.time(),
                        source_signature=trade.signature)

        age = time.time() - trade.timestamp
        if age > cfg.max_signal_age_seconds:
            signal.verdict = SignalVerdict.STALE
            signal.reason = f"signal {age:.0f}s old"
            return signal

        if action == SignalAction.BUY:
            if trade.quote_usd < cfg.min_trader_usd:
                signal.verdict = SignalVerdict.DUST
                signal.reason = f"trader size ${trade.quote_usd:.2f} below floor"
                return signal
            blocked = self.risk.check_buy(self.book.open_exposure,
                                          len(self.book.positions))
            if blocked:
                signal.verdict = blocked
                return signal
        else:
            # Always honor the trader's exits for tokens we hold; ignore
            # sells for tokens we never entered.
            if trade.mint not in self.book.positions:
                signal.verdict = SignalVerdict.DUST
                signal.reason = "no matching position"
                return signal
        return signal

    def _apply(self, signal: Signal) -> None:
        self.signal_log.append(signal)
        if signal.verdict != SignalVerdict.EMITTED:
            logger.info("Signal dropped (%s): %s %s $%.2f %s",
                        signal.verdict.value, signal.action.value,
                        signal.mint[:8], signal.trader_usd, signal.reason)
            return
        if signal.action == SignalAction.BUY:
            self.book.open(signal)
        else:
            self.book.close(signal.mint)
        logger.info("SIGNAL %s %s copy=$%.2f (trader $%.2f) | book: %s",
                    signal.action.value.upper(), signal.mint[:8],
                    signal.copy_usd, signal.trader_usd, self.book.summary())
        if self.on_signal:
            self.on_signal(signal)

    # -- time-based exits (stop loss handled by execution layer in live mode)

    def enforce_time_stops(self) -> None:
        now = time.time()
        expired = [m for m, p in self.book.positions.items()
                   if now - p.opened_at > self.config.max_hold_seconds]
        for mint in expired:
            logger.info("Time-stop exit on %s after %.0fs",
                        mint[:8], self.config.max_hold_seconds)
            self.book.close(mint)

    # -- polling loop --------------------------------------------------------

    def poll_once(self) -> int:
        """One poll cycle. Returns number of new transactions processed."""
        sigs = self.rpc.call("getSignaturesForAddress",
                             [self.wallet, {"limit": 15}]) or []
        fresh = [s for s in sigs
                 if s["err"] is None and s["signature"] not in self._seen_set]
        # Oldest first so buys precede their sells
        for entry in reversed(fresh):
            self._seen_set.add(entry["signature"])
            self.seen_signatures.append(entry["signature"])
            while len(self._seen_set) > len(self.seen_signatures):
                self._seen_set = set(self.seen_signatures)
            tx = self.rpc.get_transaction(entry["signature"])
            if tx is None:
                continue
            parsed = self.profiler._parse_transaction(self.wallet, tx)
            if parsed is None:
                continue
            trades, _programs, _ts = parsed
            for trade in trades:
                self._apply(self._evaluate(trade))
        self.enforce_time_stops()
        return len(fresh)

    def run(self, duration_seconds: Optional[float] = None) -> None:
        """Blocking poll loop. Call stop() from another thread to end."""
        started = time.time()
        # Seed seen-set so we don't replay history as live signals
        for s in (self.rpc.call("getSignaturesForAddress",
                                [self.wallet, {"limit": 50}]) or []):
            self._seen_set.add(s["signature"])
            self.seen_signatures.append(s["signature"])
        logger.info("Watching %s (poll every %.1fs, paper mode)",
                    self.wallet, self.config.poll_seconds)
        while not self._stop.is_set():
            if duration_seconds and time.time() - started > duration_seconds:
                break
            try:
                self.poll_once()
            except Exception as exc:
                logger.warning("poll failed: %s", exc)
            self._stop.wait(self.config.poll_seconds)
        logger.info("Watcher stopped. Final book: %s", self.book.summary())

    def stop(self) -> None:
        self._stop.set()


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else \
        "6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS"
    watcher = WalletWatcher(target)
    try:
        watcher.run()
    except KeyboardInterrupt:
        watcher.stop()
