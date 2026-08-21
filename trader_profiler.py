"""
Trader Profiler - On-chain behavioral analysis of a Solana trader wallet.

Fetches a wallet's transaction history from Solana RPC, classifies each
transaction into token BUY/SELL trades, reconstructs per-token round trips,
and produces a TraderProfile: win rate, position sizing, hold times, venue
mix, and activity windows.

Used as the research layer of the trading system: the profile it produces
feeds the rule parameters in copy_signal_engine.

No external dependencies - standard library only (urllib for RPC).
"""

import json
import time
import urllib.request
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Dict, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TraderProfiler")

DEFAULT_RPC = "https://api.mainnet-beta.solana.com"
WSOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS = 1_000_000_000

# DEX / launchpad programs we recognize when attributing venues
KNOWN_PROGRAMS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "pump.fun bonding curve",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "PumpSwap AMM",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM v4",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": "Raydium CPMM",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "Meteora DLMM",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "Jupiter v6",
}


class TradeSide(Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class Trade:
    """A single token buy or sell reconstructed from one transaction."""
    signature: str
    timestamp: int
    mint: str
    side: TradeSide
    token_amount: float
    sol_amount: float          # SOL spent (buy) or received (sell), incl. wSOL
    fee: float
    programs: List[str] = field(default_factory=list)


@dataclass
class RoundTrip:
    """All observed activity in one token: entries, exits, realized PnL."""
    mint: str
    buys: int = 0
    sells: int = 0
    sol_in: float = 0.0
    sol_out: float = 0.0
    first_ts: int = 0
    last_ts: int = 0
    hold_seconds: Optional[int] = None   # first buy -> first subsequent sell

    @property
    def realized_pnl(self) -> float:
        return self.sol_out - self.sol_in

    @property
    def closed(self) -> bool:
        return self.buys > 0 and self.sells > 0


@dataclass
class TraderProfile:
    """Aggregated behavioral statistics for a wallet."""
    wallet: str
    tx_count: int = 0
    span_hours: float = 0.0
    trades: List[Trade] = field(default_factory=list)
    round_trips: List[RoundTrip] = field(default_factory=list)
    venue_counts: Dict[str, int] = field(default_factory=dict)
    hourly_activity: Dict[int, int] = field(default_factory=dict)

    # Derived headline stats (filled by summarize())
    unique_tokens: int = 0
    win_rate: float = 0.0
    total_sol_in: float = 0.0
    net_pnl_sol: float = 0.0
    median_buy_sol: float = 0.0
    median_hold_seconds: Optional[float] = None
    trades_per_hour: float = 0.0

    def summarize(self) -> Dict:
        closed = [r for r in self.round_trips if r.closed and r.sol_in > 0.001]
        wins = [r for r in closed if r.realized_pnl > 0]
        buy_sizes = sorted(t.sol_amount for t in self.trades
                           if t.side == TradeSide.BUY and t.sol_amount > 0)
        holds = sorted(r.hold_seconds for r in closed if r.hold_seconds is not None)

        self.unique_tokens = len(self.round_trips)
        self.win_rate = len(wins) / len(closed) if closed else 0.0
        self.total_sol_in = sum(r.sol_in for r in closed)
        self.net_pnl_sol = sum(r.realized_pnl for r in closed)
        self.median_buy_sol = buy_sizes[len(buy_sizes) // 2] if buy_sizes else 0.0
        self.median_hold_seconds = holds[len(holds) // 2] if holds else None
        self.trades_per_hour = (len(self.trades) / self.span_hours
                                if self.span_hours > 0 else 0.0)
        return {
            "wallet": self.wallet,
            "tx_count": self.tx_count,
            "span_hours": round(self.span_hours, 1),
            "unique_tokens": self.unique_tokens,
            "closed_round_trips": len(closed),
            "win_rate": round(self.win_rate, 3),
            "total_sol_deployed": round(self.total_sol_in, 3),
            "net_realized_pnl_sol": round(self.net_pnl_sol, 3),
            "median_buy_sol": round(self.median_buy_sol, 4),
            "median_hold_seconds": self.median_hold_seconds,
            "trades_per_hour": round(self.trades_per_hour, 2),
            "venues": dict(sorted(self.venue_counts.items(),
                                  key=lambda kv: -kv[1])),
        }


class SolanaRpcClient:
    """Minimal JSON-RPC client with retry/backoff for public endpoints."""

    def __init__(self, endpoint: str = DEFAULT_RPC, throttle_seconds: float = 0.35):
        self.endpoint = endpoint
        self.throttle_seconds = throttle_seconds

    def call(self, method: str, params: list, retries: int = 5) -> Optional[dict]:
        body = json.dumps({"jsonrpc": "2.0", "id": 1,
                           "method": method, "params": params}).encode()
        for attempt in range(retries):
            try:
                req = urllib.request.Request(
                    self.endpoint, data=body,
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.loads(resp.read())
                if "error" in payload:
                    logger.warning("RPC error %s: %s", method, payload["error"])
                    time.sleep(2 ** (attempt + 1))
                    continue
                return payload.get("result")
            except Exception as exc:  # network hiccups on public RPC are routine
                logger.warning("RPC %s failed (%s), retrying", method, exc)
                time.sleep(2 ** (attempt + 1))
        return None

    def get_signatures(self, wallet: str, limit: int = 1000) -> List[dict]:
        sigs, before = [], None
        while len(sigs) < limit:
            page = min(250, limit - len(sigs))
            params = [wallet, {"limit": page}]
            if before:
                params[1]["before"] = before
            batch = self.call("getSignaturesForAddress", params)
            if not batch:
                break
            sigs.extend(batch)
            before = batch[-1]["signature"]
            if len(batch) < page:
                break
            time.sleep(self.throttle_seconds)
        return sigs

    def get_transaction(self, signature: str) -> Optional[dict]:
        result = self.call("getTransaction", [
            signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0},
        ])
        time.sleep(self.throttle_seconds)
        return result


class TraderProfiler:
    """Builds a TraderProfile from raw on-chain history."""

    def __init__(self, rpc: Optional[SolanaRpcClient] = None):
        self.rpc = rpc or SolanaRpcClient()

    def profile(self, wallet: str, max_transactions: int = 200,
                raw_transactions: Optional[List[dict]] = None) -> TraderProfile:
        """Profile a wallet. Pass raw_transactions to skip fetching
        (e.g. when replaying a saved dataset)."""
        if raw_transactions is None:
            sigs = self.rpc.get_signatures(wallet, limit=max_transactions * 2)
            ok = [s["signature"] for s in sigs if s["err"] is None]
            logger.info("Fetching %d transaction details for %s",
                        min(len(ok), max_transactions), wallet)
            raw_transactions = []
            for sig in ok[:max_transactions]:
                tx = self.rpc.get_transaction(sig)
                if tx:
                    raw_transactions.append(tx)

        trades = []
        venue_counts: Dict[str, int] = defaultdict(int)
        hourly: Dict[int, int] = defaultdict(int)
        timestamps = []

        for tx in raw_transactions:
            parsed = self._parse_transaction(wallet, tx)
            if parsed is None:
                continue
            tx_trades, programs, ts = parsed
            trades.extend(tx_trades)
            timestamps.append(ts)
            hourly[datetime.fromtimestamp(ts, tz=timezone.utc).hour] += 1
            for pid in programs:
                if pid in KNOWN_PROGRAMS:
                    venue_counts[KNOWN_PROGRAMS[pid]] += 1

        trades.sort(key=lambda t: t.timestamp)
        profile = TraderProfile(
            wallet=wallet,
            tx_count=len(timestamps),
            span_hours=((max(timestamps) - min(timestamps)) / 3600
                        if len(timestamps) > 1 else 0.0),
            trades=trades,
            round_trips=self._build_round_trips(trades),
            venue_counts=dict(venue_counts),
            hourly_activity=dict(hourly),
        )
        profile.summarize()
        return profile

    def _parse_transaction(self, wallet: str, tx: dict):
        meta = tx.get("meta") or {}
        if meta.get("err") is not None:
            return None
        msg = tx["transaction"]["message"]
        keys = [k["pubkey"] for k in msg["accountKeys"]]
        if wallet not in keys:
            return None
        widx = keys.index(wallet)
        sol_delta = (meta["postBalances"][widx] - meta["preBalances"][widx]) / LAMPORTS
        fee = meta.get("fee", 0) / LAMPORTS
        ts = tx.get("blockTime") or 0

        programs = set()
        for ins in msg.get("instructions", []):
            if ins.get("programId"):
                programs.add(ins["programId"])
        for inner in meta.get("innerInstructions") or []:
            for ins in inner.get("instructions", []):
                if ins.get("programId"):
                    programs.add(ins["programId"])

        def owned_balances(balances):
            out: Dict[str, float] = defaultdict(float)
            for b in balances or []:
                if b.get("owner") == wallet:
                    out[b["mint"]] += float(b["uiTokenAmount"]["uiAmount"] or 0)
            return out

        pre = owned_balances(meta.get("preTokenBalances"))
        post = owned_balances(meta.get("postTokenBalances"))
        wsol_delta = post.get(WSOL_MINT, 0) - pre.get(WSOL_MINT, 0)
        sol_flow = sol_delta + wsol_delta

        trades = []
        for mint in set(pre) | set(post):
            if mint == WSOL_MINT:
                continue
            delta = post.get(mint, 0) - pre.get(mint, 0)
            if abs(delta) < 1e-9:
                continue
            side = TradeSide.BUY if delta > 0 else TradeSide.SELL
            # SOL spent on a buy shows as negative flow; received on sell positive
            sol_amount = -sol_flow if side == TradeSide.BUY else sol_flow
            trades.append(Trade(
                signature=tx["transaction"]["signatures"][0],
                timestamp=ts, mint=mint, side=side,
                token_amount=abs(delta),
                sol_amount=max(sol_amount, 0.0),
                fee=fee, programs=sorted(programs),
            ))
        return trades, programs, ts

    @staticmethod
    def _build_round_trips(trades: List[Trade]) -> List[RoundTrip]:
        by_mint: Dict[str, List[Trade]] = defaultdict(list)
        for t in trades:
            by_mint[t.mint].append(t)

        round_trips = []
        for mint, mint_trades in by_mint.items():
            mint_trades.sort(key=lambda t: t.timestamp)
            rt = RoundTrip(mint=mint,
                           first_ts=mint_trades[0].timestamp,
                           last_ts=mint_trades[-1].timestamp)
            for t in mint_trades:
                if t.side == TradeSide.BUY:
                    rt.buys += 1
                    rt.sol_in += t.sol_amount
                else:
                    rt.sells += 1
                    rt.sol_out += t.sol_amount
            first_buy = next((t.timestamp for t in mint_trades
                              if t.side == TradeSide.BUY), None)
            if first_buy is not None:
                first_sell = next((t.timestamp for t in mint_trades
                                   if t.side == TradeSide.SELL
                                   and t.timestamp >= first_buy), None)
                if first_sell is not None:
                    rt.hold_seconds = first_sell - first_buy
            round_trips.append(rt)
        return round_trips


_profiler: Optional[TraderProfiler] = None


def get_trader_profiler() -> TraderProfiler:
    global _profiler
    if _profiler is None:
        _profiler = TraderProfiler()
    return _profiler
