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
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
LAMPORTS = 1_000_000_000

# Mints treated as the "cash" side of a swap rather than a position.
# Many Solana memecoin traders quote in USDC (via aggregators like DFlow),
# not SOL - PnL is invisible unless stables count as quote currency.
QUOTE_MINTS = {WSOL_MINT: "SOL", USDC_MINT: "USDC", USDT_MINT: "USDT"}
DEFAULT_SOL_PRICE_USD = 185.0  # estimate used to merge SOL+stable flows

# DEX / launchpad programs we recognize when attributing venues
KNOWN_PROGRAMS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P": "pump.fun bonding curve",
    "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA": "PumpSwap AMM",
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": "Raydium AMM v4",
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": "Raydium CPMM",
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": "Raydium CLMM",
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo": "Meteora DLMM",
    "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4": "Jupiter v6",
    "DF1ow4tspfHX9JwWJsAb9epbkA8hmpSEAtxXy1V27QBH": "DFlow aggregator",
    "99vQwtBwYtrqqD9YSXbdum3KBdxPAVxYTaQ3cfnJSrN2": "DFlow swap router",
    "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc": "Orca Whirlpool",
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
    quote_usd: float           # USD value of the quote leg (SOL + stables)
    fee: float
    programs: List[str] = field(default_factory=list)


@dataclass
class RoundTrip:
    """All observed activity in one token: entries, exits, realized PnL."""
    mint: str
    buys: int = 0
    sells: int = 0
    usd_in: float = 0.0
    usd_out: float = 0.0
    tokens_bought: float = 0.0
    tokens_sold: float = 0.0
    first_ts: int = 0
    last_ts: int = 0
    hold_seconds: Optional[int] = None   # first buy -> first subsequent sell

    @property
    def realized_pnl(self) -> float:
        return self.usd_out - self.usd_in

    @property
    def closed(self) -> bool:
        return self.buys > 0 and self.sells > 0

    @property
    def complete(self) -> bool:
        """Both legs observed AND position not opened before the window
        (sells don't exceed observed buys)."""
        return self.closed and self.tokens_sold <= self.tokens_bought * 1.05


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
    total_usd_in: float = 0.0
    net_pnl_usd: float = 0.0
    median_buy_usd: float = 0.0
    median_hold_seconds: Optional[float] = None
    trades_per_hour: float = 0.0

    def summarize(self) -> Dict:
        complete = [r for r in self.round_trips if r.complete and r.usd_in > 5]
        wins = [r for r in complete if r.realized_pnl > 0]
        buy_sizes = sorted(t.quote_usd for t in self.trades
                           if t.side == TradeSide.BUY and t.quote_usd > 0.5)
        holds = sorted(r.hold_seconds for r in complete
                       if r.hold_seconds is not None)

        self.unique_tokens = len(self.round_trips)
        self.win_rate = len(wins) / len(complete) if complete else 0.0
        self.total_usd_in = sum(r.usd_in for r in complete)
        self.net_pnl_usd = sum(r.realized_pnl for r in complete)
        self.median_buy_usd = buy_sizes[len(buy_sizes) // 2] if buy_sizes else 0.0
        self.median_hold_seconds = holds[len(holds) // 2] if holds else None
        self.trades_per_hour = (len(self.trades) / self.span_hours
                                if self.span_hours > 0 else 0.0)
        return {
            "wallet": self.wallet,
            "tx_count": self.tx_count,
            "span_hours": round(self.span_hours, 1),
            "unique_tokens": self.unique_tokens,
            "complete_round_trips": len(complete),
            "win_rate": round(self.win_rate, 3),
            "total_usd_deployed": round(self.total_usd_in, 2),
            "net_realized_pnl_usd": round(self.net_pnl_usd, 2),
            "median_buy_usd": round(self.median_buy_usd, 2),
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

    def __init__(self, rpc: Optional[SolanaRpcClient] = None,
                 sol_price_usd: float = DEFAULT_SOL_PRICE_USD):
        self.rpc = rpc or SolanaRpcClient()
        self.sol_price_usd = sol_price_usd

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

        def quote_delta(mint):
            return post.get(mint, 0) - pre.get(mint, 0)

        # Combined USD flow across all quote legs (native SOL, wSOL, stables)
        usd_flow = (sol_delta + quote_delta(WSOL_MINT)) * self.sol_price_usd
        usd_flow += quote_delta(USDC_MINT) + quote_delta(USDT_MINT)

        trades = []
        for mint in set(pre) | set(post):
            if mint in QUOTE_MINTS:
                continue
            delta = post.get(mint, 0) - pre.get(mint, 0)
            if abs(delta) < 1e-9:
                continue
            side = TradeSide.BUY if delta > 0 else TradeSide.SELL
            # Quote spent on a buy shows as negative flow; received on sell
            # positive. abs() also covers token->token routes where the
            # quote leg nets near zero.
            trades.append(Trade(
                signature=tx["transaction"]["signatures"][0],
                timestamp=ts, mint=mint, side=side,
                token_amount=abs(delta),
                quote_usd=abs(usd_flow),
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
                    rt.usd_in += t.quote_usd
                    rt.tokens_bought += t.token_amount
                else:
                    rt.sells += 1
                    rt.usd_out += t.quote_usd
                    rt.tokens_sold += t.token_amount
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
