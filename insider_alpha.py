"""
Insider Alpha - Cross-chain insider-movement analysis for FOMO wallets.

The identity layer links a FOMO trader's Solana wallet to their EVM
wallet. That linkage is what makes cross-chain insider analysis
possible: you can watch the whole person, not one address.

This analyzer looks for the timing fingerprints of informational edge —
patterns that are legal to observe from public chain data and are the
bread and butter of on-chain intelligence tools (Nansen/Arkham-style),
NOT a way to deanonymize or target private individuals. Every input is
public: on-chain fills and the trader's own published thesis posts.

Signals scored (each 0..1, with the evidence that produced it):

  1. book_talking      Bought a token BEFORE posting a bullish thesis on
                       it, then sold into the volume the post drew. The
                       classic "talk your book" / front-run-your-own-call
                       pattern.
  2. early_entry       Enters tokens well before the crowd — measured as
                       lead time between their first buy and the token's
                       first thesis coverage / broad activity.
  3. sell_into_call    Distributes (net sells) within a short window AFTER
                       their own post, i.e. the audience is exit liquidity.
  4. cross_chain_sync  Correlated activity on both of the person's chains
                       close in time — bridging ahead of a move, or the
                       same play run on two venues.

Output is an InsiderReport with a blended 0..100 score, a verdict band,
and per-signal evidence so a human can judge it. A high score is a lead
to investigate, never a verdict.

The Solana + thesis-timing analysis is fully functional against the
identity API and public RPC. EVM activity is pulled through a pluggable
EvmActivitySource (implement fetch() with your Etherscan/Basescan key or
an RPC); a NullEvmSource ships so the analyzer runs without one.

No external dependencies - standard library only.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol
import logging

from trader_profiler import TraderProfiler, TradeSide, Trade
from fomoscan_client import FomoScanClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("InsiderAlpha")

# Timing windows (seconds)
PRE_POST_ACCUMULATION_WINDOW = 24 * 3600     # bought within 24h before post
POST_DISTRIBUTION_WINDOW = 6 * 3600          # sold within 6h after post
CROSS_CHAIN_SYNC_WINDOW = 3600               # activity within 1h across chains


@dataclass
class EvmEvent:
    """A minimal cross-chain activity record from an EVM source."""
    timestamp: int
    token_symbol: Optional[str]
    token_address: Optional[str]
    direction: str          # "in" | "out"
    usd_value: float = 0.0


class EvmActivitySource(Protocol):
    """Plug in Etherscan/Basescan/Alchemy here. Return newest-first is fine."""
    def fetch(self, evm_address: str, limit: int = 100) -> List[EvmEvent]: ...


class NullEvmSource:
    """Default: no EVM data. Cross-chain signal is skipped, not faked."""
    def fetch(self, evm_address: str, limit: int = 100) -> List[EvmEvent]:
        return []


@dataclass
class SignalScore:
    name: str
    score: float                       # 0..1
    weight: float
    evidence: List[str] = field(default_factory=list)


@dataclass
class InsiderReport:
    handle: Optional[str]
    user_id: Optional[str]
    solana_address: Optional[str]
    evm_address: Optional[str]
    signals: List[SignalScore] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)

    @property
    def score(self) -> float:
        """Weighted 0..100 blend over the signals that could be computed."""
        active = [s for s in self.signals if s.weight > 0]
        wsum = sum(s.weight for s in active)
        if wsum == 0:
            return 0.0
        return round(100 * sum(s.score * s.weight for s in active) / wsum, 1)

    @property
    def verdict(self) -> str:
        s = self.score
        if s >= 70:
            return "STRONG — multiple insider-timing patterns align"
        if s >= 45:
            return "ELEVATED — worth investigating"
        if s >= 20:
            return "MILD — some timing edge, likely skill not information"
        return "LOW — no clear insider-timing pattern"

    def render(self) -> str:
        lines = [f"Insider-alpha report: "
                 f"@{self.handle or '?'} ({self.user_id or 'no id'})",
                 f"  SOL: {self.solana_address or '—'}",
                 f"  EVM: {self.evm_address or '—'}",
                 f"  SCORE {self.score}/100 — {self.verdict}", ""]
        for s in self.signals:
            state = f"{s.score:.2f}" if s.weight > 0 else "n/a"
            lines.append(f"  [{state}] {s.name} (w={s.weight})")
            for ev in s.evidence[:4]:
                lines.append(f"       • {ev}")
        for n in self.notes:
            lines.append(f"  note: {n}")
        return "\n".join(lines)


class InsiderAlphaAnalyzer:
    """Scores cross-chain insider-timing signals for one FOMO wallet."""

    def __init__(self, identity: Optional[FomoScanClient] = None,
                 profiler: Optional[TraderProfiler] = None,
                 evm_source: Optional[EvmActivitySource] = None):
        self.identity = identity
        self.profiler = profiler or TraderProfiler()
        self.evm_source = evm_source or NullEvmSource()

    # -- identity resolution -------------------------------------------------

    def _resolve(self, handle_or_wallet: str) -> dict:
        """Resolve to a FOMO user record via the identity API when
        available; otherwise treat the input as a bare Solana address."""
        if self.identity is None:
            return {"handle": None, "id": None,
                    "solanaAddress": handle_or_wallet, "evmAddress": None}
        if handle_or_wallet.startswith("0x") or len(handle_or_wallet) >= 32:
            user = self.identity.user_by_wallet(handle_or_wallet)
        else:
            user = self.identity.user_by_handle(handle_or_wallet)
        if user is None:
            return {"handle": None, "id": None,
                    "solanaAddress": handle_or_wallet
                    if not handle_or_wallet.startswith("0x") else None,
                    "evmAddress": handle_or_wallet
                    if handle_or_wallet.startswith("0x") else None}
        return user

    def _thesis_times(self, user_id: str) -> Dict[str, List[int]]:
        """token_address -> sorted post times (epoch seconds) by this author."""
        out: Dict[str, List[int]] = {}
        if self.identity is None or user_id is None:
            return out
        try:
            before = None
            for _ in range(5):                       # up to 5 pages
                page = self.identity.theses_by_user(user_id, before=before)
                for item in page.get("items", []):
                    tok = item.get("tokenAddress")
                    if not tok:
                        continue
                    ts = int((item.get("fomoCreatedAt") or 0) / 1000)
                    out.setdefault(tok, []).append(ts)
                if not page.get("hasMore") or not page.get("nextBefore"):
                    break
                before = page["nextBefore"]
        except Exception as exc:
            logger.warning("thesis fetch failed: %s", exc)
        for tok in out:
            out[tok].sort()
        return out

    # -- signal scorers ------------------------------------------------------

    @staticmethod
    def _sig_book_talking(trades_by_mint, thesis_times) -> SignalScore:
        """Bought before posting, on tokens they later called."""
        hits, checked = 0, 0
        evidence = []
        for tok, posts in thesis_times.items():
            trades = trades_by_mint.get(tok)
            if not trades or not posts:
                continue
            checked += 1
            first_post = posts[0]
            pre_buys = [t for t in trades if t.side == TradeSide.BUY
                        and 0 <= first_post - t.timestamp
                        <= PRE_POST_ACCUMULATION_WINDOW]
            if pre_buys:
                hits += 1
                lead = (first_post - min(t.timestamp for t in pre_buys)) / 3600
                usd = sum(t.quote_usd for t in pre_buys)
                evidence.append(
                    f"{tok[:8]}…: {len(pre_buys)} buy(s) ${usd:,.0f} up to "
                    f"{lead:.1f}h before first thesis post")
        weight = 3.0 if checked else 0.0
        score = hits / checked if checked else 0.0
        if not checked:
            evidence.append("no tokens with both fills and this author's theses")
        return SignalScore("book_talking", score, weight, evidence)

    @staticmethod
    def _sig_sell_into_call(trades_by_mint, thesis_times) -> SignalScore:
        """Net sells shortly after their own post — audience as exit liquidity."""
        hits, checked = 0, 0
        evidence = []
        for tok, posts in thesis_times.items():
            trades = trades_by_mint.get(tok)
            if not trades or not posts:
                continue
            checked += 1
            for post in posts:
                sells = [t for t in trades if t.side == TradeSide.SELL
                         and 0 <= t.timestamp - post
                         <= POST_DISTRIBUTION_WINDOW]
                if sells:
                    usd = sum(t.quote_usd for t in sells)
                    hits += 1
                    evidence.append(
                        f"{tok[:8]}…: sold ${usd:,.0f} within "
                        f"{POST_DISTRIBUTION_WINDOW // 3600}h after a post")
                    break
        weight = 2.0 if checked else 0.0
        score = hits / checked if checked else 0.0
        return SignalScore("sell_into_call", score, weight, evidence)

    @staticmethod
    def _sig_early_entry(profile, thesis_times) -> SignalScore:
        """How early first buys land vs the token's first thesis coverage —
        a proxy for entering before the crowd forms."""
        leads_h = []
        evidence = []
        by_mint = {}
        for t in profile.trades:
            by_mint.setdefault(t.mint, []).append(t)
        for tok, posts in thesis_times.items():
            trades = by_mint.get(tok)
            if not trades or not posts:
                continue
            first_buy = min((t.timestamp for t in trades
                             if t.side == TradeSide.BUY), default=None)
            if first_buy is None:
                continue
            lead_h = (posts[0] - first_buy) / 3600
            leads_h.append(lead_h)
            if lead_h > 0:
                evidence.append(f"{tok[:8]}…: entered {lead_h:.1f}h before "
                                "first thesis coverage")
        if not leads_h:
            return SignalScore("early_entry", 0.0, 0.0,
                               ["no token had both a first buy and a post"])
        # Map median lead time to 0..1: 0h -> 0, >=24h ahead -> 1.
        median = sorted(leads_h)[len(leads_h) // 2]
        score = max(0.0, min(1.0, median / 24.0))
        evidence.insert(0, f"median entry lead {median:.1f}h over "
                           f"{len(leads_h)} called tokens")
        return SignalScore("early_entry", score, 2.0, evidence)

    def _sig_cross_chain(self, evm_address, profile) -> SignalScore:
        """Correlated timing across the person's two chains."""
        if not evm_address:
            return SignalScore("cross_chain_sync", 0.0, 0.0,
                               ["no linked EVM wallet on record"])
        events = self.evm_source.fetch(evm_address)
        if not events:
            return SignalScore("cross_chain_sync", 0.0, 0.0,
                               ["EVM wallet on record but no EVM source "
                                "configured (plug in EvmActivitySource)"])
        sol_times = sorted(t.timestamp for t in profile.trades)
        syncs = 0
        evidence = []
        for ev in events:
            near = [s for s in sol_times
                    if abs(s - ev.timestamp) <= CROSS_CHAIN_SYNC_WINDOW]
            if near:
                syncs += 1
                if len(evidence) < 4:
                    evidence.append(
                        f"EVM {ev.direction} {ev.token_symbol or '?'} "
                        f"within {CROSS_CHAIN_SYNC_WINDOW // 60}m of a "
                        "Solana trade")
        score = min(1.0, syncs / 5.0)      # 5+ correlated events -> max
        evidence.insert(0, f"{syncs} cross-chain events within "
                           f"{CROSS_CHAIN_SYNC_WINDOW // 60}m of Solana trades")
        return SignalScore("cross_chain_sync", score, 2.0, evidence)

    # -- public API ----------------------------------------------------------

    def analyze(self, handle_or_wallet: str,
                max_transactions: int = 150) -> InsiderReport:
        user = self._resolve(handle_or_wallet)
        report = InsiderReport(
            handle=user.get("handle"), user_id=user.get("id"),
            solana_address=user.get("solanaAddress"),
            evm_address=user.get("evmAddress"))

        if not report.solana_address:
            report.notes.append("no Solana address to profile; "
                                "Solana-side signals skipped")
            profile = None
            trades_by_mint = {}
        else:
            profile = self.profiler.profile(report.solana_address,
                                            max_transactions=max_transactions)
            trades_by_mint = {}
            for t in profile.trades:
                trades_by_mint.setdefault(t.mint, []).append(t)

        thesis_times = self._thesis_times(report.user_id)
        if not thesis_times:
            report.notes.append("no thesis posts for this author via the "
                                "identity API — book-talking / call-timing "
                                "signals cannot be computed")

        if profile is not None:
            report.signals.append(
                self._sig_book_talking(trades_by_mint, thesis_times))
            report.signals.append(
                self._sig_sell_into_call(trades_by_mint, thesis_times))
            report.signals.append(
                self._sig_early_entry(profile, thesis_times))
            report.signals.append(
                self._sig_cross_chain(report.evm_address, profile))
        return report


if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else \
        "6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS"
    api = None
    import os
    if os.environ.get("IDENTITY_API_URL") and os.environ.get("IDENTITY_API_KEY"):
        api = FomoScanClient(api_key=os.environ["IDENTITY_API_KEY"],
                             base_url=os.environ["IDENTITY_API_URL"])
    report = InsiderAlphaAnalyzer(identity=api).analyze(target)
    print(report.render())
