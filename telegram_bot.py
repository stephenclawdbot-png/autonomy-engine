"""
Telegram Bot - Command center for the self-hosted identity + trading platform.

FomoScan's Telegram bot only hands out API keys by hand. This one fronts
the whole platform in this repo:

  Identity (identity_api_server / FomoScan-compatible API)
    /key                 self-serve API key, one per chat
    /usage               CU balance and period usage
    /lookup <handle>     handle -> user record + verified wallets
    /wallet <address>    wallet -> the trader behind it
    /thesis <token>      latest thesis posts for a token

  Trading (trader_profiler + copy_signal_engine)
    /profile <wallet>    on-chain behavioral profile: win rate, sizing,
                         hold times, venues (public Solana RPC)
    /watch <wallet>      live copy-signal alerts pushed to this chat
                         (paper mode; full risk pipeline applies)
    /unwatch <wallet>    stop watching
    /watches             list active watchers + paper book state

  Admin (chat ids in TELEGRAM_ADMIN_CHAT_IDS)
    /ingest <json>       add a verified user record to the identity DB
    /mintkey <cap>       mint an API key with a custom monthly cap

Configuration (environment):
    TELEGRAM_BOT_TOKEN        from @BotFather (required to run live)
    IDENTITY_API_URL          default http://127.0.0.1:8080
    IDENTITY_DB               default identity_api.db (key minting)
    TELEGRAM_ADMIN_CHAT_IDS   comma-separated chat ids

Run:  python telegram_bot.py

No external dependencies - standard library only (long polling, no
webhooks, so it runs anywhere with outbound HTTPS).
"""

import json
import os
import secrets
import threading
import time
import urllib.request
import urllib.parse
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import logging

from fomoscan_client import FomoScanClient, FomoScanError
from trader_profiler import TraderProfiler
from copy_signal_engine import WalletWatcher, Signal, SignalConfig
from wallet_stream import StreamingWalletWatcher
from backtester import Backtester
from insider_alpha import InsiderAlphaAnalyzer
from identity_api_server import get_db, SCHEMA

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TelegramBot")

TELEGRAM_API = "https://api.telegram.org"


class TelegramTransport:
    """Thin wrapper over the Bot API. Swappable for tests."""

    def __init__(self, token: str):
        self.base = f"{TELEGRAM_API}/bot{token}"

    def call(self, method: str, **params) -> dict:
        data = urllib.parse.urlencode(
            {k: v for k, v in params.items() if v is not None}).encode()
        req = urllib.request.Request(f"{self.base}/{method}", data=data)
        with urllib.request.urlopen(req, timeout=65) as resp:
            return json.loads(resp.read())

    def send(self, chat_id: int, text: str) -> None:
        # Telegram hard-limits messages to 4096 chars
        for i in range(0, len(text), 4000):
            self.call("sendMessage", chat_id=chat_id, text=text[i:i + 4000],
                      parse_mode="HTML", disable_web_page_preview=True)

    def get_updates(self, offset: Optional[int]) -> List[dict]:
        out = self.call("getUpdates", offset=offset, timeout=50,
                        allowed_updates='["message"]')
        return out.get("result", [])


@dataclass
class ChatState:
    api_key: Optional[str] = None
    watchers: Dict[str, WalletWatcher] = field(default_factory=dict)
    threads: Dict[str, threading.Thread] = field(default_factory=dict)


class PlatformBot:
    """Command dispatcher. All handlers return the reply text, so the
    whole bot is testable without Telegram (see selftest())."""

    def __init__(self, transport: Optional[TelegramTransport] = None,
                 api_url: Optional[str] = None,
                 db_path: Optional[str] = None,
                 admin_chat_ids: Optional[List[int]] = None):
        self.transport = transport
        self.api_url = (api_url or os.environ.get(
            "IDENTITY_API_URL", "http://127.0.0.1:8080")).rstrip("/")
        self.db_path = db_path or os.environ.get("IDENTITY_DB",
                                                 "identity_api.db")
        self.admin_chat_ids = admin_chat_ids if admin_chat_ids is not None \
            else [int(x) for x in os.environ.get(
                "TELEGRAM_ADMIN_CHAT_IDS", "").split(",") if x.strip()]
        self.chats: Dict[int, ChatState] = {}
        self.profiler = TraderProfiler()

    # -- helpers -------------------------------------------------------------

    def _state(self, chat_id: int) -> ChatState:
        return self.chats.setdefault(chat_id, ChatState())

    def _client(self, chat_id: int) -> Optional[FomoScanClient]:
        state = self._state(chat_id)
        if not state.api_key:
            return None
        return FomoScanClient(api_key=state.api_key, base_url=self.api_url)

    def _fmt_user(self, u: dict) -> str:
        lines = [f"<b>@{u['handle']}</b>" +
                 (f" — {u['name']}" if u.get("name") else "")]
        if u.get("bio"):
            lines.append(f"<i>{u['bio']}</i>")
        lines.append(f"id: <code>{u['id']}</code>")
        lines.append(f"SOL: <code>{u.get('solanaAddress') or '—'}</code>")
        lines.append(f"EVM: <code>{u.get('evmAddress') or '—'}</code>")
        if u.get("twitter"):
            lines.append(f"X: {u['twitter']} (self-declared)")
        return "\n".join(lines)

    # -- command handlers ----------------------------------------------------

    def cmd_start(self, chat_id: int, _arg: str) -> str:
        return ("<b>Identity + trading platform bot</b>\n\n"
                "Identity: /key /usage /lookup /wallet /thesis\n"
                "Trading: /profile /backtest /watch /unwatch /watches\n"
                "Alpha: /insider\n"
                "Details: /help")

    def cmd_help(self, chat_id: int, _arg: str) -> str:
        return ("/key — mint your API key (one per chat)\n"
                "/usage — your CU balance\n"
                "/lookup &lt;handle&gt; — handle → verified wallets (250 CU)\n"
                "/wallet &lt;address&gt; — wallet → trader (5,000 CU)\n"
                "/thesis &lt;token&gt; — latest posts on a token (25 CU)\n"
                "/profile &lt;wallet&gt; — on-chain trading profile "
                "(free, public RPC, ~1 min)\n"
                "/watch &lt;wallet&gt; — live copy-signal alerts here "
                "(WebSocket stream, paper mode)\n"
                "/backtest &lt;wallet&gt; — what copying would have "
                "returned (~1 min)\n"
                "/insider &lt;handle|wallet&gt; — cross-chain "
                "insider-timing scan (~1 min)\n"
                "/unwatch &lt;wallet&gt; · /watches\n"
                + ("\nAdmin: /ingest &lt;json&gt; · /mintkey &lt;cap&gt;"
                   if chat_id in self.admin_chat_ids else ""))

    def cmd_key(self, chat_id: int, _arg: str) -> str:
        state = self._state(chat_id)
        if state.api_key:
            return f"You already have a key:\n<code>{state.api_key}</code>"
        key = "fsk_live_" + secrets.token_hex(16)
        conn = get_db(self.db_path)
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO api_keys(key, label, plan, monthly_cap, "
            "additional_units, is_admin, created_at) VALUES(?,?,?,?,?,0,?)",
            (key, f"tg:{chat_id}", "free", 100_000, 0, int(time.time())))
        conn.commit()
        conn.close()
        state.api_key = key
        return ("Your API key (100,000 CU/month):\n"
                f"<code>{key}</code>\n"
                f"Base URL: <code>{self.api_url}</code>")

    def cmd_usage(self, chat_id: int, _arg: str) -> str:
        client = self._client(chat_id)
        if not client:
            return "No key yet — send /key first."
        me = client.me()
        u = me["usage"]
        return (f"Plan: {me['plan']} · period {u['period']}\n"
                f"Used: {u['unitsUsed']:,} CU in {u['calls']} calls\n"
                f"Remaining: {u['unitsRemaining']:,} CU "
                f"(+{u['additionalUnits']:,} top-up)")

    def cmd_lookup(self, chat_id: int, arg: str) -> str:
        if not arg:
            return "Usage: /lookup &lt;handle&gt;"
        client = self._client(chat_id)
        if not client:
            return "No key yet — send /key first."
        user = client.user_by_handle(arg)
        return self._fmt_user(user) if user else f"Unknown handle: {arg}"

    def cmd_wallet(self, chat_id: int, arg: str) -> str:
        if not arg:
            return "Usage: /wallet &lt;address&gt;  (costs 5,000 CU on a hit)"
        client = self._client(chat_id)
        if not client:
            return "No key yet — send /key first."
        user = client.user_by_wallet(arg)
        return self._fmt_user(user) if user else \
            f"No trader held for <code>{arg}</code>"

    def cmd_thesis(self, chat_id: int, arg: str) -> str:
        if not arg:
            return "Usage: /thesis &lt;token-address&gt;"
        client = self._client(chat_id)
        if not client:
            return "No key yet — send /key first."
        page = client.theses_for_token(arg)
        items = page.get("items", [])
        if not items:
            return f"No theses held for <code>{arg}</code>"
        sym = page.get("symbol") or "?"
        out = [f"<b>{sym}</b> — {page['count']} recent theses:"]
        for it in items[:5]:
            ts = time.strftime("%m-%d %H:%M",
                               time.gmtime((it["fomoCreatedAt"] or 0) / 1000))
            out.append(f"• <b>@{it['authorHandle']}</b> [{ts} UTC]: "
                       f"{(it['thesis'] or '')[:200]}")
        if page.get("hasMore"):
            out.append("…more exist (API paginates with ?before=)")
        return "\n".join(out)

    def cmd_profile(self, chat_id: int, arg: str) -> str:
        if not arg:
            return "Usage: /profile &lt;solana-wallet&gt;"
        profile = self.profiler.profile(arg, max_transactions=100)
        s = profile.summarize()
        hold = s["median_hold_seconds"]
        hold_str = f"{hold // 60}m {hold % 60}s" if hold else "—"
        venues = ", ".join(list(s["venues"])[:3]) or "—"
        return (f"<b>Profile: {arg[:8]}…{arg[-4:]}</b>\n"
                f"{s['tx_count']} txs over {s['span_hours']}h · "
                f"{s['unique_tokens']} tokens\n"
                f"Round trips: {s['complete_round_trips']} · "
                f"win rate {s['win_rate']:.0%}\n"
                f"Deployed: ${s['total_usd_deployed']:,.0f} · "
                f"realized PnL ${s['net_realized_pnl_usd']:+,.0f}\n"
                f"Median buy ${s['median_buy_usd']:,.0f} · "
                f"median hold {hold_str}\n"
                f"Venues: {venues}")

    def cmd_insider(self, chat_id: int, arg: str) -> str:
        if not arg:
            return ("Usage: /insider &lt;handle-or-wallet&gt;  "
                    "(cross-chain insider-timing scan, ~1 min)")
        client = self._client(chat_id)   # optional; enriches with identity
        analyzer = InsiderAlphaAnalyzer(identity=client, profiler=self.profiler)
        report = analyzer.analyze(arg)
        lines = [f"<b>Insider scan: @{report.handle or '?'}</b>",
                 f"SOL: <code>{report.solana_address or '—'}</code>",
                 f"EVM: <code>{report.evm_address or '—'}</code>",
                 f"<b>Score {report.score}/100</b> — {report.verdict}", ""]
        for s in report.signals:
            state = f"{s.score:.2f}" if s.weight > 0 else "n/a"
            lines.append(f"[{state}] {s.name}")
            if s.evidence:
                lines.append(f"    <i>{s.evidence[0]}</i>")
        for n in report.notes:
            lines.append(f"<i>note: {n}</i>")
        lines.append("\n<i>Public-data timing analysis; a high score is a "
                     "lead to investigate, not proof of wrongdoing.</i>")
        return "\n".join(lines)

    def cmd_backtest(self, chat_id: int, arg: str) -> str:
        if not arg:
            return "Usage: /backtest &lt;solana-wallet&gt;  (takes ~1 min)"
        profile = self.profiler.profile(arg, max_transactions=100)
        if not profile.trades:
            return "No trades found for that wallet."
        out = [f"<b>Backtest: {arg[:8]}…{arg[-4:]}</b> "
               f"({len(profile.trades)} trades, {profile.span_hours:.0f}h)\n"
               "Copying with the default risk config would have returned:"]
        for res in Backtester().sweep(profile.trades,
                                      latencies=(5.0,),
                                      slippages=(0.0, 0.03, 0.08)):
            s = res.summary()
            out.append(f"• slippage {100 * res.slippage_pct:.0f}%: "
                       f"{s['round_trips']} trips, WR {s['win_rate']:.0%}, "
                       f"${s['usd_deployed']:,.0f} → "
                       f"<b>${s['net_pnl_usd']:+,.0f}</b> "
                       f"({100 * s['return_on_turnover']:+.1f}%)")
        out.append("<i>Entries/exits priced off the trader's own fills, "
                   "degraded by slippage; never-exited tokens marked at "
                   "25% recovery.</i>")
        return "\n".join(out)

    def cmd_watch(self, chat_id: int, arg: str) -> str:
        if not arg:
            return "Usage: /watch &lt;solana-wallet&gt;"
        state = self._state(chat_id)
        if arg in state.watchers:
            return f"Already watching <code>{arg}</code>"
        if len(state.watchers) >= 3:
            return "Max 3 watchers per chat — /unwatch one first."

        def on_signal(sig: Signal, _chat=chat_id, _wallet=arg):
            text = (f"🟢 BUY" if sig.action.value == "buy" else "🔴 SELL")
            text += (f" <code>{sig.mint[:10]}…</code>\n"
                     f"trader ${sig.trader_usd:,.0f} → copy "
                     f"${sig.copy_usd:,.0f} (paper)\n"
                     f"watcher: <code>{_wallet[:8]}…</code>")
            if self.transport:
                self.transport.send(_chat, text)

        watcher = StreamingWalletWatcher(arg, SignalConfig(),
                                         on_signal=on_signal)
        thread = threading.Thread(target=watcher.run, daemon=True,
                                  name=f"watch-{arg[:8]}")
        state.watchers[arg] = watcher
        state.threads[arg] = thread
        thread.start()
        return (f"Watching <code>{arg}</code> via WebSocket stream "
                "(sub-second signals, polling fallback) — copy signals "
                "will be pushed here. Paper mode; risk pipeline active: "
                "staleness/dust filters, exposure caps, loss-streak halt.")

    def cmd_unwatch(self, chat_id: int, arg: str) -> str:
        state = self._state(chat_id)
        watcher = state.watchers.pop(arg, None)
        state.threads.pop(arg, None)
        if not watcher:
            return f"Not watching <code>{arg}</code>"
        watcher.stop()
        book = watcher.book.summary()
        return (f"Stopped <code>{arg}</code>. Final paper book: "
                f"{book['closed_trades']} closed, "
                f"${book['realized_pnl_usd']:+,.2f} realized")

    def cmd_watches(self, chat_id: int, _arg: str) -> str:
        state = self._state(chat_id)
        if not state.watchers:
            return "No active watchers. /watch &lt;wallet&gt; to start."
        out = []
        for wallet, watcher in state.watchers.items():
            b = watcher.book.summary()
            out.append(f"<code>{wallet[:8]}…</code> — "
                       f"{b['open_positions']} open "
                       f"(${b['open_exposure_usd']}), "
                       f"{b['closed_trades']} closed, "
                       f"${b['realized_pnl_usd']:+,.2f}"
                       + (" ⛔ halted" if b["risk_halted"] else ""))
        return "\n".join(out)

    # -- admin ---------------------------------------------------------------

    def cmd_ingest(self, chat_id: int, arg: str) -> str:
        if chat_id not in self.admin_chat_ids:
            return "Admin only."
        try:
            body = json.loads(arg)
            assert body.get("handle")
        except Exception:
            return ('Usage: /ingest {"handle":"x","solanaAddress":"…"}')
        import uuid as _uuid
        conn = get_db(self.db_path)
        conn.executescript(SCHEMA)
        evm = body.get("evmAddress")
        conn.execute(
            "INSERT INTO users(id, handle, name, bio, banner, "
            "profile_picture, twitter, solana_address, evm_address, "
            "created_at) VALUES(?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(handle) DO UPDATE SET "
            "solana_address=excluded.solana_address, "
            "evm_address=excluded.evm_address, name=excluded.name",
            (body.get("id") or str(_uuid.uuid4()),
             body["handle"].lstrip("@"), body.get("name"), body.get("bio"),
             body.get("banner"), body.get("profilePicture"),
             body.get("twitter"), body.get("solanaAddress"),
             evm.lower() if evm else None, int(time.time())))
        conn.commit()
        conn.close()
        return f"Ingested @{body['handle'].lstrip('@')}"

    def cmd_mintkey(self, chat_id: int, arg: str) -> str:
        if chat_id not in self.admin_chat_ids:
            return "Admin only."
        try:
            cap = int(arg or 100_000)
        except ValueError:
            return "Usage: /mintkey &lt;monthly-cap&gt;"
        key = "fsk_live_" + secrets.token_hex(16)
        conn = get_db(self.db_path)
        conn.executescript(SCHEMA)
        conn.execute(
            "INSERT INTO api_keys(key, label, plan, monthly_cap, "
            "additional_units, is_admin, created_at) VALUES(?,?,?,?,0,0,?)",
            (key, f"minted-by:{chat_id}", "custom", cap, int(time.time())))
        conn.commit()
        conn.close()
        return f"Minted ({cap:,} CU/mo):\n<code>{key}</code>"

    # -- dispatch ------------------------------------------------------------

    COMMANDS = {
        "start": cmd_start, "help": cmd_help, "key": cmd_key,
        "usage": cmd_usage, "lookup": cmd_lookup, "wallet": cmd_wallet,
        "thesis": cmd_thesis, "profile": cmd_profile, "watch": cmd_watch,
        "backtest": cmd_backtest, "insider": cmd_insider,
        "unwatch": cmd_unwatch, "watches": cmd_watches,
        "ingest": cmd_ingest, "mintkey": cmd_mintkey,
    }

    def dispatch(self, chat_id: int, text: str) -> Optional[str]:
        text = (text or "").strip()
        if not text.startswith("/"):
            return None
        parts = text[1:].split(None, 1)
        cmd = parts[0].split("@")[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else ""
        handler = self.COMMANDS.get(cmd)
        if handler is None:
            return "Unknown command — /help"
        try:
            return handler(self, chat_id, arg)
        except FomoScanError as exc:
            return f"API error: {exc.code} — {exc.message}"
        except Exception as exc:
            logger.exception("command /%s failed", cmd)
            return f"Error: {exc}"

    # -- long-poll loop ------------------------------------------------------

    def run(self) -> None:
        assert self.transport, "TELEGRAM_BOT_TOKEN required for live mode"
        offset = None
        logger.info("Bot polling (api=%s, db=%s, admins=%s)",
                    self.api_url, self.db_path, self.admin_chat_ids)
        while True:
            try:
                for upd in self.transport.get_updates(offset):
                    offset = upd["update_id"] + 1
                    msg = upd.get("message") or {}
                    chat_id = (msg.get("chat") or {}).get("id")
                    if chat_id is None:
                        continue
                    reply = self.dispatch(chat_id, msg.get("text", ""))
                    if reply:
                        self.transport.send(chat_id, reply)
            except KeyboardInterrupt:
                return
            except Exception as exc:
                logger.warning("poll error: %s — retrying in 3s", exc)
                time.sleep(3)


def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        raise SystemExit("Set TELEGRAM_BOT_TOKEN (get one from @BotFather). "
                         "Optional: IDENTITY_API_URL, IDENTITY_DB, "
                         "TELEGRAM_ADMIN_CHAT_IDS")
    PlatformBot(transport=TelegramTransport(token)).run()


if __name__ == "__main__":
    main()
