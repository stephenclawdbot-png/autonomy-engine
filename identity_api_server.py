"""
Identity API Server - Self-hosted, FomoScan-compatible identity + thesis API.

A drop-in reimplementation of the API surface documented in
FOMOSCAN_API.md (and docs/fomoscan_openapi.json): the same nine
endpoints, response shapes, error model, and compute-unit billing —
backed by a local SQLite database that YOU populate with your own
verified handle <-> wallet links and thesis posts.

What is and is not the same:
- Identical: paths, JSON shapes, auth headers (Bearer / X-Api-Key),
  hit/miss CU prices (250 lookup, 5,000 wallet, 25 thesis page, 25
  miss), monthly hard cap with non-expiring top-ups, stable error codes.
- Different: no crawler fleet — POST .../resolve answers from the local
  tables only (x-fomoscan-resolve: cached) and 404s otherwise. The data
  is whatever you ingest via the admin endpoints or CLI; this ships
  EMPTY. FomoScan's dataset is theirs — build your own.

Run:
    python identity_api_server.py init-db
    python identity_api_server.py create-key --plan free --cap 100000
    python identity_api_server.py create-admin-key
    python identity_api_server.py serve --port 8080
    python identity_api_server.py seed-demo          # optional demo rows

Ingest (admin key required):
    POST /admin/users   {"handle": ..., "solanaAddress": ..., ...}
    POST /admin/theses  {"tokenAddress": ..., "authorId": ..., "thesis": ...}

No external dependencies - standard library only. fomoscan_client.py
works against this server unchanged (point base_url at it).
"""

import argparse
import json
import re
import secrets
import sqlite3
import threading
import time
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("IdentityAPI")

DEFAULT_DB = "identity_api.db"

# CU prices — identical to the reference API
CU_LOOKUP_HIT = 250
CU_WALLET_HIT = 5000
CU_THESIS_PAGE = 25
CU_MISS = 25

PAGE_FEED = 20          # global + per-user thesis pages
PAGE_TOKEN = 25         # per-token thesis pages

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    handle TEXT NOT NULL UNIQUE,
    name TEXT, bio TEXT, banner TEXT, profile_picture TEXT, twitter TEXT,
    solana_address TEXT, evm_address TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_users_sol ON users(solana_address);
CREATE INDEX IF NOT EXISTS idx_users_evm ON users(evm_address);

CREATE TABLE IF NOT EXISTS theses (
    id TEXT PRIMARY KEY,
    token_address TEXT, token_network TEXT, token_symbol TEXT,
    author_id TEXT NOT NULL REFERENCES users(id),
    thesis TEXT NOT NULL,
    fomo_created_at INTEGER NOT NULL,
    seq INTEGER
);
CREATE INDEX IF NOT EXISTS idx_theses_token ON theses(token_address, seq DESC);
CREATE INDEX IF NOT EXISTS idx_theses_author ON theses(author_id, seq DESC);

CREATE TABLE IF NOT EXISTS api_keys (
    key TEXT PRIMARY KEY,
    label TEXT,
    plan TEXT NOT NULL DEFAULT 'free',
    monthly_cap INTEGER NOT NULL DEFAULT 100000,
    additional_units INTEGER NOT NULL DEFAULT 0,
    is_admin INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS usage (
    key TEXT NOT NULL,
    period TEXT NOT NULL,
    units_used INTEGER NOT NULL DEFAULT 0,
    calls INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (key, period)
);
"""

_db_lock = threading.Lock()


def get_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def user_record(row: sqlite3.Row) -> Dict:
    """The exact user-record shape of the reference API."""
    return {
        "id": row["id"],
        "handle": row["handle"],
        "name": row["name"],
        "bio": row["bio"],
        "banner": row["banner"],
        "profilePicture": row["profile_picture"],
        "twitter": row["twitter"],
        "solanaAddress": row["solana_address"],
        "evmAddress": row["evm_address"],
    }


def thesis_item(row: sqlite3.Row) -> Dict:
    return {
        "id": row["id"],
        "tokenAddress": row["token_address"],
        "tokenNetwork": row["token_network"],
        "tokenSymbol": row["token_symbol"],
        "authorId": row["author_id"],
        "authorHandle": row["author_handle"],
        "authorName": row["author_name"],
        "thesis": row["thesis"],
        "fomoCreatedAt": row["fomo_created_at"],
    }


class Billing:
    """Monthly hard cap + non-expiring top-ups, charged every call."""

    @staticmethod
    def period() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m")

    @staticmethod
    def snapshot(conn, key_row) -> Dict:
        period = Billing.period()
        row = conn.execute(
            "SELECT units_used, calls FROM usage WHERE key=? AND period=?",
            (key_row["key"], period)).fetchone()
        used = row["units_used"] if row else 0
        grant_used = min(used, key_row["monthly_cap"])
        overflow = used - grant_used
        return {
            "period": period,
            "unitsUsed": used,
            "unitsRemaining": max(0, key_row["monthly_cap"] - used),
            "additionalUnits": max(0, key_row["additional_units"] - overflow),
            "calls": row["calls"] if row else 0,
        }

    @staticmethod
    def charge(conn, key_row, cost: int) -> bool:
        """Charge cost CU. Returns False when the hard cap blocks the call."""
        with _db_lock:
            snap = Billing.snapshot(conn, key_row)
            available = snap["unitsRemaining"] + snap["additionalUnits"]
            if cost > available:
                return False
            conn.execute(
                "INSERT INTO usage(key, period, units_used, calls) VALUES(?,?,?,1) "
                "ON CONFLICT(key, period) DO UPDATE SET "
                "units_used = units_used + excluded.units_used, calls = calls + 1",
                (key_row["key"], snap["period"], cost))
            conn.commit()
        return True


class Handler(BaseHTTPRequestHandler):
    server_version = "IdentityAPI/1.0"
    db_path = DEFAULT_DB          # overridden by serve()
    openapi_path: Optional[str] = None

    # -- plumbing ------------------------------------------------------------

    def log_message(self, fmt, *args):
        logger.info("%s %s", self.address_string(), fmt % args)

    def _send(self, status: int, body: Dict, headers: Optional[Dict] = None):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(payload)

    def _error(self, status: int, code: str, message: str):
        self._send(status, {"error": {"code": code, "message": message}})

    def _auth(self, conn) -> Optional[sqlite3.Row]:
        token = None
        auth = self.headers.get("Authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:].strip()
        if not token:
            token = self.headers.get("X-Api-Key", "").strip() or None
        if not token:
            return None
        return conn.execute("SELECT * FROM api_keys WHERE key=?",
                            (token,)).fetchone()

    def _read_body(self) -> Dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return {}

    # -- billing wrapper -----------------------------------------------------

    def _charged(self, conn, key_row, cost: int) -> bool:
        if Billing.charge(conn, key_row, cost):
            return True
        self._error(429, "RATE_LIMITED",
                    "monthly compute-unit cap reached — no rollover; "
                    "top up or wait for the next period")
        return False

    # -- routing -------------------------------------------------------------

    def do_GET(self):
        self._route("GET")

    def do_POST(self):
        self._route("POST")

    def _route(self, method: str):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = {k: v[0] for k, v in parse_qs(parsed.query).items()}
        conn = get_db(self.db_path)
        try:
            if method == "GET" and path == "/openapi.json":
                return self._serve_openapi()
            if method == "GET" and path in ("/", "/docs"):
                return self._send(200, {
                    "name": "Identity API (self-hosted, FomoScan-compatible)",
                    "spec": "/openapi.json",
                    "reference": "FOMOSCAN_API.md"})

            key_row = self._auth(conn)
            if key_row is None:
                return self._error(401, "UNAUTHORIZED",
                                   "invalid or missing API key")

            if path.startswith("/admin/"):
                return self._admin(conn, key_row, method, path)

            m = re.match(r"^/v2/user/handle/([^/]+)/resolve$", path)
            if m and method == "POST":
                return self._user_by_handle(conn, key_row, m.group(1),
                                            resolve=True)
            m = re.match(r"^/v2/user/handle/([^/]+)$", path)
            if m and method == "GET":
                return self._user_by_handle(conn, key_row, m.group(1))
            m = re.match(r"^/v2/user/wallet/([^/]+)$", path)
            if m and method == "GET":
                return self._user_by_wallet(conn, key_row, m.group(1))
            m = re.match(r"^/v2/user/id/([^/]+)$", path)
            if m and method == "GET":
                return self._user_by_id(conn, key_row, m.group(1))

            if path == "/v2/thesis" and method == "GET":
                return self._thesis_feed(conn, key_row, query, None, None)
            m = re.match(r"^/v2/thesis/token/([^/]+)$", path)
            if m and method == "GET":
                return self._thesis_feed(conn, key_row, query,
                                         token=m.group(1), author=None)
            m = re.match(r"^/v2/thesis/user/([^/]+)/token/([^/]+)$", path)
            if m and method == "GET":
                return self._thesis_feed(conn, key_row, query,
                                         token=m.group(2), author=m.group(1))
            m = re.match(r"^/v2/thesis/user/([^/]+)$", path)
            if m and method == "GET":
                return self._thesis_feed(conn, key_row, query,
                                         token=None, author=m.group(1))

            if path == "/v2/me" and method == "GET":
                return self._me(conn, key_row)

            return self._error(404, "NOT_FOUND", "no such endpoint")
        except Exception:
            logger.exception("request failed")
            self._error(500, "INTERNAL_ERROR", "internal error")
        finally:
            conn.close()

    # -- identity endpoints --------------------------------------------------

    def _user_by_handle(self, conn, key_row, handle: str, resolve=False):
        handle = handle.lstrip("@").lower()
        row = conn.execute("SELECT * FROM users WHERE lower(handle)=?",
                           (handle,)).fetchone()
        cost = CU_LOOKUP_HIT if row else CU_MISS
        if not self._charged(conn, key_row, cost):
            return
        if row:
            headers = {"x-fomoscan-resolve": "cached"} if resolve else None
            return self._send(200, user_record(row), headers)
        if resolve:
            return self._error(404, "FLEET_UNAVAILABLE",
                               "self-hosted mode has no crawler fleet; "
                               "ingest this handle via /admin/users")
        self._error(404, "NOT_FOUND", "unknown handle")

    def _user_by_wallet(self, conn, key_row, address: str):
        if address.startswith("0x"):
            row = conn.execute(
                "SELECT * FROM users WHERE lower(evm_address)=? "
                "ORDER BY created_at ASC LIMIT 1",
                (address.lower(),)).fetchone()
        else:
            row = conn.execute(
                "SELECT * FROM users WHERE solana_address=? "
                "ORDER BY created_at ASC LIMIT 1",
                (address,)).fetchone()
        cost = CU_WALLET_HIT if row else CU_MISS
        if not self._charged(conn, key_row, cost):
            return
        if row:
            return self._send(200, user_record(row))
        self._error(404, "NOT_FOUND", "no user held for this address")

    def _user_by_id(self, conn, key_row, user_id: str):
        row = conn.execute("SELECT * FROM users WHERE id=?",
                           (user_id,)).fetchone()
        cost = CU_LOOKUP_HIT if row else CU_MISS
        if not self._charged(conn, key_row, cost):
            return
        if row:
            return self._send(200, user_record(row))
        self._error(404, "NOT_FOUND", "unknown user id")

    # -- thesis endpoints ----------------------------------------------------

    def _thesis_feed(self, conn, key_row, query: Dict,
                     token: Optional[str], author: Optional[str]):
        page_size = PAGE_TOKEN if (token and not author) else PAGE_FEED
        before = query.get("before")

        where, params = [], []
        if token:
            if token.startswith("0x"):
                token = token.lower()
            where.append("t.token_address=?")
            params.append(token)
        if author:
            where.append("t.author_id=?")
            params.append(author)
        if before:
            ref = conn.execute("SELECT seq FROM theses WHERE id=?",
                               (before,)).fetchone()
            if ref is None:
                # billing note: 400s for a bad cursor are not charged
                return self._error(400, "BAD_REQUEST",
                                   "unknown `before` id — cannot resume "
                                   "from a thesis that no longer exists")
            where.append("t.seq < ?")
            params.append(ref["seq"])

        if not self._charged(conn, key_row, CU_THESIS_PAGE):
            return

        sql = ("SELECT t.*, u.handle AS author_handle, u.name AS author_name "
               "FROM theses t LEFT JOIN users u ON u.id = t.author_id ")
        if where:
            sql += "WHERE " + " AND ".join(where) + " "
        sql += "ORDER BY t.seq DESC LIMIT ?"
        rows = conn.execute(sql, params + [page_size + 1]).fetchall()
        has_more = len(rows) > page_size
        rows = rows[:page_size]
        items = [thesis_item(r) for r in rows]

        body = {
            "tokenAddress": token,
            "tokenNetwork": rows[0]["token_network"] if rows else None,
            "symbol": rows[0]["token_symbol"] if rows else None,
            "updatedAt": rows[0]["fomo_created_at"] if rows else None,
            "count": len(items),
            "hasMore": has_more,
            "nextBefore": rows[-1]["id"] if rows and has_more else None,
            "items": items,
        }
        if token is None:
            # Global/user feeds share the shape minus the token header fields
            for k in ("tokenAddress", "tokenNetwork", "symbol"):
                body.pop(k, None)
        self._send(200, body)

    # -- account -------------------------------------------------------------

    def _me(self, conn, key_row):
        self._send(200, {
            "key": key_row["key"][:12] + "…",
            "label": key_row["label"],
            "plan": key_row["plan"],
            "scopes": ["identities:read", "thesis:read"] +
                      (["admin"] if key_row["is_admin"] else []),
            "entitlement": {"monthlyCap": key_row["monthly_cap"]},
            "usage": Billing.snapshot(conn, key_row),
        })

    # -- admin ingestion (self-host extension, not in the reference API) -----

    def _admin(self, conn, key_row, method: str, path: str):
        if not key_row["is_admin"]:
            return self._error(403, "FORBIDDEN", "admin key required")
        body = self._read_body() if method == "POST" else {}

        if path == "/admin/users" and method == "POST":
            if not body.get("handle"):
                return self._error(400, "VALIDATION_ERROR", "handle required")
            uid = body.get("id") or str(uuid.uuid4())
            evm = body.get("evmAddress")
            conn.execute(
                "INSERT INTO users(id, handle, name, bio, banner, "
                "profile_picture, twitter, solana_address, evm_address, "
                "created_at) VALUES(?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(handle) DO UPDATE SET name=excluded.name, "
                "bio=excluded.bio, banner=excluded.banner, "
                "profile_picture=excluded.profile_picture, "
                "twitter=excluded.twitter, "
                "solana_address=excluded.solana_address, "
                "evm_address=excluded.evm_address",
                (uid, body["handle"].lstrip("@"), body.get("name"),
                 body.get("bio"), body.get("banner"),
                 body.get("profilePicture"), body.get("twitter"),
                 body.get("solanaAddress"),
                 evm.lower() if evm else None, int(time.time())))
            conn.commit()
            row = conn.execute("SELECT * FROM users WHERE lower(handle)=?",
                               (body["handle"].lstrip("@").lower(),)).fetchone()
            return self._send(200, user_record(row))

        if path == "/admin/theses" and method == "POST":
            if not body.get("authorId") or not body.get("thesis"):
                return self._error(400, "VALIDATION_ERROR",
                                   "authorId and thesis required")
            tid = body.get("id") or str(uuid.uuid4())
            created = int(body.get("fomoCreatedAt") or time.time() * 1000)
            token = body.get("tokenAddress")
            with _db_lock:
                seq = (conn.execute("SELECT COALESCE(MAX(seq),0)+1 AS s "
                                    "FROM theses").fetchone()["s"])
                conn.execute(
                    "INSERT OR REPLACE INTO theses(id, token_address, "
                    "token_network, token_symbol, author_id, thesis, "
                    "fomo_created_at, seq) VALUES(?,?,?,?,?,?,?,?)",
                    (tid, token.lower() if token and token.startswith("0x")
                     else token, body.get("tokenNetwork"),
                     body.get("tokenSymbol"), body["authorId"],
                     body["thesis"], created, seq))
                conn.commit()
            return self._send(200, {"id": tid, "seq": seq})

        return self._error(404, "NOT_FOUND", "no such admin endpoint")

    def _serve_openapi(self):
        if self.openapi_path:
            try:
                spec = json.load(open(self.openapi_path))
                spec.setdefault("info", {})["title"] = \
                    "Identity API (self-hosted, FomoScan-compatible)"
                spec["servers"] = [{"url": "/", "description": "this server"}]
                return self._send(200, spec)
            except Exception:
                pass
        self._error(404, "NOT_FOUND", "spec snapshot not available")


# -- CLI ---------------------------------------------------------------------

def cmd_init_db(args):
    conn = get_db(args.db)
    conn.executescript(SCHEMA)
    conn.commit()
    print(f"initialized {args.db}")


def cmd_create_key(args):
    conn = get_db(args.db)
    conn.executescript(SCHEMA)
    key = ("fsk_admin_" if args.admin else "fsk_live_") + secrets.token_hex(16)
    conn.execute(
        "INSERT INTO api_keys(key, label, plan, monthly_cap, "
        "additional_units, is_admin, created_at) VALUES(?,?,?,?,?,?,?)",
        (key, args.label, args.plan, args.cap, args.topup,
         1 if args.admin else 0, int(time.time())))
    conn.commit()
    print(key)


def cmd_seed_demo(args):
    conn = get_db(args.db)
    conn.executescript(SCHEMA)
    uid = str(uuid.uuid4())
    conn.execute(
        "INSERT OR IGNORE INTO users(id, handle, name, bio, banner, "
        "profile_picture, twitter, solana_address, evm_address, created_at) "
        "VALUES(?,?,?,?,?,?,?,?,?,?)",
        (uid, "demotrader", "Demo Trader", "seeded example — not a real user",
         None, None, None,
         "6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS", None,
         int(time.time())))
    for i in range(3):
        conn.execute(
            "INSERT OR IGNORE INTO theses(id, token_address, token_network, "
            "token_symbol, author_id, thesis, fomo_created_at, seq) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (str(uuid.uuid4()), "So11111111111111111111111111111111111111112",
             "sol", "SOL", uid, f"demo thesis #{i + 1}",
             int(time.time() * 1000) + i, i + 1))
    conn.commit()
    print("seeded demo user 'demotrader' with 3 theses")


def cmd_serve(args):
    conn = get_db(args.db)
    conn.executescript(SCHEMA)
    conn.close()
    Handler.db_path = args.db
    Handler.openapi_path = args.openapi
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    logger.info("Identity API listening on %s:%d (db=%s)",
                args.host, args.port, args.db)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--db", default=DEFAULT_DB)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init-db").set_defaults(fn=cmd_init_db)

    k = sub.add_parser("create-key")
    k.add_argument("--label", default=None)
    k.add_argument("--plan", default="free")
    k.add_argument("--cap", type=int, default=100_000)
    k.add_argument("--topup", type=int, default=0)
    k.add_argument("--admin", action="store_true")
    k.set_defaults(fn=cmd_create_key)

    a = sub.add_parser("create-admin-key")
    a.set_defaults(fn=lambda args: cmd_create_key(
        argparse.Namespace(db=args.db, label="admin", plan="admin",
                           cap=10_000_000, topup=0, admin=True)))

    sub.add_parser("seed-demo").set_defaults(fn=cmd_seed_demo)

    s = sub.add_parser("serve")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8080)
    s.add_argument("--openapi", default="docs/fomoscan_openapi.json")
    s.set_defaults(fn=cmd_serve)

    args = ap.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
