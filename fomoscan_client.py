"""
FomoScan Client - Identity + thesis-feed API client for the trading system.

Wraps the FomoScan B2B API (see FOMOSCAN_API.md, scraped 2026-08-21):
handle <-> wallet resolution and per-token/per-author thesis feeds, with
local compute-unit (CU) accounting so a budget is never burned blind -
a wallet resolution costs 5,000 CU per hit, so results are cached by
stable user id.

Usage:
    client = FomoScanClient(api_key="fsk_live_...")
    user = client.user_by_wallet("6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS")
    feed = client.theses_for_token(mint)
    print(client.usage)   # local CU spend estimate
    print(client.me())    # server-side truth (0 CU)

No external dependencies - standard library only.
"""

import json
import time
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FomoScanClient")

DEFAULT_BASE_URL = "https://api.fomoscan.sh"

# Fixed per-endpoint prices from the published spec (CU)
CU_LOOKUP_HIT = 250        # handle or id lookup hit
CU_WALLET_HIT = 5000       # wallet resolution hit
CU_THESIS_PAGE = 25        # any thesis page, hit or miss
CU_MISS = 25               # flat miss price on any endpoint


class FomoScanError(Exception):
    """API error with the spec's stable error code (e.g. RATE_LIMITED)."""

    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message


@dataclass
class CuUsage:
    """Local estimate of compute units spent by this client instance.

    An estimate only - `/v2/me` is the server-side truth. Misses are
    counted at the flat 25 CU price the spec defines.
    """
    spent: int = 0
    calls: int = 0
    by_endpoint: Dict[str, int] = field(default_factory=dict)

    def add(self, endpoint: str, cost: int) -> None:
        self.spent += cost
        self.calls += 1
        self.by_endpoint[endpoint] = self.by_endpoint.get(endpoint, 0) + cost


class FomoScanClient:
    """Minimal client for the FomoScan identity + thesis API."""

    def __init__(self, api_key: str, base_url: str = DEFAULT_BASE_URL,
                 timeout: float = 20.0, cu_budget: Optional[int] = None):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.cu_budget = cu_budget          # optional local hard stop
        self.usage = CuUsage()
        self._user_cache: Dict[str, dict] = {}   # id/handle/wallet -> record

    # -- transport -----------------------------------------------------------

    def _request(self, method: str, path: str,
                 params: Optional[Dict[str, str]] = None) -> dict:
        if self.cu_budget is not None and self.usage.spent >= self.cu_budget:
            raise FomoScanError(0, "LOCAL_BUDGET_EXHAUSTED",
                                f"local CU budget {self.cu_budget} spent")
        url = self.base_url + path
        if params:
            from urllib.parse import urlencode
            url += "?" + urlencode(params)
        req = urllib.request.Request(url, method=method, headers={
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json",
        })
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return {"status": resp.status,
                        "headers": dict(resp.headers),
                        "json": json.loads(resp.read() or b"{}")}
        except urllib.error.HTTPError as exc:
            body = {}
            try:
                body = json.loads(exc.read() or b"{}")
            except Exception:
                pass
            err = (body.get("error") or {})
            code = err.get("code", "HTTP_ERROR")
            if exc.code == 404:
                # 404 is a data answer (unknown handle/wallet/id), not a fault
                return {"status": 404, "headers": dict(exc.headers), "json": body}
            raise FomoScanError(exc.code, code,
                                err.get("message", str(exc))) from exc

    # -- identity endpoints --------------------------------------------------

    def user_by_handle(self, handle: str) -> Optional[dict]:
        """Resolve a fomo.family handle. 250 CU hit / 25 CU miss."""
        key = "h:" + handle.lstrip("@").lower()
        if key in self._user_cache:
            return self._user_cache[key]
        r = self._request("GET", f"/v2/user/handle/{handle.lstrip('@')}")
        hit = r["status"] == 200
        self.usage.add("user/handle", CU_LOOKUP_HIT if hit else CU_MISS)
        record = r["json"] if hit else None
        if record:
            self._cache_user(record, extra_keys=[key])
        return record

    def user_by_wallet(self, address: str) -> Optional[dict]:
        """Resolve a wallet to the trader behind it. 5,000 CU hit / 25 miss.

        Cached aggressively - this is the expensive call. EVM addresses
        must keep their 0x prefix; the chain is inferred server-side.
        """
        key = "w:" + address
        if key in self._user_cache:
            return self._user_cache[key]
        r = self._request("GET", f"/v2/user/wallet/{address}")
        hit = r["status"] == 200
        self.usage.add("user/wallet", CU_WALLET_HIT if hit else CU_MISS)
        record = r["json"] if hit else None
        if record:
            self._cache_user(record, extra_keys=[key])
        return record

    def user_by_id(self, user_id: str) -> Optional[dict]:
        """Resolve a stable FOMO user id. 250 CU hit / 25 CU miss."""
        key = "i:" + user_id
        if key in self._user_cache:
            return self._user_cache[key]
        r = self._request("GET", f"/v2/user/id/{user_id}")
        hit = r["status"] == 200
        self.usage.add("user/id", CU_LOOKUP_HIT if hit else CU_MISS)
        record = r["json"] if hit else None
        if record:
            self._cache_user(record)
        return record

    def resolve_handle_live(self, handle: str) -> dict:
        """POST live resolve: crawls fomo.family if no wallet is held.

        Returns {"pending": True, ...} on 202 (queued, not failed);
        the user record on 200. Typically 4-7s, held at most 15s.
        250 CU hit / 25 CU miss.
        """
        r = self._request("POST", f"/v2/user/handle/{handle.lstrip('@')}/resolve")
        hit = r["status"] == 200
        self.usage.add("user/resolve", CU_LOOKUP_HIT if hit else CU_MISS)
        if hit:
            self._cache_user(r["json"])
            mode = r["headers"].get("x-fomoscan-resolve", "?")
            logger.info("resolved %s (%s)", handle, mode)
        return r["json"]

    def _cache_user(self, record: dict, extra_keys: Optional[List[str]] = None):
        keys = ["i:" + record["id"], "h:" + record["handle"].lower()]
        if record.get("solanaAddress"):
            keys.append("w:" + record["solanaAddress"])
        if record.get("evmAddress"):
            keys.append("w:" + record["evmAddress"])
        for k in keys + (extra_keys or []):
            self._user_cache[k] = record

    # -- thesis endpoints (25 CU per page, hit or miss) ----------------------

    def theses(self, before: Optional[str] = None) -> dict:
        """The unfiltered global thesis feed, newest first, 20/page."""
        params = {"before": before} if before else None
        r = self._request("GET", "/v2/thesis", params)
        self.usage.add("thesis", CU_THESIS_PAGE)
        return r["json"]

    def theses_for_token(self, token_address: str,
                         before: Optional[str] = None) -> dict:
        """Theses about one token, newest first, 25/page."""
        params = {"before": before} if before else None
        r = self._request("GET", f"/v2/thesis/token/{token_address}", params)
        self.usage.add("thesis/token", CU_THESIS_PAGE)
        return r["json"]

    def theses_by_user(self, user_id: str,
                       before: Optional[str] = None) -> dict:
        """Theses posted by one author, newest first, 20/page."""
        params = {"before": before} if before else None
        r = self._request("GET", f"/v2/thesis/user/{user_id}", params)
        self.usage.add("thesis/user", CU_THESIS_PAGE)
        return r["json"]

    def theses_by_user_for_token(self, user_id: str, token_address: str,
                                 before: Optional[str] = None) -> dict:
        """One author's theses about one token."""
        params = {"before": before} if before else None
        r = self._request(
            "GET", f"/v2/thesis/user/{user_id}/token/{token_address}", params)
        self.usage.add("thesis/user/token", CU_THESIS_PAGE)
        return r["json"]

    def backfill_theses_for_token(self, token_address: str,
                                  max_pages: int = 10) -> Iterator[dict]:
        """Walk a token's thesis wall backwards; yields items oldest-last.

        Stops at max_pages to bound CU spend (25 CU per page).
        """
        before = None
        for _ in range(max_pages):
            page = self.theses_for_token(token_address, before=before)
            for item in page.get("items", []):
                yield item
            if not page.get("hasMore") or not page.get("nextBefore"):
                return
            before = page["nextBefore"]

    # -- account -------------------------------------------------------------

    def me(self) -> dict:
        """Key introspection: plan, scopes, unit buckets. Costs 0 CU."""
        return self._request("GET", "/v2/me")["json"]


def annotate_trader(client: FomoScanClient, wallet: str) -> Optional[dict]:
    """Convenience: resolve a profiled wallet to its FOMO identity.

    Intended for use next to trader_profiler: one 5,000-CU resolution
    gives the handle/bio/socials behind a wallet worth copying, after
    which everything else (their thesis feed, cheap id lookups) keys on
    the stable `id`.
    """
    user = client.user_by_wallet(wallet)
    if user is None:
        logger.info("No FOMO identity held for %s", wallet)
        return None
    logger.info("Wallet %s belongs to @%s (%s)", wallet[:8],
                user["handle"], user.get("name"))
    return user
