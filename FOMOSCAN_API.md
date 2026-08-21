# FomoScan API Reference (scraped)

Scraped from [fomoscan.sh](https://www.fomoscan.sh/#api-plans),
[fomoscan.sh/api](https://www.fomoscan.sh/api), and the live OpenAPI
spec (snapshot: [`docs/fomoscan_openapi.json`](docs/fomoscan_openapi.json),
API v1.1.0, fetched 2026-08-21). Client implementation:
[`fomoscan_client.py`](fomoscan_client.py).

## What it is

FomoScan is an **independent, unofficial** wallet index for
fomo.family traders: 1,000,000+ proof-verified `handle → wallet` links
across Solana and EVM, plus a feed of trader "thesis" posts per token
and per author. Marketed as "proof-verified FOMO trader identity" —
every returned address is claimed to be a verified match, never a
guess, with opted-out traders excluded.

- Web: `https://fomoscan.sh` (lookup UI, leaderboard, copy-trading page)
- Interactive Swagger docs: `https://api.fomoscan.sh/docs`
- Contact: `fomoscan-support@proton.me` · X: `@fomoscansh`

## Plans & access ("api-plans" section)

There are **no published pricing tiers** — the `#api-plans` section on
the site is a single free-access offer:

> **Get free access** — no card required · we send your API keys and
> docs over Telegram

- Request a key via Telegram: `https://t.me/flamingoscan` (prefilled
  "GM, I am interested in the free access to the fomoscan API")
- Higher volume / per-org entitlements / enterprise: email
  `fomoscan-support@proton.me` ("tell us what you're building and
  we'll size a key for you")

## Billing model (compute units)

One credit balance across every endpoint. From the spec, verbatim
rules:

- Every call costs **compute units (CU)** — no free calls, no
  deduplication.
- A **hit** costs the endpoint's price; a **miss costs a flat 25 CU**
  whatever you called.
- Fixed prices: **handle or id lookup 250 CU · wallet resolution
  5,000 CU · thesis page 25 CU**.
- One monthly counter, **hard cap, no rollover**. Top-up
  (`additionalUnits`) never expires and is spent only after the
  month's grant is exhausted.
- `GET /v2/me` reports usage and remaining units, costs 0 CU.

## Authentication

```
Authorization: Bearer fsk_live_…      (or fsk_test_…)
# also accepted:
X-Api-Key: fsk_live_…
```

Base URL: `https://api.fomoscan.sh`
(the site frontend uses origin `https://api-production-9541.up.railway.app`,
same service).

## Endpoints

| Method & path | Purpose | Cost (hit / miss) |
|---|---|---|
| `GET /v2/user/handle/{handle}` | fomo.family handle → user record + verified wallets | 250 / 25 CU |
| `GET /v2/user/wallet/{address}` | wallet → the trader behind it (chain inferred; EVM needs `0x`) | **5,000** / 25 CU |
| `GET /v2/user/id/{id}` | stable user id → user record (ids never change; handles do) | 250 / 25 CU |
| `POST /v2/user/handle/{handle}/resolve` | live resolve: if no wallet is held, a crawler goes to fomo.family to prove one (~4–7 s, ≤15 s; `202 {pending}` if queued; `x-fomoscan-resolve: cached\|live`) | 250 / 25 CU |
| `GET /v2/thesis` | every thesis on the platform, newest first, 20/page | 25 CU/page |
| `GET /v2/thesis/token/{tokenAddress}` | theses about one token, newest first, 25/page | 25 CU/page |
| `GET /v2/thesis/user/{id}` | theses by one author, 20/page | 25 CU/page |
| `GET /v2/thesis/user/{id}/token/{tokenAddress}` | one author on one token | 25 CU/page |
| `GET /v2/me` | key introspection: plan, scopes, entitlement, both unit buckets | 0 CU |

### User record shape (all identity endpoints)

```json
{
  "id": "a1f2…c3d4",            // stable FOMO user id — cache/join on this
  "handle": "frankdegods",       // canonical handle (users rename!)
  "name": "frank",
  "bio": "in the candle business",
  "banner": "https://…",
  "profilePicture": "https://…",
  "twitter": "https://x.com/frankdegods",  // self-declared, NOT verified
  "solanaAddress": "498g1rVn…jxkjAayQ",    // verified, or null
  "evmAddress": "0x696d12…fa9d8e28"        // verified, or null
}
```

`200` with both addresses `null` = user known, no verified wallet held
(never "has none"). `404` = handle/wallet/id unknown.

### Thesis feed shape

Page envelope: `{tokenAddress, tokenNetwork, symbol, updatedAt, count,
hasMore, nextBefore, items[]}`. Items carry `{id, tokenAddress,
tokenNetwork, tokenSymbol, authorId, authorHandle, authorName, thesis,
fomoCreatedAt}`.

Pagination contract:
- **Live feed**: poll with no `before`, dedupe on item `id`.
- **Backfill**: pass `nextBefore` back as `?before=` until `hasMore`
  is false. `before` is a thesis **id**, not a timestamp; an unknown
  `before` is a `400`.

### Error model

Flat JSON `{"error": {"code", "message"}}` with stable codes:
`NOT_FOUND, VALIDATION_ERROR, QUERY_TOO_SHORT, RATE_LIMITED,
UNAUTHORIZED, QUEUE_UNAVAILABLE, SCRAPER_UNHEALTHY, INTERNAL_ERROR,
NOT_IMPLEMENTED, EVM_REQUIRES_VERIFIED_SVM, SYNC_ALREADY_RUNNING,
TIMEOUT, BAD_REQUEST, FORBIDDEN, FLEET_UNAVAILABLE, …`

## Example

```bash
curl -H "Authorization: Bearer $FOMOSCAN_KEY" \
  https://api.fomoscan.sh/v2/user/handle/frankdegods
```

## Fit with this repo's trading system

- **`/v2/user/wallet/{address}`** answers "who is the trader behind
  `6SHqkz…3obS`?" — identity context for any wallet
  `trader_profiler.py` profiles. At 5,000 CU per hit, resolve once and
  cache by `id`.
- **`/v2/thesis/token/{tokenAddress}`** gives narrative context for a
  token the tracked wallet just bought — `copy_signal_engine` signals
  can be enriched with "is anyone posting a thesis on this?" at 25 CU
  per check.
- **`/v2/thesis/user/{id}`** turns a resolved trader into a followable
  post feed alongside their on-chain trades.

`fomoscan_client.py` wraps all of this with CU cost tracking and no
external dependencies.
