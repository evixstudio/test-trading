# polymarket-hft-rs vs NautilusTrader: End-to-End Validation

Thorough audit of the polymarket-hft-rs bot against the reference NautilusTrader Polymarket adapter (`crates/adapters/polymarket/`). Covers every stage of the pipeline: data fetch, order construction, signing, HTTP transport, and P&L accounting.

Each finding is classified:
- 🔴 **P0** — Likely affects correctness of live trading.
- 🟡 **P1/P2** — Quality / robustness issue, not an immediate correctness bug.
- ✅ **OK** — Verified equivalent to Nautilus.

---

## 1. Data fetch (Polymarket WebSocket)

### 1.1 ✅ WebSocket wire-format parsing
- `book` and `price_change` event schemas match Nautilus `PolymarketBookSnapshot` / `PolymarketQuote` / `PolymarketQuotes` exactly.
- `event_type`, `asset_id`, `price`, `size`, `side`, `best_ask`, `timestamp` fields extracted correctly at `src/ws_feed.rs:174-234`.

### 1.2 ✅ Subscription protocol
- Initial subscribe: `{"type": "market", "assets_ids": [...]}` — matches Nautilus.
- Mid-session subscribe: `{"assets_ids": [...], "operation": "subscribe"}` — matches Nautilus.
- Unsubscribe format matches.
- Missing `custom_feature_enabled: false` field — harmless (that flag only opts into new-market / market-resolved notifications).

### 1.3 ✅ Best-ask derivation
- Uses local `HashMap::keys().min()` to find best ask (`src/orderbook.rs:170`), not the wire `best_ask` field.
- This is authoritative and order-independent; Polymarket sends asks descending but `.min()` doesn't care.
- `best_ask` field on `price_change` is only used as an "empty book" sentinel (lines 99-111) — correct.

### 1.4 ✅ BUY-side skip is safe
- `src/orderbook.rs:115-117` drops non-SELL deltas.
- Per Polymarket protocol, BUY-side `price_change` only mutates the bid ladder; the `best_ask` field in those events is informational only.
- Dropping BUY events loses **no ask-side information** for a pure market-buy strategy.

### 1.5 ✅ Snapshot reconciliation
- `update_book_snapshot` clears then re-inserts — semantically equivalent to Nautilus' `BookAction::Clear + Add` delta sequence.

### 1.6 ✅ Basis-points integer math
- `(price * 10000.0).round() as u32` handles all three Polymarket tick sizes (0.01 / 0.001 / 0.0001).
- `.round()` before `as u32` guards against f64 imprecision (e.g. `0.29999... * 10000 = 2999.99...`).

---

## 2. Depth / slippage calculation

### 2.1 🔴 P0 — `DEPTH_RANGE` does not track `winning_worst` cap

**Files:** `src/orderbook.rs:5`, `src/gap_engine.rs:73`

```rust
// orderbook.rs
const DEPTH_RANGE: f64 = 0.30;   // depth counted up to best_ask + 0.30

// gap_engine.rs
let winning_worst = (winning_ask + SLIPPAGE_BUFFER).min(0.60);
// ^^^^^^^^^^^^ hard cap at 0.60
```

The depth filter (`winning_depth < shares`, line 64) uses liquidity up to `best_ask + 0.30`, but the actual FAK order can only sweep up to `worst_price = 0.60` (when cap engages).

| `winning_ask` | `winning_worst` sent | Depth range upper bound | Phantom depth |
|---|---|---|---|
| 0.10 | 0.40 | 0.40 | none |
| 0.25 | 0.55 | 0.55 | none |
| 0.30 | 0.60 | 0.60 | none |
| 0.40 | **0.60 (capped)** | **0.70** | 10¢ |
| 0.50 | **0.60 (capped)** | **0.80** | 20¢ |

With the default `--max-price 0.55`, `winning_ask` can be up to 0.55 → phantom depth up to 25¢ wide.

**Impact:** depth filter passes on liquidity that sits above the $0.60 worst-price cap. FAK order partially fills, bot books fewer shares than expected.

**Fix suggestion:** compute `effective_range = winning_worst - winning_ask` and pass it into a parameterised depth query, or drop the `.min(0.60)` cap (slippage is already bounded by `winning_ask + 0.30 ≤ 0.85` given `max_price ≤ 0.55`).

---

## 3. Order construction (executor.rs)

### 3.1 🔴 P0 — `Decimal::from_f64_retain` precision loss on cap-boundary prices

**File:** `src/executor.rs:56-58`

```rust
let dec_price = Decimal::from_f64_retain(price)
    .expect("valid f64")
    .trunc_with_scale(TICK_SIZE_DECIMALS);   // 2
```

Verified against `rust_decimal-1.41.0` source (`decimal.rs:1878`):
```rust
assert_eq!("0.1000000000000000055511151231",
           Decimal::from_f64_retain(0.1_f64).unwrap().to_string());
```

`from_f64_retain` preserves every bit of f64 mantissa. For values like `0.6`, the f64 is `0.5999999999999999777955395073`, so the Decimal retains that value. `trunc_with_scale(2)` then floors → **`0.59`**, not `0.60`.

**Chain of consequences:**
1. `gap_engine.rs:73` — `.min(0.60)` where `0.60_f64 = 0.5999999999999999777...`
2. Whenever the cap engages, `winning_worst` sent to `executor` is already `0.5999...`.
3. `executor.rs:56` — `Decimal::from_f64_retain(0.5999...).trunc_with_scale(2) = 0.59`.
4. EIP-712 order submitted with limit price `0.59`, not `0.60`.

**Numerical behavior of f64 round-trips for other round decimals** (floor after retain):
- `0.1 → 0.10` ✓ (f64 rounds UP, trunc safe)
- `0.3 → 0.29` ✗ (f64 rounds DOWN)
- `0.6 → 0.59` ✗
- `0.7 → 0.69` ✗
- `0.5 → 0.50` ✓ (exactly representable)
- `0.25, 0.125, 0.0625 → exact` ✓

With default `--max-price 0.55` and `SLIPPAGE_BUFFER = 0.30`, the cap engages for `winning_ask ≥ 0.30`. For BTC 5-min markets this is a common operating region → **bug fires frequently**.

**Impact:** bot operates with 29¢ slippage tolerance instead of 30¢, occasionally 1¢ shy of intended — may cause partial fills when full fill was possible. Not a showstopper (Polymarket accepts `0.59`), but the bot is silently more conservative than its config states.

**Fix suggestion:** use string-based construction or `Decimal::from_f64` (the default, which calls `from_f64(n, true)` with `remove_excess_bits=true` and gives `0.6` exactly). Or convert bps → Decimal directly without ever touching `from_f64_retain`.

### 3.2 ✅ Maker/taker amount math
Identical to Nautilus `compute_maker_taker_amounts`:
```
BUY:   taker = qty.trunc(2),     maker = (qty * price).trunc(4)
SELL:  maker = qty.trunc(2),     taker = (qty * price).trunc(4)
then  atomic = decimal.normalize().trunc_with_scale(6).mantissa()
```
Verified line-for-line against `execution/order_builder.rs:260-281`.

### 3.3 ✅ EIP-712 signing
- Domain: `name="Polymarket CTF Exchange"`, `version="1"`, `chainId=137`, `verifyingContract=0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` — matches Nautilus `signing/eip712.rs:49,54-56`.
- Order struct fields and ordering match `ORDER_TYPEHASH` from the CTF Exchange Solidity contract.
- `sign_hash_sync` via alloy produces the same `r||s||v` with `v ∈ {27,28}`.

### 3.4 🟡 P2 — Salt entropy is weaker than Nautilus
**File:** `src/executor.rs:107-115`

```rust
let seconds = now.as_secs_f64();
let r: f64 = rand::random();
let raw = (seconds * r).round() as u64;
raw & ((1u64 << 53) - 1)
```

- `r ∈ [0, 1)`, `seconds ≈ 2^31` → `raw ≤ seconds`. Effective entropy bounded by ~31 bits of the multiplication spread plus f64 precision on `r` — considerably less than the 53-bit mask implies.
- Nautilus uses UUID v4 bytes → full 53-bit entropy after masking.
- Collision probability is still negligible in practice; Polymarket doesn't enforce salt uniqueness globally, only replay protection on `(maker, hash)` pairs.

**Not an active bug**, but worth tightening.

### 3.5 🟡 P2 — `TICK_SIZE_DECIMALS = 2` hardcoded
**File:** `src/config.rs:9`

- Works for BTC 5m markets (tick = 0.01) ✓
- Breaks if bot is ever pointed at markets with 0.001 or 0.0001 tick size — price truncation at 2 decimals will round the limit price down.
- Nautilus fetches `tick_decimals` from instrument metadata dynamically.

### 3.6 ✅ `expiration = 0`, `nonce = 0`, `taker = 0x0`, `signatureType` mapping
All match Nautilus conventions for FAK/IOC market orders.

### 3.7 ✅ Side encoding
`Side::Buy = 0`, `Side::Sell = 1` via the SDK enum. Matches Nautilus `order_side_to_u8`.

---

## 4. HTTP order submission (direct_post.rs)

### 4.1 ✅ L2 HMAC signing
**File:** `src/direct_post.rs:103-116`

```
message = "{timestamp}POST/order{body}"
key     = URL_SAFE.decode(api_secret)
sig     = URL_SAFE.encode(HMAC_SHA256(key, message))
```

Matches Nautilus `common/credential.rs:168-173` byte-for-byte:
```rust
let message = format!("{timestamp}{method}{request_path}{body}");
let key = hmac::Key::new(hmac::HMAC_SHA256, &self.secret_bytes);
URL_SAFE.encode(hmac::sign(&key, message.as_bytes()).as_ref())
```

### 4.2 ✅ Auth header names
`POLY_ADDRESS`, `POLY_API_KEY`, `POLY_PASSPHRASE`, `POLY_SIGNATURE`, `POLY_TIMESTAMP` — all present, same names as Nautilus.

### 4.3 ✅ Timestamp format
- polymarket-hft-rs: `chrono::Utc::now().timestamp()` (i64 seconds since epoch).
- Nautilus: `atomic_clock.get_time_ns() / 1_000_000_000` (u64 seconds).
- Both serialize as decimal string, both match Polymarket's expected format.

### 4.4 ✅ Request body
Uses SDK's `Serialize` impl on `SignedOrder`. Nautilus builds an equivalent `PolymarketOrder` JSON. Field names (`makerAmount`, `takerAmount`, `feeRateBps`, etc. — camelCase) and signature inclusion match.

### 4.5 ✅ HTTP/2 optimizations
Nautilus doesn't apply these — they are a legitimate perf tuning on top of the same protocol. Not a correctness issue.

---

## 5. Gap engine / strategy logic

### 5.1 ✅ Gap direction and side selection (`gap_engine.rs:51-55`)
- `btc_gap > 0` → UP token, else DOWN. Matches the round's YES/NO semantics.
- Token ID swap guard in `token_resolver.rs:105-119` catches cases where Gamma returns `[DN, UP]` instead of `[UP, DN]`.

### 5.2 ✅ Filters 1–5 (price availability, BTC freshness, timing window, gap threshold, max price)
All defensive, correctly ordered, no logic bugs.

### 5.3 🔴 (restated from §2.1) Depth filter sees phantom liquidity
Already covered in §2.1. Same root cause.

### 5.4 🔴 (restated from §3.1) `winning_worst` cap produces `0.59` not `0.60`
Already covered in §3.1. Same root cause.

---

## 6. P&L accounting (types.rs:49-75)

### 6.1 🟡 P1 — Hardcoded fee rate `0.072` may disagree with live fee schedule

**File:** `src/types.rs:63`

```rust
let fee_shares = self.ordered_shares * 0.072 * (1.0 - fill_price);
let effective_shares = self.ordered_shares - fee_shares;
```

Nautilus' `execution/parse.rs:300-304` documents the actual fee model:
> The `fee_rate` here is the effective rate from `feeSchedule.rate` (e.g. 0.03 for 3%), not the `fee_rate_bps` field on a trade or order. **The latter is the maximum fee cap used for order signing and is never the value actually charged.**

Nautilus' `execution/submitter.rs:91` fetches fee per-token via `get_fee_rate_bps(token_id)` with a 300s cache. polymarket-hft-rs hardcodes `0.072` without fetching the live schedule.

**Two separate concerns:**

1. The `7.2%` constant is not obviously documented anywhere — it may be empirically calibrated to BTC 5m markets, but has no dynamic fallback if Polymarket changes the schedule.

2. The formula uses `(1 - fill_price)` unconditionally. Per Polymarket's contract semantics, the fee factor is `min(price, 1-price)`:
   - For `fill_price ≥ 0.5` → `min = 1 - price` → formula is correct.
   - For `fill_price < 0.5` → `min = price` → formula **overestimates** the fee. Since the bot's default `--max-price 0.55`, fills below 0.50 are common.

**Impact:**
- P&L ledger shown in logs/Redis underreports true net P&L for fills below 0.50.
- Daily-loss-limit halt (`main.rs:654`) compares against understated P&L → bot halts trading *earlier* than intended on loss days. Fails-safe.
- Does NOT affect order execution (fee is computed and deducted by the CTF contract, not the bot).

**Fix suggestion:** either fetch the live fee rate (as Nautilus does) or at least use `min(fill_price, 1-fill_price)` in the formula.

### 6.2 ✅ Win/loss determination and payout ($1 per effective share) is correct for binary CTF.

---

## 7. Token resolution (token_resolver.rs)

### 7.1 ✅ Slug construction and round boundary math
- `slug = "btc-updown-5m-{round_start_ts}"` where `round_start_ts = (now / 300) * 300`.
- Fallback to previous round's slug handles the transient gap just after boundary.

### 7.2 ✅ UP/DOWN swap detection
Reads `tokens[].outcome` after parsing `clobTokenIds`; if the UP outcome's `token_id` matches index 1, swaps the array. Defensive and correct.

### 7.3 ✅ `near_boundary` window (8s)
Triggers transition in first 8s of new round. BTC boundary price lookup tolerates Δ ≤ 5s; usually within the 8s window there's a fresh-enough tick.

---

## 8. Binance BTC feed (binance_feed.rs)

### 8.1 ✅ Connection and parsing
- Uses `btcusdt@aggTrade`, extracts `p` (price) field from each event.
- 15s stale-timeout + exponential backoff on reconnect.
- `watch::Sender` for zero-lock distribution to the hot path.
- Nautilus doesn't include a Binance feed; this is strategy-specific, not comparable.

### 8.2 ✅ 10-min history buffer for boundary price lookup
`btc_tracker::lookup_boundary_price` picks the price closest to boundary_ts, accepts up to Δ=5s → labels as `exact(Δ≤3s)` / `approx(Δ≤5s)` / `stale(>5s)`.

---

## 9. Main loop (main.rs)

### 9.1 ✅ Round transition (lines 317-474)
- Triggered by `near_boundary(now, 8)` + slug-change detection.
- Resolves open position using BTC price at new round's boundary.
- Unsubscribes old tokens, subscribes new; resets book and per-round flags.
- No off-by-one or missed-round issues.

### 9.2 ✅ Post-rejection drain (lines 705-732)
After a FAK rejection, drains buffered WS frames for up to 100ms so the bot doesn't act on a stale book. Solid engineering.

### 9.3 ✅ Per-side rejection guard (lines 524-537)
Blocks re-submission against an identical best-ask until a new ask appears. Uses 0.0001 tolerance (1 bps) which matches the bps-quantization of the local book. Correct.

### 9.4 🟡 P2 — Daily P&L relies on §6.1's fee formula
Since `daily_pnl += pnl_result.pnl` and `pnl_result.pnl` uses the hardcoded fee, daily-loss halt thresholds are measured in "bot units" not true dollars. Fails safer (halts earlier), but not exactly what the operator configures.

---

## 10. Severity summary

| # | Severity | Finding |
|---|---|---|
| 2.1 | 🔴 P0 | Depth range over-counts liquidity above the $0.60 worst cap |
| 3.1 | 🔴 P0 | `from_f64_retain + trunc_with_scale` truncates 0.60 → 0.59 (and 0.30→0.29, 0.70→0.69) |
| 6.1 | 🟡 P1 | Hardcoded 7.2% fee with `(1 − fill_price)` factor — wrong sign for fills < $0.50 |
| 3.4 | 🟡 P2 | Salt entropy bounded by `seconds * rand`, not full 53-bit |
| 3.5 | 🟡 P2 | `TICK_SIZE_DECIMALS = 2` hardcoded, breaks on non-penny-tick markets |
| 9.4 | 🟡 P2 | Daily-loss halt threshold derives from §6.1's approximated P&L |

Everything else — WS parsing, book management, EIP-712 domain, HMAC auth, HTTP headers, maker/taker math, order semantics, token resolution, main loop flow — is **verified equivalent to NautilusTrader's reference implementation**.

---

## 11. What I verified vs NautilusTrader reference

| Component | Nautilus file | polymarket-hft-rs file | Status |
|---|---|---|---|
| WS message schema | `websocket/messages.rs` | `src/ws_feed.rs` (inline) | ✅ |
| WS book snapshot parsing | `websocket/parse.rs::parse_book_snapshot` | `src/orderbook.rs::update_book_snapshot` | ✅ |
| WS price-change parsing | `websocket/parse.rs::parse_book_deltas` | `src/orderbook.rs::update_price_change` | ✅ (SELL-only by design) |
| Best-ask extraction | `websocket/parse.rs::parse_quote_from_price_change` | `src/orderbook.rs::recompute` | ✅ |
| Subscription wire format | `websocket/handler.rs:126-138` | `src/ws_feed.rs:66-102` | ✅ |
| EIP-712 domain & Order | `signing/eip712.rs` | `src/executor.rs:126-144` | ✅ |
| Maker/taker amount math | `execution/order_builder.rs::compute_maker_taker_amounts` | `src/executor.rs::build_limit_order` | ✅ |
| HMAC L2 auth | `common/credential.rs::sign` | `src/direct_post.rs::post_order` | ✅ |
| Auth headers | `http/clob.rs:142-154` | `src/direct_post.rs:131-135` | ✅ |
| Fee model | `execution/parse.rs:300-304`, `submitter.rs::get_fee_rate_bps` | `src/types.rs::calculate_pnl` | 🟡 §6.1 |

Not compared (out of scope / strategy-specific):
- Binance BTC feed — Nautilus has no equivalent.
- Redis event bus — Nautilus uses a different engine.
- Strategy filters (gap threshold, timing window) — bot-specific.
