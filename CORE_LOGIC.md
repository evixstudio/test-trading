# Polymarket HFT Bot - Core Logic Documentation

## Overview

This document explains the core trading logic of the Rust-based Polymarket HFT bot, focusing on data retrieval, order placement, and the up/down decision mechanism with buffers.

---

## 1. Data Retrieval from Polymarket APIs

### 1.1 Token Resolution (Gamma API)

**File:** `src/token_resolver.rs`

The bot resolves UP/DOWN token IDs for the current 5-minute BTC round using the Gamma API.

**Process:**
- Calculates the current round timestamp: `ts = (now / 300) * 300` (300-second rounds)
- Queries `https://gamma-api.polymarket.com/events` with slug `btc-updown-5m-{ts}`
- Falls back to previous round if current round not available
- Extracts `clobTokenIds` from the market data (handles both JSON array and string formats)
- Corrects token ID ordering if UP/DOWN are swapped
- Returns a `TokenPair` containing:
  - `up_id`, `dn_id`: U256 token IDs
  - `up_id_str`, `dn_id_str`: String representations
  - `boundary`: Round timestamp (Price-to-beat reference)

**Key Function:** `resolve_tokens(http, timestamp) -> TokenPair`

---

### 1.2 WebSocket Feed (Real-time Orderbook)

**File:** `src/ws_feed.rs`

The bot maintains a persistent WebSocket connection to Polymarket's orderbook feed.

**Connection:**
- URL: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Auto-reconnect on disconnection
- Liveness ping after 15s of no data

**Subscription:**
- Initial subscribe: `{"type": "market", "assets_ids": [up_id, dn_id]}`
- Mid-connection subscribe: `{"assets_ids": [...], "operation": "subscribe"}`
- Unsubscribe: `{"assets_ids": [...], "operation": "unsubscribe"}`

**Event Types Processed:**

1. **`price_change`** (Incremental updates)
   - Contains: `asset_id`, `side`, `price`, `size`, `best_ask`
   - Only SELL-side events update the local ask book
   - BUY-side events are ignored
   - Empty/missing `best_ask` clears the ask book

2. **`book`** (Full snapshots)
   - Contains: `asset_id`, `asks` array
   - Each ask level: `{"price": "...", "size": "..."}`
   - Replaces entire local book for that token

**Message Parsing:**
- Uses `simd-json` for high-performance JSON parsing
- Handles both text and binary WebSocket frames
- Returns `WsOutcome::BookUpdated` if book mutated, `MessageNoOp` otherwise, or `Timeout`

---

### 1.3 BTC Price Feed (Binance)

**File:** `src/binance_feed.rs`

External BTC price reference for gap calculation.

**Process:**
- Connects to Binance WebSocket: `wss://stream.binance.com:9443/ws/btcusdt@aggTrade`
- Receives aggregated trade updates
- Broadcasts current price via `watch::Receiver<BtcPrice>` (zero-lock reads)
- Maintains 10-minute price history in `VecDeque<(timestamp, price)>`
- Auto-reconnect with exponential backoff (max 15s)

**BTC Price Structure:**
```rust
struct BtcPrice {
    price: f64,
    timestamp: f64,
}
```

---

## 2. Local Orderbook Management

**File:** `src/orderbook.rs`

The `ShadowBook` maintains a local copy of UP/DOWN ask books for fast decision-making.

### Data Structure

```rust
struct ShadowBook {
    up_asks: HashMap<PriceBps, f64>,  // Price in basis points (1 bps = 0.0001)
    dn_asks: HashMap<PriceBps, f64>,
    up_ask: f64,          // Cached best ask price
    up_ask_size: f64,     // Cached depth within slippage range
    dn_ask: f64,
    dn_ask_size: f64,
    last_book_event: Instant,
}
```

### Update Mechanisms

**1. `update_price_change()`** - Incremental updates
- Parses SELL-side `price_change` events
- Inserts/deletes ask levels in HashMap
- Recomputes best ask and depth if changed

**2. `update_book_snapshot()`** - Full replacement
- Replaces entire HashMap with new asks array
- Always triggers recompute

### Depth Calculation with Buffer

**Critical:** Depth is calculated only within the slippage buffer range.

```rust
const DEPTH_RANGE: f64 = 0.30;  // Must match SLIPPAGE_BUFFER in gap_engine

fn recompute(&mut self, is_up: bool) -> bool {
    let best_price = min(asks.keys());
    let max_bps = price_to_bps(best_price + DEPTH_RANGE);  // best_price + 0.30
    
    let depth: f64 = asks
        .iter()
        .filter(|&(&k, _)| k <= max_bps)  // Only count liquidity within $0.30 of best ask
        .map(|(_, &v)| v)
        .sum();
    
    // Cache best_price and depth
}
```

**Why this matters:** The bot only considers liquidity that can be filled within the slippage buffer. Orders deeper than $0.30 from the best ask are ignored.

---

## 3. Up/Down Decision Logic (Gap Engine)

**File:** `src/gap_engine.rs`

The core decision engine determines when to trade and which side.

### Function Signature

```rust
pub fn check_gap(
    book: &ShadowBook,
    btc_price: f64,
    btc_age: f64,
    price_to_beat: f64,
    current_time: f64,
    round_start: u64,
    gap_threshold_pct: f64,
    max_price: f64,
    window_start: f64,
    window_end: f64,
    shares: f64,
) -> Option<GapSignal>
```

### Filter Chain (HOT PATH - zero allocation)

**Filter 1: Price Availability**
```rust
if book.up_ask <= 0.0 || book.dn_ask <= 0.0 {
    return None;  // Both sides must have active asks
}
```

**Filter 2: Data Freshness**
```rust
if btc_price <= 0.0 || price_to_beat <= 0.0 || btc_age > 10.0 {
    return None;  // BTC price must be fresh (< 10s old)
}
```

**Filter 3: Timing Window**
```rust
let seconds_remaining = 300.0 - (current_time - round_start as f64);
if seconds_remaining > window_start || seconds_remaining < window_end {
    return None;  // Only trade within configured window (e.g., 180s-30s remaining)
}
```

**Filter 4: Gap Threshold (Adaptive)**
```rust
let btc_gap = btc_price - price_to_beat;
let min_gap = price_to_beat * gap_threshold_pct;  // e.g., 0.04% of PTB

if btc_gap.abs() < min_gap {
    return None;  // Gap must exceed threshold
}
```

**Filter 5: Value Filter**
```rust
let (winning_side, winning_ask, winning_id_up) = if btc_gap > 0.0 {
    ("UP", book.up_ask, true)
} else {
    ("DOWN", book.dn_ask, false)
};

if winning_ask >= max_price {
    return None;  // Winning side must be cheap enough (e.g., < $0.55)
}
```

**Filter 6: Depth Filter (Liquidity Check)**
```rust
let winning_depth = if winning_id_up { book.up_ask_size } else { book.dn_ask_size };
if winning_depth < shares {
    return None;  // Not enough liquidity within slippage range
}
```

### Slippage Buffer Application

```rust
const SLIPPAGE_BUFFER: f64 = 0.30;  // Fixed 30-cent buffer

// Apply slippage, but cap at $0.60 max
let winning_worst = (winning_ask + SLIPPAGE_BUFFER).min(0.60);
```

**Purpose:** The buffer accounts for price movement between signal generation and order execution. The order is placed at `winning_worst` (best_ask + $0.30), ensuring fill even if price moves slightly against us.

### Return Value

```rust
struct GapSignal {
    side: &'static str,      // "UP" or "DOWN"
    winning_ask: f64,        // Current best ask
    winning_worst: f64,     // Order price (ask + slippage)
    is_up: bool,            // true if UP, false if DOWN
    shares: f64,            // Quantity to trade
    btc_gap: f64,           // BTC price - PTB
    seconds_remaining: f64, // Time left in round
}
```

---

## 4. Order Placement

**Files:** `src/executor.rs`, `src/direct_post.rs`

### 4.1 Order Construction

**Function:** `build_limit_order()`

Builds a limit order struct with hardcoded Polymarket parameters:
- `TICK_SIZE_DECIMALS = 2` (tick size = 0.01)
- `LOT_SIZE_SCALE = 2`
- `USDC_DECIMALS = 6`
- `FEE_RATE_BPS = 1000` (10% fee)

**Process:**
```rust
// Truncate size and price to required scales
dec_size = size.trunc_with_scale(LOT_SIZE_SCALE)
dec_price = price.trunc_with_scale(TICK_SIZE_DECIMALS)

// Calculate taker/maker amounts based on side
if Side::Buy {
    taker_amount = dec_size           // shares
    maker_amount = dec_size * dec_price  // USDC cost
}

// Convert to atomic units (6 decimals)
taker_fixed = to_fixed_u128(taker_amount)
maker_fixed = to_fixed_u128(maker_amount)

// Generate salt (masked to IEEE 754 integer range)
salt = (timestamp * random()) & ((1 << 53) - 1)
```

### 4.2 Local Order Signing

**Function:** `sign_order_local()`

Signs the order using EIP-712 with hardcoded domain (zero HTTP calls).

**Domain:**
```rust
Eip712Domain {
    name: "Polymarket CTF Exchange",
    version: "1",
    chain_id: 137,  // Polygon mainnet
    verifying_contract: 0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E,
}
```

**Process:**
- Computes EIP-712 signing hash
- Signs locally using `PrivateKeySigner` (alloy library)
- Returns `SignedOrder` with signature and `OrderType::FAK`

### 4.3 Optimized HTTP/2 Order Posting

**Class:** `FastOrderClient` in `src/direct_post.rs`

Bypasses the SDK's default reqwest client with performance optimizations:

**HTTP/2 Optimizations:**
- `.no_proxy()` - Avoids OS proxy lookup (5-100ms savings)
- `.tcp_nodelay(true)` - Disables Nagle's algorithm
- `.http2_adaptive_window(true)` - Better flow control
- `.http2_initial_stream_window_size(512KB)` - Larger initial window
- `.http2_keep_alive_interval(10s)` - Sends PING frames every 10s
- `.http2_keep_alive_while_idle(true)` - PINGs even when idle
- `.pool_max_idle_per_host(10)` - Keeps more warm connections
- `.pool_idle_timeout(90s)` - Longer connection reuse

### 4.4 HMAC L2 Authentication

**Algorithm (matches SDK):**
```
message = "{timestamp}{METHOD}{path}{body}"
key = base64_url_decode(api_secret)
signature = base64_url_encode(HMAC-SHA256(key, message))
```

**Headers:**
- `POLY_ADDRESS`: Wallet address
- `POLY_API_KEY`: API key
- `POLY_PASSPHRASE`: Passphrase
- `POLY_SIGNATURE`: HMAC signature
- `POLY_TIMESTAMP`: Unix timestamp

### 4.5 Order Execution Flow

**Function:** `market_buy_fak()`

```rust
let order = build_limit_order(...);
let signed = sign_order_local(&order, OrderType::FAK, owner);
let response = fast_client.post_order(&signed).await;
```

**Latency Breakdown:**
- `build_ms`: Order struct construction
- `sign_ms`: Local EIP-712 signing
- `post_ms`: HTTP POST to CLOB
- `total_ms`: End-to-end latency

**Response Parsing:**
```rust
struct OrderResult {
    success: bool,
    order_id: String,
    error: String,
    filled_price: f64,   // maker_amount / taker_amount
    filled_shares: f64,  // taker_amount
}
```

### 4.6 Connection Prewarming

**Function:** `prewarm()`

Called every 60s to keep HTTP/2 connection warm:
- Sends GET request to CLOB host
- Prevents TLS/TCP handshake on hot path
- Logs latency (warns if > 50ms)

---

## 5. Main Trading Loop

**File:** `src/main.rs`

### Round Lifecycle

1. **Token Resolution**
   - Query Gamma API for current round tokens
   - Lookup Price-to-Beat (PTB) from BTC history or cache
   - Subscribe to UP/DOWN via WebSocket

2. **Orderbook Processing Loop**
   - Receive WebSocket updates via `ws.next_update()`
   - Update `ShadowBook` with price_change/book events
   - Check round boundary every 5 seconds

3. **Signal Evaluation**
   - Call `gap_engine::check_gap()` on every book update
   - Apply filter chain (availability, freshness, timing, gap, value, depth)
   - If signal passes, execute trade

4. **Trade Execution**
   - Build order with `winning_worst` price (ask + slippage)
   - Sign locally (EIP-712)
   - POST via `FastOrderClient`
   - On success: Record position, mark traded_this_round
   - On rejection: Increment retry count, invalidate book side, drain stale WS events

5. **Round Boundary**
   - Detect near-boundary (within 8s)
   - Resolve old positions using final BTC price
   - Calculate P&L with fee model
   - Sync balance from API
   - Switch to new round tokens
   - Reset book, retry counters, rejection guards

### Rejection Handling

**Guard Mechanism:**
```rust
// After rejection, store the ask price
if signal.is_up {
    last_rejected_up_ask = signal.winning_ask;
    book.invalidate_side(true);  // Clear UP book
} else {
    last_rejected_dn_ask = signal.winning_ask;
    book.invalidate_side(false);  // Clear DOWN book
}

// Skip if current ask matches rejected price (within 0.0001 tolerance)
if (current_ask - rejected_ask).abs() < 0.0001 {
    continue;
}
```

**WS Drain Loop:**
After FAK rejection (blocking 500ms-3s), drain buffered WS events to avoid acting on stale data:
```rust
loop {
    match ws.next_update(..., Duration::from_millis(2)).await {
        Ok(BookUpdated) => drain_count += 1,
        Ok(Timeout) => break,  // Buffer empty
        Err(_) => break,
    }
}
```

---

## 6. Key Constants

```rust
// Trading parameters
const ROUND_SECONDS: u64 = 300;           // 5-minute rounds
const SLIPPAGE_BUFFER: f64 = 0.30;       // 30-cent slippage
const DEPTH_RANGE: f64 = 0.30;           // Depth calculation range

// Polymarket parameters
const TICK_SIZE_DECIMALS: u32 = 2;        // 0.01 tick size
const FEE_RATE_BPS: u64 = 1000;          // 10% fee
const USDC_DECIMALS: u32 = 6;             // 6 decimal places
const MIN_ORDER_VALUE: f64 = 1.01;        // Minimum $1.01 order

// API endpoints
const CLOB_HOST: &str = "https://clob.polymarket.com";
const GAMMA_HOST: &str = "https://gamma-api.polymarket.com";
const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
```

---

## 7. Performance Optimizations

1. **Zero-allocation hot path** - `check_gap()` is `#[inline]` with no heap allocations
2. **SIMD JSON parsing** - Uses `simd-json` for WS message parsing
3. **Local signing** - EIP-712 signing done locally (no HTTP calls)
4. **HTTP/2 pooling** - Connection reuse with keep-alive
5. **Watch channels** - Zero-lock BTC price reads via `watch::Receiver`
6. **Basis point arithmetic** - Prices stored as u32 (1 bps = 0.0001) for fast comparison
7. **Shadow book caching** - Best ask and depth cached after every update
8. **Prewarming** - Periodic requests to keep TLS/TCP warm

---

## 8. Data Flow Summary

```
Binance WS → BTC Price (watch channel)
     ↓
Gamma API → Token IDs (UP/DOWN)
     ↓
Polymarket WS → Orderbook Updates (price_change/book)
     ↓
ShadowBook → Local ask books with depth calculation
     ↓
Gap Engine → Filter chain → GapSignal (if all pass)
     ↓
Executor → Build Order → Local Sign → FastOrderClient POST
     ↓
CLOB API → Order Response → Filled/Rejected
```
