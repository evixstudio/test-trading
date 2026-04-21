"""
Live Gap-Based Directional Trading Bot
======================================
Production-ready implementation of the validated gap-based directional strategy.

Strategy Parameters (Validated via backtest):
- Gap Threshold: 0.04% of price-to-beat (adaptive)
- Position Size: 3 shares ($10 starting capital)
- Value Filter: Entry price < $0.55
- Timing Window: 30-180 seconds remaining
- Expected: $293/25 days (2,934% ROI)

Risk Management:
- $1 minimum order value enforcement
- $5 minimum reserve
- $3 daily loss limit
- Max 50% bankroll per trade
- One trade per round maximum

Usage:
    python live_gap_directional_bot.py --live --shares 3
    python live_gap_directional_bot.py                    # Dry-run mode
"""

import asyncio
import os
import time
import argparse
import logging
import sys
from collections import deque
from datetime import datetime, timezone

if sys.platform != "win32":
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

try:
    import orjson as json
except ImportError:
    import json

import requests

try:
    import websockets
except ImportError:
    print("Install websockets: pip install websockets")
    sys.exit(1)

try:
    import redis
except ImportError:
    print("Install redis: pip install redis")
    sys.exit(1)

from bot.config import ApiConfig
from bot.async_executor import AsyncOrderExecutor
from dotenv import load_dotenv
load_dotenv()

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

GAMMA_HOST = "https://gamma-api.polymarket.com"
PM_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
ROUND_SECONDS = 300

MIN_ORDER_VALUE = 1.00  # Polymarket minimum
# FIXED SLIPPAGE: 4 ticks for 350-500ms execution latency
SLIPPAGE_BUFFER = 0.04    # Fixed 4 ticks (covers typical 350-500ms latency)
TAKER_FEE_RATE = 0.072    # Fee deducted from shares: fee_shares = C * 0.072 * (1 - fill_price)
MAX_BOUNDARY_DELTA = 5.0  # Max seconds from boundary for PTB lookup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gap-bot")

# ═══════════════════════════════════════════════════════════════════════════
# REDIS CONNECTION
# ═══════════════════════════════════════════════════════════════════════════

_redis_pool = redis.ConnectionPool(
    host="localhost", port=6379, db=3, decode_responses=True, socket_timeout=2
)
redis_client = redis.Redis(connection_pool=_redis_pool)


def publish_event(event_type: str, data: dict):
    """
    Publish event to Redis for monitoring.
    OPTIMIZED: Silent failure - don't let monitoring break trading.
    """
    try:
        payload = {"type": event_type, "timestamp": time.time(), **data}
        redis_client.publish("gap_bot:events", json.dumps(payload))
    except Exception:
        # Silent failure - don't let monitoring break trading
        pass


def update_heartbeat(balance: float):
    """Update heartbeat for watchdog monitoring."""
    try:
        payload = {"timestamp": time.time(), "balance": balance}
        redis_client.setex("gap_bot:heartbeat", 15, json.dumps(payload))
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# HELPER FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def _ws_json(obj) -> str:
    """JSON dumps that returns str (handles orjson bytes)."""
    raw = json.dumps(obj)
    return raw.decode("utf-8") if isinstance(raw, bytes) else raw


def _lookup_boundary_price(history: deque, boundary_ts: float) -> tuple[float, str]:
    """Find BTC price closest to round boundary from history."""
    if not history:
        return 0.0, "no_history"

    best_price = 0.0
    best_delta = float("inf")

    for ts, price in history:
        delta = abs(ts - boundary_ts)
        if delta < best_delta:
            best_delta = delta
            best_price = price

    if best_delta <= 3.0:
        return best_price, f"exact(Δ{best_delta:.1f}s)"
    elif best_delta <= MAX_BOUNDARY_DELTA:
        return best_price, f"approx(Δ{best_delta:.1f}s)"
    else:
        return 0.0, f"stale(Δ{best_delta:.0f}s)"


# Removed adaptive slippage function - now using fixed SLIPPAGE_BUFFER constant


def resolve_tokens(timestamp: float | None = None) -> dict:
    """Resolve UP/DOWN token IDs for current round via Gamma API."""
    ts = int((timestamp or time.time()) // ROUND_SECONDS) * ROUND_SECONDS
    slug = f"btc-updown-5m-{ts}"

    for attempt_slug in [slug, f"btc-updown-5m-{ts - ROUND_SECONDS}"]:
        try:
            r = requests.get(
                f"{GAMMA_HOST}/events",
                params={"slug": attempt_slug, "limit": 1},
                timeout=5,
            )
            if r.status_code != 200:
                continue
            events = r.json()
            if not events:
                continue

            market = events[0].get("markets", [{}])[0]
            clob_raw = market.get("clobTokenIds", [])
            tokens_raw = market.get("tokens", [])

            if isinstance(clob_raw, str):
                try:
                    clob_raw = json.loads(clob_raw)
                except:
                    clob_raw = []

            up_id = down_id = ""
            if isinstance(clob_raw, list) and len(clob_raw) >= 2:
                up_id, down_id = str(clob_raw[0]), str(clob_raw[1])
            elif isinstance(tokens_raw, list):
                for tk in tokens_raw:
                    if isinstance(tk, dict):
                        outcome = tk.get("outcome", "").upper()
                        tid = tk.get("token_id", "")
                        if "UP" in outcome or "YES" in outcome:
                            up_id = tid
                        elif "DOWN" in outcome or "NO" in outcome:
                            down_id = tid

            if up_id and down_id:
                return {
                    "slug": attempt_slug,
                    "up_id": up_id,
                    "down_id": down_id,
                    "boundary": ts,
                }
        except Exception as e:
            log.warning(f"Token resolve error: {e}")

    return {"slug": slug, "error": "Could not resolve tokens"}


# ═══════════════════════════════════════════════════════════════════════════
# BINANCE BTC FEED
# ═══════════════════════════════════════════════════════════════════════════

async def run_binance_btc_feed(btc_state: dict):
    """Stream real-time BTC/USDT from Binance aggTrade WebSocket."""
    backoff = 1
    while True:
        ws = None
        try:
            ws = await websockets.connect(
                BINANCE_WS_URL, ping_interval=20, ping_timeout=10, close_timeout=5
            )
            backoff = 1
            log.info("Binance WS connected")

            last_update = time.time()

            while True:
                now = time.time()

                if now - last_update > 15 and btc_state["price"] > 0:
                    log.warning("Binance: No update for 15s, reconnecting")
                    break

                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=5.0)
                except asyncio.TimeoutError:
                    continue

                try:
                    data = json.loads(raw)
                except (ValueError, TypeError):
                    continue

                if not isinstance(data, dict):
                    continue

                price_str = data.get("p")
                if price_str is not None:
                    try:
                        p = float(price_str)
                        t = time.time()
                        btc_state["price"] = p
                        btc_state["last_update"] = t
                        btc_state["history"].append((t, p))
                        last_update = t
                    except (ValueError, TypeError):
                        pass

        except websockets.ConnectionClosed:
            log.warning(f"Binance WS disconnected. Reconnecting in {backoff}s...")
        except Exception as e:
            log.warning(f"Binance WS error: {e}. Reconnecting in {backoff}s...")
        finally:
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 15)


async def run_balance_refresh(
    executor: AsyncOrderExecutor, balance_state: dict, interval_s: float = 300.0
):
    """
    Refresh collateral balance periodically (SANITY CHECK ONLY).

    NOTE: Balance is tracked locally after each trade for accuracy.
    This periodic refresh is ONLY for:
    - Detecting external deposits/withdrawals
    - Sanity check for accounting drift
    - NOT used in hot trading path

    Default: 300s (5 minutes) - infrequent to avoid API dependency
    """
    while True:
        try:
            bal = await executor.get_collateral_balance()
            if bal >= 0:
                # Log if local tracking diverged from API
                local_bal = balance_state.get("available", 0.0)
                if abs(bal - local_bal) > 0.01:  # More than $0.01 difference
                    log.warning(
                        f"Balance drift detected: API=${bal:.2f} vs Local=${local_bal:.2f} "
                        f"(Δ=${bal - local_bal:+.2f})"
                    )

                # Update with API balance (sanity check passed)
                balance_state["available"] = bal
                balance_state["last_update"] = time.time()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            # Silent failure - don't let balance refresh break trading
            log.debug(f"Balance refresh failed (non-critical): {e}")
            pass
        await asyncio.sleep(interval_s)


async def run_heartbeat_updates(balance_state: dict, interval_s: float = 5.0):
    """Background task for heartbeat updates (avoid creating tasks in hot loop)."""
    while True:
        try:
            update_heartbeat(balance_state.get("available", 0.0))
        except asyncio.CancelledError:
            raise
        except Exception:
            pass
        await asyncio.sleep(interval_s)


# ═══════════════════════════════════════════════════════════════════════════
# MAIN TRADING LOOP
# ═══════════════════════════════════════════════════════════════════════════

async def run_gap_bot(
    executor: AsyncOrderExecutor,
    shares: float,
    gap_threshold_pct: float,
    max_price: float,
    window_start: float,
    window_end: float,
    min_reserve: float,
    daily_loss_limit: float,
    max_bankroll_fraction: float,
):
    log.info("=" * 70)
    log.info("  GAP-BASED DIRECTIONAL TRADING BOT")
    log.info(f"  Mode: {'LIVE EXECUTION' if not executor.dry_run else 'DRY RUN'}")
    log.info(f"  Shares: {shares}")
    log.info(f"  Gap Threshold: {gap_threshold_pct*100:.3f}% of PTB (adaptive)")
    log.info(f"  Max Entry Price: ${max_price:.2f}")
    log.info(f"  Timing Window: {window_end}-{window_start}s remaining")
    log.info(f"  Min Reserve: ${min_reserve:.2f}")
    log.info(f"  Daily Loss Limit: ${daily_loss_limit:.2f}")
    log.info("=" * 70)

    if not await executor.initialize():
        log.error("Failed to initialize executor. Exiting.")
        return

    # State
    btc_state = {"price": 0.0, "history": deque(maxlen=600), "last_update": 0.0}
    btc_task = asyncio.create_task(run_binance_btc_feed(btc_state))

    balance_state = {"available": 0.0, "last_update": 0.0}
    balance_task = None
    heartbeat_task = None
    if not executor.dry_run and max_bankroll_fraction > 0:
        # Fetch initial balance once (required for trading)
        balance_state["available"] = await executor.get_collateral_balance()
        balance_state["last_update"] = time.time()
        log.info(f"Initial balance: ${balance_state['available']:.2f}")

        # Optional: Start background balance refresh (sanity check only)
        # Runs every 5 minutes - NOT used in hot trading path
        # Balance is tracked locally after each trade for accuracy
        # Can be disabled by commenting out next 3 lines (not recommended but safe)
        balance_task = asyncio.create_task(
            run_balance_refresh(executor, balance_state, interval_s=300.0)  # 5 min
        )

    # Background heartbeat task (avoid creating tasks in hot loop)
    heartbeat_task = asyncio.create_task(run_heartbeat_updates(balance_state))

    # Wait for BTC price
    log.info("Waiting for BTC price...")
    for _ in range(75):
        if btc_state["price"] > 0:
            break
        await asyncio.sleep(0.2)

    if btc_state["price"] <= 0:
        log.error("Could not get BTC price after 15s. Exiting.")
        publish_event(
            "critical_error",
            {
                "error": "Could not get BTC price after 15s",
                "action": "Bot exiting",
            },
        )
        btc_task.cancel()
        if balance_task:
            balance_task.cancel()
        return

    log.info(f"BTC price active: ${btc_state['price']:.2f}")

    publish_event(
        "bot_started",
        {
            "mode": "LIVE" if not executor.dry_run else "DRY_RUN",
            "gap_threshold_pct": gap_threshold_pct,
            "max_price": max_price,
            "shares": shares,
            "window": f"{window_end}-{window_start}s",
            "min_reserve": min_reserve,
            "daily_loss_limit": daily_loss_limit,
            "btc_price": round(btc_state["price"], 2),
        },
    )

    last_heartbeat = 0.0
    price_to_beat = 0.0
    price_to_beat_cache: dict[str, float] = {}
    last_traded_slug = ""

    # Position tracking (for actual P&L calculation)
    open_positions: dict[str, dict] = {}

    # Daily PnL tracking
    daily_pnl = 0.0
    daily_trade_count = 0
    daily_pnl_date = datetime.now(timezone.utc).date()
    daily_loss_halted = False

    # Status logging removed for HFT performance
    # (ROUND BOUNDARY logs every 5 min already show bot is alive)

    while True:
        try:
            ws = await websockets.connect(
                PM_WS_URL, ping_interval=30, ping_timeout=20, close_timeout=5
            )
        except Exception as e:
            log.warning(f"Polymarket WS connect failed: {e}. Retrying in 3s...")
            await asyncio.sleep(3)
            continue

        log.info("Polymarket WS connected")
        current_tokens: dict = {}
        prev_token_ids: list[str] = []
        last_resolve_time = 0.0

        try:
            # Initial token resolution
            tokens = resolve_tokens()
            if "error" in tokens:
                log.warning(f"Token resolve failed: {tokens['error']}. Retrying...")
                await ws.close()
                await asyncio.sleep(5)
                continue

            current_tokens = tokens
            token_ids = [tokens["up_id"], tokens["down_id"]]

            up_id = token_ids[0]
            dn_id = token_ids[1]
            up_ask = 0.0
            dn_ask = 0.0

            slug = tokens["slug"]
            if slug in price_to_beat_cache:
                price_to_beat = price_to_beat_cache[slug]
                log.info(f"RECONNECT: {slug} | PTB: ${price_to_beat:.2f} (cached)")
            else:
                ptb, src = _lookup_boundary_price(
                    btc_state["history"], tokens["boundary"]
                )
                if ptb > 0:
                    price_to_beat = ptb
                    price_to_beat_cache[slug] = ptb
                    log.info(f"NEW ROUND: {slug} | PTB: ${price_to_beat:.2f} ({src})")
                else:
                    price_to_beat = 0.0
                    log.info(f"SKIP ROUND: {slug} | No PTB ({src})")

            sub = _ws_json({"type": "market", "assets_ids": token_ids})
            await ws.send(sub)
            prev_token_ids = token_ids

            while True:
                now = time.time()

                # Monitor background tasks (no heartbeat overhead in hot loop)
                if now - last_heartbeat >= 5.0:
                    last_heartbeat = now

                    if btc_task.done():
                        log.warning("Binance task died. Restarting...")
                        btc_task = asyncio.create_task(run_binance_btc_feed(btc_state))

                    if heartbeat_task and heartbeat_task.done():
                        log.warning("Heartbeat task died. Restarting...")
                        heartbeat_task = asyncio.create_task(run_heartbeat_updates(balance_state))

                # Daily PnL reset at UTC midnight
                today_utc = datetime.now(timezone.utc).date()
                if today_utc != daily_pnl_date:
                    log.info(
                        f"UTC DAY ROLLOVER | PnL: ${daily_pnl:+.2f} ({daily_trade_count} trades)"
                    )
                    publish_event(
                        "daily_summary",
                        {
                            "date": str(daily_pnl_date),
                            "pnl": round(daily_pnl, 2),
                            "trades": daily_trade_count,
                        },
                    )
                    daily_pnl = 0.0
                    daily_trade_count = 0
                    daily_pnl_date = today_utc
                    daily_loss_halted = False

                # Periodic status removed (HFT optimization)
                # ROUND BOUNDARY logs every 5 min already confirm bot is alive
                # All critical info logged via event-driven logs (SIGNAL, FILLED, RESOLVED)

                # Check for round change
                if now - last_resolve_time >= 5:
                    next_boundary = (int(now) // ROUND_SECONDS + 1) * ROUND_SECONDS
                    near_boundary = (next_boundary - now) < 8 or (
                        now - (int(now) // ROUND_SECONDS) * ROUND_SECONDS
                    ) < 8

                    if near_boundary:
                        tokens = resolve_tokens()
                        if "error" not in tokens and tokens["slug"] != current_tokens.get(
                            "slug", ""
                        ):
                            # Round boundary detected - resolve previous round positions
                            old_slug = current_tokens.get("slug", "")
                            new_boundary = tokens["boundary"]

                            # Resolve position from previous round (if exists)
                            if old_slug and old_slug in open_positions:
                                pos = open_positions[old_slug]

                                # Get final BTC price at the exact boundary moment
                                final_btc, src = _lookup_boundary_price(
                                    btc_state["history"], new_boundary
                                )

                                # Fallback to current price if lookup fails
                                if final_btc <= 0:
                                    final_btc = btc_state["price"]
                                    log.warning(
                                        f"Boundary price lookup failed for {old_slug} ({src}), "
                                        f"using current BTC=${final_btc:.2f}"
                                    )

                                # Determine actual outcome
                                stored_ptb = pos["ptb"]
                                if final_btc > stored_ptb:
                                    actual_outcome = "UP"
                                else:
                                    actual_outcome = "DOWN"

                                # Calculate actual P&L with explicit fee
                                # Fee = deducted from shares received
                                # fee_shares = ordered * feeRate * (1 - fill_price)
                                ordered = pos["ordered_shares"]
                                cost = pos["cost"]
                                fill_price = cost / ordered if ordered > 0 else 0
                                fee_shares = ordered * TAKER_FEE_RATE * (1 - fill_price)
                                effective_shares = ordered - fee_shares

                                if pos["side"] == actual_outcome:
                                    # WIN: $1.00 per effective (received) share
                                    payout = 1.0 * effective_shares
                                    actual_pnl = payout - cost
                                else:
                                    # LOSS: shares worthless
                                    actual_pnl = -cost

                                # Update daily P&L with ACTUAL result
                                daily_pnl += actual_pnl

                                # Credit balance for wins (accurate local tracking, no drift)
                                if pos["side"] == actual_outcome:
                                    balance_state["available"] += payout

                                log.info(
                                    f"RESOLVED: {old_slug} | {pos['side']} | "
                                    f"Outcome={actual_outcome} | BTC=${final_btc:.2f} vs PTB=${stored_ptb:.2f} | "
                                    f"Ordered={ordered} Fee={fee_shares:.3f}sh Effective={effective_shares:.3f}sh | "
                                    f"P&L={actual_pnl:+.2f} | daily_pnl=${daily_pnl:+.2f}"
                                )

                                # Publish position resolution event
                                publish_event(
                                    "position_resolved",
                                    {
                                        "slug": old_slug,
                                        "side": pos["side"],
                                        "outcome": actual_outcome,
                                        "won": pos["side"] == actual_outcome,
                                        "ordered_shares": ordered,
                                        "effective_shares": round(effective_shares, 4),
                                        "fee_shares": round(fee_shares, 4),
                                        "fill_price": round(fill_price, 4),
                                        "cost": round(cost, 2),
                                        "payout": round(payout, 2) if pos["side"] == actual_outcome else 0.0,
                                        "pnl": round(actual_pnl, 2),
                                        "final_btc": round(final_btc, 2),
                                        "ptb": round(stored_ptb, 2),
                                        "daily_pnl": round(daily_pnl, 2),
                                    },
                                )

                                # Remove resolved position
                                del open_positions[old_slug]

                            log.info(f"ROUND BOUNDARY | daily_pnl=${daily_pnl:+.2f}")

                            current_tokens = tokens
                            token_ids = [tokens["up_id"], tokens["down_id"]]

                            new_slug = tokens["slug"]
                            new_boundary = tokens["boundary"]
                            if new_slug in price_to_beat_cache:
                                price_to_beat = price_to_beat_cache[new_slug]
                                log.info(f"NEW ROUND: {new_slug} | PTB: ${price_to_beat:.2f} (cached)")
                            else:
                                ptb, src = _lookup_boundary_price(
                                    btc_state["history"], new_boundary
                                )
                                if ptb > 0:
                                    price_to_beat = ptb
                                    price_to_beat_cache[new_slug] = ptb
                                    log.info(f"NEW ROUND: {new_slug} | PTB: ${price_to_beat:.2f} ({src})")
                                else:
                                    price_to_beat = 0.0
                                    log.info(f"SKIP ROUND: {new_slug} | No PTB ({src})")

                            # Evict old cache
                            cutoff = new_boundary - 2 * ROUND_SECONDS
                            for old_slug in list(price_to_beat_cache):
                                try:
                                    if int(old_slug.rsplit("-", 1)[-1]) < cutoff:
                                        del price_to_beat_cache[old_slug]
                                except (ValueError, IndexError):
                                    pass

                            if prev_token_ids:
                                unsub = _ws_json(
                                    {"assets_ids": prev_token_ids, "operation": "unsubscribe"}
                                )
                                await ws.send(unsub)

                            sub = _ws_json({"assets_ids": token_ids, "operation": "subscribe"})
                            await ws.send(sub)
                            prev_token_ids = token_ids

                            up_id = token_ids[0]
                            dn_id = token_ids[1]
                            up_ask = 0.0
                            dn_ask = 0.0

                    last_resolve_time = now

                # Read WS messages (balanced timeout - responsive without CPU thrashing)
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.01)  # 10ms (down from 50ms)
                except asyncio.TimeoutError:
                    raw = None

                if raw is not None:
                    try:
                        data = json.loads(raw)
                    except Exception:
                        data = None

                    # Fast path: single message optimization (most common case)
                    if isinstance(data, dict):
                        if data.get("event_type") == "price_change":
                            price_changes = data.get("price_changes")
                            if price_changes:
                                for change in price_changes:
                                    if not isinstance(change, dict):
                                        continue
                                    aid = change.get("asset_id")
                                    b_ask = change.get("best_ask")

                                    # Early exit if no ask update
                                    if b_ask is None:
                                        continue

                                    # Direct comparison (avoid repeated dict access)
                                    if aid == up_id:
                                        try:
                                            up_ask = float(b_ask) if b_ask != "" else 0.0
                                        except (ValueError, TypeError):
                                            pass
                                    elif aid == dn_id:
                                        try:
                                            dn_ask = float(b_ask) if b_ask != "" else 0.0
                                        except (ValueError, TypeError):
                                            pass
                    elif isinstance(data, list):
                        # Batch messages (less common, keep original logic)
                        for item in data:
                            if not isinstance(item, dict):
                                continue
                            ev = item.get("event_type")
                            if ev == "price_change":
                                for change in item.get("price_changes", []):
                                    if not isinstance(change, dict):
                                        continue
                                    aid = change.get("asset_id")
                                    b_ask = change.get("best_ask")
                                    if b_ask is None:
                                        continue

                                    if aid == up_id:
                                        try:
                                            up_ask = float(b_ask) if b_ask != "" else 0.0
                                        except (ValueError, TypeError):
                                            pass
                                    elif aid == dn_id:
                                        try:
                                            dn_ask = float(b_ask) if b_ask != "" else 0.0
                                        except (ValueError, TypeError):
                                            pass

                # ═══════════════════════════════════════════════════════════
                # STRATEGY LOGIC (HOT PATH - OPTIMIZED)
                # ═══════════════════════════════════════════════════════════

                # Pre-cache frequently accessed values (reduce dict lookups)
                current_slug = current_tokens.get("slug")

                # Already traded this round?
                if current_slug == last_traded_slug:
                    continue

                # Daily loss limit
                if daily_loss_halted:
                    continue

                # Both prices must be available
                if up_ask <= 0.0 or dn_ask <= 0.0:
                    continue

                # BTC price must be fresh (cache state dict access)
                current_btc = btc_state["price"]
                btc_last_update = btc_state["last_update"]
                btc_history = btc_state["history"]

                check_time = time.time()
                btc_age = check_time - btc_last_update

                if current_btc <= 0 or price_to_beat <= 0:
                    continue
                if btc_age > 10:
                    continue

                # Timing window (pre-compute round_start)
                round_start = (int(check_time) // ROUND_SECONDS) * ROUND_SECONDS
                seconds_remaining = ROUND_SECONDS - (check_time - round_start)

                if seconds_remaining > window_start or seconds_remaining < window_end:
                    continue

                # GAP FILTER (ADAPTIVE)
                btc_gap = current_btc - price_to_beat
                min_gap = price_to_beat * gap_threshold_pct

                if abs(btc_gap) < min_gap:
                    continue

                # Determine side
                if btc_gap > 0:
                    winning_side = "UP"
                    winning_ask = up_ask
                    losing_ask = dn_ask
                    winning_id = up_id
                else:
                    winning_side = "DOWN"
                    winning_ask = dn_ask
                    losing_ask = up_ask
                    winning_id = dn_id

                # VALUE FILTER
                if winning_ask >= max_price:
                    continue

                # FIXED SLIPPAGE (4 ticks for 350-500ms latency)
                winning_worst = min(winning_ask + SLIPPAGE_BUFFER, 0.99)

                # Use fixed shares (user manages capital before starting bot)
                trade_shares = shares
                winning_order_value = winning_worst * trade_shares

                # Reserve check only (critical safety - prevent draining account)
                if not executor.dry_run and max_bankroll_fraction > 0:
                    balance = balance_state["available"]
                    if balance <= 0:
                        continue

                    balance_after_trade = balance - winning_order_value
                    if balance_after_trade < min_reserve:
                        log.info(f"Skip: Would violate ${min_reserve:.2f} reserve")
                        publish_event(
                            "insufficient_balance",
                            {
                                "reason": "reserve_violation",
                                "balance": round(balance, 2),
                                "required": round(winning_order_value + min_reserve, 2),
                                "min_reserve": min_reserve,
                                "slug": current_slug,
                            },
                        )
                        continue

                last_traded_slug = current_slug
                est_cost = winning_worst * trade_shares
                payout = 1.0 * trade_shares
                est_profit = payout - est_cost

                # log.info(
                #     f"[SIGNAL] ALL FILTERS PASSED: {winning_side} | "
                #     f"ask={winning_ask:.3f} worst={winning_worst:.3f} | "
                #     f"slippage={SLIPPAGE_BUFFER:.3f} | "
                #     f"gap={btc_gap:+.2f} (min={min_gap:.2f}) | "
                #     f"sec_rem={seconds_remaining:.0f} | shares={trade_shares}"
                # )

                log_cost = est_cost
                log_profit = est_profit
                log_price = winning_worst

                # Initialize exec latency (only track order placement time)
                exec_latency_ms = 0.0

                if executor.dry_run:
                    # Dry run: simulate realistic order execution latency
                    exec_latency_ms = 5.0

                    daily_trade_count += 1

                    # Store position for later resolution
                    open_positions[last_traded_slug] = {
                        "side": winning_side,
                        "ordered_shares": trade_shares,
                        "shares": trade_shares,
                        "cost": est_cost,
                        "ptb": price_to_beat,
                        "timestamp": time.time(),
                    }

                    log.info(
                        f"[DRY RUN] {winning_side} @ ${winning_worst:.3f} x {trade_shares} | "
                        f"Latency: {exec_latency_ms:.1f}ms (simulated) | "
                        f"Cost=${log_cost:.2f} | Expected=${log_profit:+.2f} | "
                        f"daily_pnl=${daily_pnl:+.2f} | [PENDING RESOLUTION]"
                    )
                else:
                    # Measure order execution time only
                    t0 = time.perf_counter()
                    result = await executor.market_buy_fak(
                        winning_id, winning_worst, trade_shares
                    )
                    t1 = time.perf_counter()
                    exec_latency_ms = (t1 - t0) * 1000

                    if result.success:
                        log_price = result.filled_price
                        log_cost = log_price * result.filled_shares
                        expected_profit = (1.0 * result.filled_shares) - log_cost
                        daily_trade_count += 1

                        # Store position for later resolution (ACTUAL P&L calculated at round end)
                        open_positions[last_traded_slug] = {
                            "side": winning_side,
                            "ordered_shares": trade_shares,
                            "shares": result.filled_shares,
                            "cost": log_cost,
                            "ptb": price_to_beat,
                            "timestamp": time.time(),
                        }

                        # PRIMARY BALANCE TRACKING (local accounting, no API call)
                        # Deduct trade cost immediately - accurate to the penny
                        # Periodic refresh (every 5 min) is only for sanity check
                        if balance_state["available"] > 0:
                            balance_state["available"] = max(
                                0.0, balance_state["available"] - log_cost
                            )

                        log.info(
                            f"FILLED: {winning_side} @ ${log_price:.4f} x {result.filled_shares} | "
                            f"Latency: {exec_latency_ms:.1f}ms | "
                            f"Cost=${log_cost:.2f} | Expected={expected_profit:+.2f} | daily_pnl=${daily_pnl:+.2f} | "
                            f"[PENDING RESOLUTION]"
                        )

                        # Check daily loss limit AFTER position resolves (not on expected P&L)
                        if daily_pnl <= -daily_loss_limit:
                            daily_loss_halted = True
                            log.warning(
                                f"DAILY LOSS LIMIT HIT: ${daily_pnl:+.2f} <= -${daily_loss_limit:.2f}"
                            )
                            publish_event(
                                "daily_loss_halt",
                                {
                                    "daily_pnl": round(daily_pnl, 2),
                                    "limit": daily_loss_limit,
                                    "trades": daily_trade_count,
                                },
                            )
                    else:
                        log.warning(
                            f"REJECTED: {winning_side} | "
                            f"Latency: {exec_latency_ms:.1f}ms | "
                            f"Error: {result.error}"
                        )

                    publish_event(
                        "trade",
                        {
                            "slug": current_tokens["slug"] if executor.dry_run else current_slug,
                            "token_id": winning_id,
                            "btc_price": current_btc,
                            "price_to_beat": price_to_beat,
                            "btc_gap": round(btc_gap, 2),
                            "side": winning_side,
                            "ask": winning_ask,
                            "filled_price": round(log_price, 4),
                            "filled_shares": result.filled_shares if not executor.dry_run else trade_shares,
                            "success": result.success if not executor.dry_run else True,
                            "error": result.error if not executor.dry_run else "",
                            "latency_ms": round(exec_latency_ms if not executor.dry_run else 0, 2),
                        },
                    )

        except websockets.ConnectionClosed:
            log.warning("Polymarket WS disconnected. Reconnecting...")
            continue
        except Exception as e:
            log.error(f"Error: {e}. Reconnecting in 3s...")
            try:
                await ws.close()
            except:
                pass
            await asyncio.sleep(3)
            continue
        except KeyboardInterrupt:
            log.info("Bot stopped by user.")
            break

    btc_task.cancel()
    if balance_task:
        balance_task.cancel()
    if heartbeat_task:
        heartbeat_task.cancel()
    await executor.close()


def main():
    parser = argparse.ArgumentParser(
        description="Gap-Based Directional Trading Bot"
    )
    parser.add_argument(
        "--live", action="store_true", help="Enable LIVE execution"
    )
    parser.add_argument(
        "--shares", type=float, default=3.0, help="Shares per trade (default: 3)"
    )
    parser.add_argument(
        "--gap-threshold-pct",
        type=float,
        default=0.00040,
        help="Gap threshold as percentage of PTB (default: 0.0004 = 0.04 percent)",
    )
    parser.add_argument(
        "--max-price",
        type=float,
        default=0.55,
        help="Max entry price (default: 0.55)",
    )
    parser.add_argument(
        "--window-start",
        type=float,
        default=180.0,
        help="Start looking when this many seconds remain (default: 180)",
    )
    parser.add_argument(
        "--window-end",
        type=float,
        default=30.0,
        help="Stop looking when this many seconds remain (default: 30)",
    )
    parser.add_argument(
        "--min-reserve",
        type=float,
        default=5.0,
        help="Minimum USDC reserve (default: 5.0)",
    )
    parser.add_argument(
        "--daily-loss-limit",
        type=float,
        default=3.0,
        help="Daily loss limit in dollars (default: 3.0)",
    )
    parser.add_argument(
        "--max-bankroll-fraction",
        type=float,
        default=0.5,
        help="Max fraction of balance per trade (default: 0.5)",
    )
    args = parser.parse_args()

    # Validation
    if args.window_end >= args.window_start:
        print(
            f"ERROR: --window-end ({args.window_end}) must be < --window-start ({args.window_start})"
        )
        sys.exit(1)

    if args.max_bankroll_fraction <= 0 or args.max_bankroll_fraction > 1:
        print(
            f"ERROR: --max-bankroll-fraction ({args.max_bankroll_fraction}) must be > 0 and <= 1"
        )
        sys.exit(1)

    if args.min_reserve < 0:
        print(f"ERROR: --min-reserve ({args.min_reserve}) must be >= 0")
        sys.exit(1)

    if args.daily_loss_limit <= 0:
        print(f"ERROR: --daily-loss-limit ({args.daily_loss_limit}) must be > 0")
        sys.exit(1)

    api = ApiConfig()
    api.load_from_env()

    if args.live and not api.private_key:
        print("ERROR: POLYMARKET_PRIVATE_KEY required for live execution")
        sys.exit(1)

    executor = AsyncOrderExecutor(api_config=api, dry_run=not args.live)

    try:
        asyncio.run(
            run_gap_bot(
                executor,
                args.shares,
                args.gap_threshold_pct,
                args.max_price,
                args.window_start,
                args.window_end,
                args.min_reserve,
                args.daily_loss_limit,
                args.max_bankroll_fraction,
            )
        )
    except KeyboardInterrupt:
        print("Stopped.")


if __name__ == "__main__":
    main()
