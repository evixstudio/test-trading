#!/usr/bin/env python3
"""
Telegram Dispatcher for Gap-Based Directional Bot
==================================================
Runs as a separate process. Listens to the Redis 'gap_bot:events' channel
and sends formatted Telegram alerts for all critical events.

Event types handled:
  - bot_started:          Bot initialization
  - trade:                Order placed (success or rejected)
  - position_resolved:    Position outcome verified at round end (WIN/LOSS)
  - daily_loss_halt:      Daily loss limit triggered
  - daily_summary:        End of day summary
  - insufficient_balance: Balance too low for trading
  - critical_error:       Critical system errors

Usage:
    python telegram_dispatcher_gap_bot.py
"""

import os
import json
import time
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

import requests

try:
    import redis
except ImportError:
    print("Install redis: pip install redis")
    sys.exit(1)

from dotenv import load_dotenv

load_dotenv()

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BalanceAllowanceParams, AssetType

from auto_redeemer import schedule_auto_redeem

# Telegram configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8144343309:AAHA0Y7yTIFO8tfPBTAFKaLrOAcO3zQ3ahE")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "-5040586558")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] Dispatcher: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("telegram-dispatcher")

ET = ZoneInfo("America/New_York")


def slug_to_et(slug: str) -> str:
    """Convert 'btc-updown-5m-{ts}' slug to 'HH:MM-HH:MM ET' range."""
    try:
        ts = int(slug.rsplit("-", 1)[-1])
        start = datetime.fromtimestamp(ts, tz=ET)
        end = datetime.fromtimestamp(ts + 300, tz=ET)
        return f"{start.strftime('%I:%M')}-{end.strftime('%I:%M %p')} ET"
    except (ValueError, IndexError):
        return slug


def ts_to_et_ms(epoch: float) -> str:
    """Format a Unix epoch timestamp as 'HH:MM:SS.mmm ET'."""
    if not epoch or epoch <= 0:
        return "N/A"
    dt = datetime.fromtimestamp(epoch, tz=ET)
    ms = int((epoch % 1) * 1000)
    return f"{dt.strftime('%I:%M:%S')}.{ms:03d} ET"


# ── Polymarket CLOB Client (for auto-redeem + balance) ──────────────────


def init_clob_client() -> ClobClient | None:
    """Initialize CLOB client for trade lookups and auto-redeem."""
    pk = os.getenv("POLYMARKET_PRIVATE_KEY", "")
    proxy = os.getenv("POLYMARKET_PROXY_ADDRESS", "")
    sig_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))

    if not pk:
        log.warning("POLYMARKET_PRIVATE_KEY not set. Auto-redeem disabled.")
        return None

    try:
        client = ClobClient(
            "https://clob.polymarket.com",
            key=pk,
            chain_id=137,
        )
        creds = client.create_or_derive_api_creds()
        client = ClobClient(
            "https://clob.polymarket.com",
            key=pk,
            chain_id=137,
            creds=creds,
            signature_type=sig_type,
            funder=proxy,
        )
        log.info("Polymarket CLOB client initialized for auto-redeem + balance")
        return client
    except Exception as e:
        log.error(f"Failed to init CLOB client: {e}")
        return None


def fetch_account_balance(client: ClobClient | None) -> str:
    """Fetch current USDC balance from Polymarket."""
    if not client:
        return "N/A"
    try:
        sig_type = int(os.getenv("POLYMARKET_SIGNATURE_TYPE", "0"))
        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL, signature_type=sig_type
        )
        ba = client.get_balance_allowance(params)
        balance = float(ba.get("balance", "0")) / 1e6
        return f"${balance:,.2f}"
    except Exception as e:
        log.warning(f"Failed to fetch balance: {e}")
        return "N/A"


# ── Telegram ─────────────────────────────────────────────────────────────


def send_telegram(text: str):
    """Send message to Telegram with retry logic."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured (TOKEN or CHAT_ID missing)")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }

    for attempt in range(3):
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            log.info(f"Telegram sent: {text[:50]}...")
            return
        except requests.exceptions.RequestException as e:
            log.warning(f"Telegram send failed (attempt {attempt + 1}): {e}")
            time.sleep(2)

    log.error("Failed to send Telegram after 3 attempts")


# ── Event Handlers ───────────────────────────────────────────────────────


def handle_bot_started(data: dict):
    """Bot initialization notification."""
    mode = data.get("mode", "UNKNOWN")
    gap_threshold_pct = data.get("gap_threshold_pct", 0) * 100
    max_price = data.get("max_price", 0)
    shares = data.get("shares", 0)
    window = data.get("window", "?")
    min_reserve = data.get("min_reserve", 0)
    daily_loss_limit = data.get("daily_loss_limit", 0)
    btc_price = data.get("btc_price", 0)

    msg = (
        f"🤖 *Bot Started*\n"
        f"Mode: `{mode}`\n"
        f"\n"
        f"Gap: `{gap_threshold_pct:.3f}%` of PTB\n"
        f"Max Price: `${max_price:.2f}` | Shares: `{shares:.1f}`\n"
        f"Window: `{window}` | Reserve: `${min_reserve:.2f}`\n"
        f"Loss Limit: `${daily_loss_limit:.2f}`\n"
        f"\n"
        f"BTC: `${btc_price:,.2f}`\n"
        f"_Ready for trading_"
    )
    send_telegram(msg)


def handle_trade_event(data: dict):
    """Order placed notification (FILLED or REJECTED)."""
    slug = data.get("slug", "?")
    side = data.get("side", "?")
    success = data.get("success", False)
    btc = data.get("btc_price", 0)
    ptb = data.get("price_to_beat", 0)
    gap = data.get("btc_gap", 0)
    ask = data.get("ask", 0)
    winning_worst = data.get("winning_worst", 0)
    order_value = data.get("order_value", 0)
    shares = data.get("shares", 0)
    seconds_remaining = data.get("seconds_remaining", 0)
    up_ask = data.get("up_ask", 0)
    dn_ask = data.get("dn_ask", 0)
    balance_val = data.get("balance", 0)
    filled_price = data.get("filled_price", 0)
    filled_shares = data.get("filled_shares", 0)
    latency = data.get("latency_ms", 0)
    build_ms = data.get("build_ms", 0)
    sign_ms = data.get("sign_ms", 0)
    post_ms = data.get("post_ms", 0)
    error = data.get("error", "")
    retry_count = data.get("retry_count", 0)
    max_retries = data.get("max_retries", 0)
    drain_count = data.get("drain_count", 0)
    drain_ms = data.get("drain_ms", 0)
    eval_ms = data.get("eval_ms", 0)

    time_str = slug_to_et(slug)

    if success:
        cost = filled_price * filled_shares
        expected_pnl = (1.0 * filled_shares) - cost
        saved_per_sh = winning_worst - filled_price

        msg = (
            f"✅ *FILLED* — *{side}*\n"
            f"Round: `{time_str}`\n"
            f"\n"
            f"BTC: `${btc:,.2f}` vs PTB `${ptb:,.2f}`\n"
            f"Gap: `${gap:+.0f}` | Sec Left: `{seconds_remaining:.0f}s`\n"
            f"\n"
            f"Fill: `${filled_price:.4f}` × `{filled_shares:.1f}` sh = `${cost:.2f}`\n"
            f"Worst: `${winning_worst:.4f}` (saved `${saved_per_sh:.4f}`/sh)\n"
            f"Expected P&L: `${expected_pnl:+.2f}`\n"
            f"\n"
            f"Eval: `{eval_ms:.1f}ms` | Latency: `{latency:.0f}ms` _(build={build_ms:.0f} sign={sign_ms:.0f} post={post_ms:.0f})_\n"
            f"Balance: `${balance_val:.2f}`\n"
            f"\n"
            f"_Pending resolution at round end_"
        )
    else:
        remaining = max_retries - retry_count
        drain_line = f"_Drained {drain_count} events in {drain_ms:.1f}ms_" if drain_count > 0 else "_No stale events drained_"

        if remaining > 0:
            footer = f"_Will retry ({remaining} attempt{'s' if remaining != 1 else ''} remaining)_"
        else:
            footer = "_Retries exhausted — no position this round_"

        msg = (
            f"❌ *REJECTED ({retry_count}/{max_retries})* — *{side}*\n"
            f"Round: `{time_str}`\n"
            f"\n"
            f"BTC: `${btc:,.2f}` vs PTB `${ptb:,.2f}`\n"
            f"Gap: `${gap:+.0f}` | Sec Left: `{seconds_remaining:.0f}s`\n"
            f"\n"
            f"Ask: `${ask:.4f}` → Worst: `${winning_worst:.4f}`\n"
            f"Shares: `{shares:.1f}` | Value: `${order_value:.2f}`\n"
            f"\n"
            f"Book (post-drain):\n"
            f"UP: `${up_ask:.4f}` | DN: `${dn_ask:.4f}`\n"
            f"{drain_line}\n"
            f"\n"
            f"Error: `{error}`\n"
            f"Eval: `{eval_ms:.1f}ms` | Latency: `{latency:.0f}ms` _(build={build_ms:.0f} sign={sign_ms:.0f} post={post_ms:.0f})_\n"
            f"Balance: `${balance_val:.2f}`\n"
            f"\n"
            f"{footer}"
        )

    send_telegram(msg)


def handle_position_resolved(data: dict, clob_client=None):
    """Position outcome notification (WIN/LOSS) with auto-redeem."""
    slug = data.get("slug", "?")
    side = data.get("side", "?")
    outcome = data.get("outcome", "?")
    won = data.get("won", False)
    ordered_shares = data.get("ordered_shares", 0)
    effective_shares = data.get("effective_shares", 0)
    fee_shares = data.get("fee_shares", 0)
    fill_price = data.get("fill_price", 0)
    cost = data.get("cost", 0)
    payout = data.get("payout", 0)
    pnl = data.get("pnl", 0)
    final_btc = data.get("final_btc", 0)
    ptb = data.get("ptb", 0)
    daily_pnl = data.get("daily_pnl", 0)

    time_str = slug_to_et(slug)

    if won:
        emoji = "🎯"
        result = "WIN"
    else:
        emoji = "💔"
        result = "LOSS"

    msg = (
        f"{emoji} *{result}* — *{side}*\n"
        f"Round: `{time_str}`\n"
        f"\n"
        f"P&L: `${pnl:+.2f}`\n"
        f"Daily P&L: `${daily_pnl:+.2f}`\n"
        f"\n"
        f"BTC Final: `${final_btc:,.2f}` vs PTB `${ptb:,.2f}`\n"
        f"Side: *{side}* → Outcome: *{outcome}*\n"
        f"\n"
        f"Ordered: `{ordered_shares:.1f}` sh @ `${fill_price:.4f}`\n"
        f"Fee: `{fee_shares:.3f}` sh | Net: `{effective_shares:.3f}` sh\n"
        f"Cost: `${cost:.2f}` → Payout: `${payout:.2f}`"
    )

    if won:
        msg += f"\n\n_Auto-redeem scheduled_"

    send_telegram(msg)

    if won:
        schedule_auto_redeem(slug, won, slug_to_et, send_telegram, clob_client=clob_client)


def handle_daily_loss_halt(data: dict):
    """Daily loss limit triggered notification."""
    daily_pnl = data.get("daily_pnl", 0)
    limit = data.get("limit", 0)
    trades = data.get("trades", 0)

    msg = (
        f"🛑 *DAILY LOSS LIMIT HIT*\n"
        f"Daily P&L: `${daily_pnl:.2f}`\n"
        f"Limit: `${-limit:.2f}`\n"
        f"Trades Today: `{trades}`\n"
        f"_Trading halted until UTC midnight_"
    )
    send_telegram(msg)


def handle_daily_summary(data: dict):
    """End of day summary notification."""
    date = data.get("date", "?")
    pnl = data.get("pnl", 0)
    trades = data.get("trades", 0)

    emoji = "📈" if pnl > 0 else "📉" if pnl < 0 else "➡️"

    msg = (
        f"{emoji} *Daily Summary*\n"
        f"Date: `{date}`\n"
        f"P&L: `${pnl:+.2f}`\n"
        f"Trades: `{trades}`\n"
        f"Avg P&L: `${pnl/trades if trades > 0 else 0:.2f}`"
    )
    send_telegram(msg)


def handle_insufficient_balance(data: dict):
    """Insufficient balance notification (rate-limited to avoid spam)."""
    reason = data.get("reason", "unknown")
    balance = data.get("balance", 0)
    required = data.get("required", 0)
    slug = data.get("slug", "?")

    # Rate limiting: only send once per 5 minutes
    now = time.time()
    if hasattr(handle_insufficient_balance, '_last_alert'):
        if now - handle_insufficient_balance._last_alert < 300:  # 5 minutes
            return
    handle_insufficient_balance._last_alert = now

    time_str = slug_to_et(slug)

    if reason == "reserve_violation":
        msg = (
            f"⚠️ *Insufficient Balance*\n"
            f"Round: `{time_str}`\n"
            f"Reason: Reserve protection\n"
            f"Balance: `${balance:.2f}`\n"
            f"Required: `${required:.2f}`\n"
            f"_Add funds to continue trading_"
        )
    else:
        msg = (
            f"⚠️ *Insufficient Balance*\n"
            f"Round: `{time_str}`\n"
            f"Reason: Below minimum order\n"
            f"Balance: `${balance:.2f}`\n"
            f"_Add funds to continue trading_"
        )
    send_telegram(msg)


def handle_critical_error(data: dict):
    """Critical error notification."""
    error = data.get("error", "Unknown error")
    action = data.get("action", "Unknown action")

    msg = (
        f"🚨 *CRITICAL ERROR*\n"
        f"Error: `{error}`\n"
        f"Action: `{action}`\n"
        f"_Check bot immediately!_"
    )
    send_telegram(msg)


# ── Main Event Loop ──────────────────────────────────────────────────────


def main():
    """Main event loop - subscribe to Redis and dispatch events."""
    log.info("Starting Gap Bot Telegram Dispatcher...")

    # Initialize rate limit tracker
    handle_insufficient_balance._last_alert = 0

    # Initialize CLOB client for auto-redeem + balance
    clob_client = init_clob_client()

    try:
        r = redis.Redis(host="localhost", port=6379, db=3, decode_responses=True)
        r.ping()
        pubsub = r.pubsub()
        pubsub.subscribe("gap_bot:events")
        log.info("Subscribed to Redis channel 'gap_bot:events'")
    except Exception as e:
        log.error(f"Failed to connect to Redis: {e}")
        return

    balance_str = fetch_account_balance(clob_client)
    send_telegram(
        f"*Telegram Dispatcher Started*\n"
        f"Listening for gap bot events...\n"
        f"Balance: `{balance_str}`\n"
        f"Auto-redeem: `{'ENABLED' if clob_client and os.getenv('POLY_BUILDER_API_KEY') else 'DISABLED'}`"
    )

    log.info("Listening for events...")
    while True:
        try:
            message = pubsub.get_message(timeout=1.0)
            if message and message["type"] == "message":
                data = json.loads(message["data"])
                event_type = data.get("type")

                log.info(f"Received event: {event_type}")

                # Dispatch to appropriate handler
                if event_type == "bot_started":
                    handle_bot_started(data)
                elif event_type == "trade":
                    handle_trade_event(data)
                elif event_type == "position_resolved":
                    handle_position_resolved(data, clob_client=clob_client)
                elif event_type == "daily_loss_halt":
                    handle_daily_loss_halt(data)
                elif event_type == "daily_summary":
                    handle_daily_summary(data)
                elif event_type == "insufficient_balance":
                    handle_insufficient_balance(data)
                elif event_type == "critical_error":
                    handle_critical_error(data)
                else:
                    log.warning(f"Unknown event type: {event_type}")

        except KeyboardInterrupt:
            log.info("Shutting down...")
            break
        except Exception as e:
            log.error(f"Error processing message: {e}")
            time.sleep(1)


if __name__ == "__main__":
    main()
