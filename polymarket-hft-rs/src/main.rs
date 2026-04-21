#![allow(dead_code)]

mod binance_feed;
mod btc_tracker;
mod config;
mod direct_post;
mod executor;
mod gap_engine;
mod orderbook;
mod redis_bus;
mod token_resolver;
mod types;
mod ws_feed;

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use clap::Parser;
use rust_decimal::prelude::ToPrimitive;
use tracing::{debug, info, warn};

use crate::binance_feed::spawn_binance_feed;
use crate::btc_tracker::lookup_boundary_price;
use crate::config::{ApiConfig, Cli, MIN_ORDER_VALUE, ROUND_SECONDS};

const MAX_RETRIES_PER_ROUND: u32 = 5;
use crate::direct_post::FastOrderClient;
use crate::executor::{AuthenticatedClient, Executor};
use crate::orderbook::ShadowBook;
use crate::redis_bus::RedisBus;
use crate::token_resolver::near_boundary;
use crate::types::{OpenPosition, TokenPair};
use crate::ws_feed::{WsFeed, WsOutcome};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    dotenvy::dotenv().ok();

    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .init();

    info!("======================================================================");
    info!("  POLYMARKET GAP-BASED DIRECTIONAL BOT (Rust)");
    info!("  Mode: {}", if cli.live { "LIVE" } else { "DRY-RUN" });
    info!("  Shares: {} | Gap: {:.4}%", cli.shares, cli.gap_threshold_pct * 100.0);
    info!("  Max Price: ${:.2} | Window: {}-{}s", cli.max_price, cli.window_end, cli.window_start);
    info!("  Reserve: ${:.2} | Daily Loss Limit: ${:.2}", cli.min_reserve, cli.daily_loss_limit);
    info!("======================================================================");

    // Validate parameters
    if cli.window_end >= cli.window_start {
        anyhow::bail!("window_end must be < window_start");
    }

    let api_config = ApiConfig::from_env()?;

    if cli.live && api_config.private_key.is_empty() {
        anyhow::bail!("POLYMARKET_PRIVATE_KEY required for live execution");
    }

    let executor = Executor::new(&api_config, !cli.live)?;

    let client: Option<AuthenticatedClient> = if cli.live {
        Some(executor.authenticate().await?)
    } else {
        info!("DRY-RUN mode: skipping CLOB authentication");
        None
    };

    let fast_client: Option<FastOrderClient> = if let Some(ref c) = client {
        let fc = FastOrderClient::from_authenticated(c)?;
        info!("FastOrderClient created (optimized HTTP/2, direct POST)");
        if let Ok(elapsed) = fc.prewarm().await {
            info!("FastOrderClient pre-warmed in {:.1}ms", elapsed.as_secs_f64() * 1000.0);
        }
        Some(fc)
    } else {
        None
    };

    let mut redis = RedisBus::new();
    if let Err(e) = redis.connect(&cli.redis_host, cli.redis_port).await {
        warn!("Redis connection failed: {e} -- continuing without monitoring");
    }

    let http = reqwest::Client::builder()
        .pool_max_idle_per_host(2)
        .timeout(Duration::from_secs(10))
        .tcp_nodelay(true)
        .build()?;

    run_gap_loop(&executor, client.as_ref(), fast_client.as_ref(), &redis, &http, cli).await;

    Ok(())
}

async fn run_gap_loop(
    executor: &Executor,
    client: Option<&AuthenticatedClient>,
    fast_client: Option<&FastOrderClient>,
    redis: &RedisBus,
    http: &reqwest::Client,
    cli: Cli,
) {
    let is_live = client.is_some();

    // Spawn Binance BTC feed
    let (btc_rx, btc_history) = spawn_binance_feed();

    // Wait for BTC price
    info!("Waiting for BTC price...");
    for _ in 0..75 {
        let btc = *btc_rx.borrow();
        if btc.price > 0.0 {
            info!("BTC price active: ${:.2}", btc.price);
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let btc = *btc_rx.borrow();
    if btc.price <= 0.0 {
        warn!("Could not get BTC price after 15s. Exiting.");
        redis.publish_event(
            "critical_error",
            &serde_json::json!({"error": "No BTC price", "action": "Bot exiting"}),
        );
        return;
    }

    redis.publish_event(
        "bot_started",
        &serde_json::json!({
            "mode": if is_live { "LIVE" } else { "DRY_RUN" },
            "gap_threshold_pct": cli.gap_threshold_pct,
            "max_price": cli.max_price,
            "shares": cli.shares,
            "window": format!("{}-{}s", cli.window_end, cli.window_start),
            "min_reserve": cli.min_reserve,
            "daily_loss_limit": cli.daily_loss_limit,
            "btc_price": btc.price,
        }),
    );

    // Balance tracking (local accounting for accuracy)
    let mut balance = 0.0_f64;
    if is_live {
        if let Some(c) = client {
            match c.balance_allowance(
                polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest::builder()
                    .asset_type(polymarket_client_sdk::clob::types::AssetType::Collateral)
                    .build(),
            )
            .await
            {
                Ok(ba) => {
                    let raw_balance = ba.balance.to_f64().unwrap_or(0.0);
                    balance = raw_balance / 1_000_000.0;
                    info!("Initial balance fetched: ${:.2}", balance);
                }
                Err(e) => {
                    warn!("Failed to fetch initial balance: {e}");
                }
            }
        } else {
            warn!("Client is None, cannot fetch balance");
        }
    }

    // Daily P&L tracking
    let mut daily_pnl = 0.0;
    let mut daily_trade_count = 0u32;
    let mut daily_loss_halted = false;
    let mut daily_pnl_date = chrono::Utc::now().date_naive();

    // Position tracking
    let mut open_positions: HashMap<String, OpenPosition> = HashMap::new();
    let mut price_to_beat_cache: HashMap<String, f64> = HashMap::new();
    let mut traded_this_round = false;
    let mut round_retry_count: u32 = 0;
    let mut need_balance_sync = false;
    let mut last_rejected_up_ask: f64 = 0.0;
    let mut last_rejected_dn_ask: f64 = 0.0;

    let mut last_heartbeat = Instant::now();
    let mut last_prewarm = Instant::now();
    let mut ws = WsFeed::new();

    loop {
        if let Err(e) = ws.connect().await {
            warn!("WS connect failed: {e}. Retrying in 3s...");
            tokio::time::sleep(Duration::from_secs(3)).await;
            continue;
        }

        let mut current_tokens: Option<TokenPair>;
        let mut price_to_beat = 0.0;

        match token_resolver::resolve_tokens(http, None).await {
            Ok(tokens) => {
                let slug = &tokens.slug;
                info!("ROUND: {} | UP={} DN={}", slug, &tokens.up_id_str[..8], &tokens.dn_id_str[..8]);

                // Try to get PTB from cache or lookup
                if let Some(&cached_ptb) = price_to_beat_cache.get(slug) {
                    price_to_beat = cached_ptb;
                    info!("PTB: ${:.2} (cached)", price_to_beat);
                } else {
                    let hist = btc_history.lock().await;
                    let lookup = lookup_boundary_price(&hist, tokens.boundary as f64);
                    drop(hist);

                    if lookup.valid() {
                        price_to_beat = lookup.price;
                        price_to_beat_cache.insert(slug.clone(), price_to_beat);
                        info!("PTB: ${:.2} ({})", price_to_beat, lookup.source);
                    } else {
                        info!("SKIP ROUND: {} | No PTB ({})", slug, lookup.source);
                    }
                }

                let ids = [tokens.up_id_str.as_str(), tokens.dn_id_str.as_str()];
                if let Err(e) = ws.subscribe_initial(&ids).await {
                    warn!("WS subscribe failed: {e}");
                    ws.close().await;
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
                current_tokens = Some(tokens);
            }
            Err(e) => {
                warn!("Token resolve failed: {e}. Retrying in 5s...");
                ws.close().await;
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        }

        let mut book = ShadowBook::default();
        let mut last_resolve_check = Instant::now();

        loop {
            let eval_start = Instant::now();

            // Heartbeat + book state summary
            if last_heartbeat.elapsed() >= Duration::from_secs(5) {
                redis.update_heartbeat(balance);
                let book_age_ms = book.last_book_event.elapsed().as_millis();
                info!(
                    "BOOK: up={:.4}/{:.2} dn={:.4}/{:.2} | age={}ms | guards: rej_up={:.4} rej_dn={:.4} | retries={}/{}",
                    book.up_ask, book.up_ask_size, book.dn_ask, book.dn_ask_size,
                    book_age_ms,
                    last_rejected_up_ask, last_rejected_dn_ask,
                    round_retry_count, MAX_RETRIES_PER_ROUND,
                );
                last_heartbeat = Instant::now();
            }

            // Keep FastOrderClient HTTP/2 connection warm (prevents TLS cold-start on trade)
            // With HTTP/2 keep-alive enabled, prewarm every 60s for observability
            if let Some(fc) = fast_client {
                if last_prewarm.elapsed() >= Duration::from_secs(60) {
                    match fc.prewarm().await {
                        Ok(elapsed) => {
                            let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
                            info!("PREWARM: fast_client in {:.1}ms", elapsed_ms);
                            // Warn if unusually slow (indicates connection was cold)
                            if elapsed_ms > 50.0 {
                                warn!("PREWARM SLOW: {:.1}ms (expected <30ms, connection may have been dropped)", elapsed_ms);
                            }
                        }
                        Err(e) => warn!("PREWARM: fast_client failed: {e}"),
                    }
                    last_prewarm = Instant::now();
                }
            }

            // Daily P&L reset at UTC midnight
            let today_utc = chrono::Utc::now().date_naive();
            if today_utc != daily_pnl_date {
                info!(
                    "UTC DAY ROLLOVER | PnL: ${:+.2} ({} trades)",
                    daily_pnl, daily_trade_count
                );
                redis.publish_event(
                    "daily_summary",
                    &serde_json::json!({
                        "date": daily_pnl_date.to_string(),
                        "pnl": daily_pnl,
                        "trades": daily_trade_count,
                    }),
                );
                daily_pnl = 0.0;
                daily_trade_count = 0;
                daily_pnl_date = today_utc;
                daily_loss_halted = false;
            }

            // Check for round change
            if last_resolve_check.elapsed() >= Duration::from_secs(5) {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                if near_boundary(now, 8) {
                    if let Ok(new_tokens) = token_resolver::resolve_tokens(http, None).await {
                        let cur_slug = current_tokens
                            .as_ref()
                            .map(|t| t.slug.as_str())
                            .unwrap_or("");

                        if new_tokens.slug != cur_slug {
                            // Round boundary - resolve old position
                            if !cur_slug.is_empty() {
                                if let Some(pos) = open_positions.remove(cur_slug) {
                                    // Get final BTC price at boundary
                                    let hist = btc_history.lock().await;
                                    let lookup =
                                        lookup_boundary_price(&hist, new_tokens.boundary as f64);
                                    drop(hist);

                                    let final_btc = if lookup.valid() {
                                        lookup.price
                                    } else {
                                        let btc = *btc_rx.borrow();
                                        warn!(
                                            "Boundary price lookup failed ({}), using current BTC=${:.2}",
                                            lookup.source, btc.price
                                        );
                                        btc.price
                                    };

                                    let pnl_result = pos.calculate_pnl(final_btc);
                                    daily_pnl += pnl_result.pnl;

                                    let outcome = if final_btc > pos.ptb { "UP" } else { "DOWN" };
                                    info!(
                                        "RESOLVED: {} | {} | Outcome={} | BTC=${:.2} vs PTB=${:.2} | P&L={:+.2} | daily_pnl=${:+.2}",
                                        cur_slug, pos.side, outcome, final_btc, pos.ptb, pnl_result.pnl, daily_pnl
                                    );

                                    redis.publish_event(
                                        "position_resolved",
                                        &serde_json::json!({
                                            "slug": cur_slug,
                                            "side": pos.side,
                                            "outcome": outcome,
                                            "won": pnl_result.won,
                                            "ordered_shares": pos.ordered_shares,
                                            "fill_price": pnl_result.fill_price,
                                            "fee_shares": pnl_result.fee_shares,
                                            "effective_shares": pnl_result.effective_shares,
                                            "cost": pos.cost,
                                            "payout": pnl_result.payout,
                                            "pnl": pnl_result.pnl,
                                            "final_btc": final_btc,
                                            "ptb": pos.ptb,
                                            "daily_pnl": daily_pnl,
                                        }),
                                    );
                                }

                                info!("ROUND BOUNDARY | daily_pnl=${:+.2}", daily_pnl);

                                // Sync balance if we traded in previous round
                                if need_balance_sync && is_live {
                                    if let Some(c) = client {
                                        match c.balance_allowance(
                                            polymarket_client_sdk::clob::types::request::BalanceAllowanceRequest::builder()
                                                .asset_type(polymarket_client_sdk::clob::types::AssetType::Collateral)
                                                .build(),
                                        )
                                        .await
                                        {
                                            Ok(ba) => {
                                                let api_balance = ba.balance.to_f64().unwrap_or(0.0) / 1_000_000.0;
                                                let drift = api_balance - balance;
                                                if drift.abs() > 0.01 {
                                                    warn!(
                                                        "Balance drift: API=${:.2} vs Local=${:.2} (Δ=${:+.2})",
                                                        api_balance, balance, drift
                                                    );
                                                }
                                                balance = api_balance;
                                                info!("Balance synced: ${:.2}", balance);
                                                need_balance_sync = false;
                                            }
                                            Err(e) => {
                                                warn!("Balance sync failed (will retry next round): {e}");
                                                // Keep flag set to retry next round
                                            }
                                        }
                                    }
                                }
                            }

                            // New round setup
                            let new_slug = &new_tokens.slug;
                            if let Some(&cached_ptb) = price_to_beat_cache.get(new_slug) {
                                price_to_beat = cached_ptb;
                                info!("NEW ROUND: {} | PTB: ${:.2} (cached)", new_slug, price_to_beat);
                            } else {
                                let hist = btc_history.lock().await;
                                let lookup =
                                    lookup_boundary_price(&hist, new_tokens.boundary as f64);
                                drop(hist);

                                if lookup.valid() {
                                    price_to_beat = lookup.price;
                                    price_to_beat_cache.insert(new_slug.clone(), price_to_beat);
                                    info!(
                                        "NEW ROUND: {} | PTB: ${:.2} ({})",
                                        new_slug, price_to_beat, lookup.source
                                    );
                                } else {
                                    price_to_beat = 0.0;
                                    info!(
                                        "SKIP ROUND: {} | No PTB ({})",
                                        new_slug, lookup.source
                                    );
                                }
                            }

                            // Evict old cache entries
                            let cutoff = new_tokens.boundary.saturating_sub(2 * ROUND_SECONDS);
                            price_to_beat_cache.retain(|slug, _| {
                                slug.rsplit('-')
                                    .next()
                                    .and_then(|ts| ts.parse::<u64>().ok())
                                    .map_or(false, |ts| ts >= cutoff)
                            });

                            // Unsubscribe old, subscribe new
                            if let Some(ref old) = current_tokens {
                                let old_ids = [old.up_id_str.as_str(), old.dn_id_str.as_str()];
                                let _ = ws.unsubscribe(&old_ids).await;
                            }

                            let new_ids =
                                [new_tokens.up_id_str.as_str(), new_tokens.dn_id_str.as_str()];
                            if let Err(e) = ws.subscribe(&new_ids).await {
                                warn!("WS subscribe failed on round change: {e}");
                                break;
                            }

                            book.reset();
                            traded_this_round = false;
                            round_retry_count = 0;
                            last_rejected_up_ask = 0.0;
                            last_rejected_dn_ask = 0.0;
                            current_tokens = Some(new_tokens);
                        }
                    }
                }
                last_resolve_check = Instant::now();
            }

            let tokens = match &current_tokens {
                Some(t) => t,
                None => break,
            };

            match ws
                .next_update(
                    &mut book,
                    &tokens.up_id_str,
                    &tokens.dn_id_str,
                    Duration::from_millis(10),
                )
                .await
            {
                Ok(WsOutcome::BookUpdated) => {}
                Ok(_) => continue,
                Err(e) => {
                    warn!("WS disconnected: {e}. Reconnecting...");
                    break;
                }
            }

            // ═══════════════════════════════════════════════════════════
            // STRATEGY LOGIC (HOT PATH)
            // ═══════════════════════════════════════════════════════════

            // Filter: Already traded or retries exhausted this round?
            if traded_this_round {
                continue;
            }
            if round_retry_count >= MAX_RETRIES_PER_ROUND {
                continue;
            }

            // Filter: Daily loss halted?
            if daily_loss_halted {
                continue;
            }

            // Get current BTC state
            let btc = *btc_rx.borrow();
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();
            let btc_age = now - btc.timestamp;
            let round_start = (now as u64 / ROUND_SECONDS) * ROUND_SECONDS;

            // Per-side guard: skip if current ask matches a recently rejected price
            let btc_gap_dir = btc.price - price_to_beat;
            let (current_ask, rejected_ask) = if btc_gap_dir > 0.0 {
                (book.up_ask, last_rejected_up_ask)
            } else {
                (book.dn_ask, last_rejected_dn_ask)
            };
            if rejected_ask > 0.0 && (current_ask - rejected_ask).abs() < 0.0001 {
                debug!(
                    "SKIP(stale-ask): {} | ask={:.4} == rejected={:.4}",
                    if btc_gap_dir > 0.0 { "UP" } else { "DOWN" }, current_ask, rejected_ask,
                );
                continue;
            }

            // Check gap signal
            let signal = match gap_engine::check_gap(
                &book,
                btc.price,
                btc_age,
                price_to_beat,
                now,
                round_start,
                cli.gap_threshold_pct,
                cli.max_price,
                cli.window_start,
                cli.window_end,
                cli.shares,
            ) {
                Some(s) => s,
                None => continue,
            };

            // Polymarket minimum order value check ($1.00)
            let order_value = signal.winning_worst * signal.shares;
            if order_value < MIN_ORDER_VALUE {
                continue;
            }

            // Reserve check (critical safety)
            if is_live {
                if balance <= 0.0 || (balance - order_value) < cli.min_reserve {
                    info!(
                        "Skip: Reason: balance={:.2}, order_value={:.2}, remaining={:.2}, min_reserve={:.2}",
                        balance, order_value, balance - order_value, cli.min_reserve
                    );
                    redis.publish_event(
                        "insufficient_balance",
                        &serde_json::json!({
                            "reason": "reserve_violation",
                            "balance": balance,
                            "required": order_value + cli.min_reserve,
                            "min_reserve": cli.min_reserve,
                            "slug": tokens.slug,
                        }),
                    );
                    continue;
                }
            }

            let trade_token_id = if signal.is_up {
                tokens.up_id
            } else {
                tokens.dn_id
            };

            // Execute trade
            if !is_live {
                // Dry run
                traded_this_round = true;
                daily_trade_count += 1;

                let cost = signal.winning_worst * signal.shares;
                open_positions.insert(
                    tokens.slug.clone(),
                    OpenPosition {
                        side: signal.side.to_string(),
                        ordered_shares: signal.shares,
                        cost,
                        ptb: price_to_beat,
                        timestamp: now,
                    },
                );

                info!(
                    "[DRY RUN] {} @ ${:.3} x {} | Gap={:+.2} | Sec={:.0} | Cost=${:.2} | daily_pnl=${:+.2}",
                    signal.side, signal.winning_worst, signal.shares, signal.btc_gap,
                    signal.seconds_remaining, cost, daily_pnl
                );
            } else {
                // Live execution
                let eval_ms = eval_start.elapsed().as_secs_f64() * 1000.0;

                let (result, latency) =
                    executor.market_buy_fak(fast_client.unwrap(), trade_token_id, signal.winning_worst, signal.shares).await;

                let mut drain_count = 0u32;
                let mut drain_ms = 0.0f64;

                if result.success {
                    traded_this_round = true;
                    last_rejected_up_ask = 0.0;
                    last_rejected_dn_ask = 0.0;
                    let cost = result.filled_price * result.filled_shares;
                    daily_trade_count += 1;

                    // Store position for resolution
                    open_positions.insert(
                        tokens.slug.clone(),
                        OpenPosition {
                            side: signal.side.to_string(),
                            ordered_shares: result.filled_shares,
                            cost,
                            ptb: price_to_beat,
                            timestamp: now,
                        },
                    );

                    // Mark for balance sync at next round boundary
                    need_balance_sync = true;

                    let expected_profit = (1.0 * result.filled_shares) - cost;
                    info!(
                        "FILLED: {} @ ${:.4} x {} | Latency: {:.1}ms (build={:.1} sign={:.1} post={:.1}) | Cost=${:.2} | Expected={:+.2} | daily_pnl=${:+.2}",
                        signal.side, result.filled_price, result.filled_shares,
                        latency.total_ms, latency.build_ms, latency.sign_ms, latency.post_ms,
                        cost, expected_profit, daily_pnl
                    );

                    // Check daily loss limit after position resolves
                    if daily_pnl <= -cli.daily_loss_limit {
                        daily_loss_halted = true;
                        warn!(
                            "DAILY LOSS LIMIT HIT: ${:+.2} <= -${:.2}",
                            daily_pnl, cli.daily_loss_limit
                        );
                        redis.publish_event(
                            "daily_loss_halt",
                            &serde_json::json!({
                                "daily_pnl": daily_pnl,
                                "limit": cli.daily_loss_limit,
                                "trades": daily_trade_count,
                            }),
                        );
                    }
                } else {
                    round_retry_count += 1;
                    let order_value = signal.winning_worst * signal.shares;
                    let ask_size = if signal.is_up { book.up_ask_size } else { book.dn_ask_size };
                    warn!(
                        "REJECTED ({}/{}): {} | Ask=${:.4} AskSize={:.2} Worst=${:.4} | Shares={:.1} OrderVal=${:.2} | BTC=${:.2} PTB=${:.2} Gap={:+.0} | Sec={:.0} | Bal=${:.2} | Latency: {:.1}ms (build={:.1} sign={:.1} post={:.1}) | Error: {}",
                        round_retry_count, MAX_RETRIES_PER_ROUND,
                        signal.side, signal.winning_ask, ask_size, signal.winning_worst,
                        signal.shares, order_value,
                        btc.price, price_to_beat, signal.btc_gap,
                        signal.seconds_remaining, balance,
                        latency.total_ms, latency.build_ms, latency.sign_ms, latency.post_ms,
                        result.error
                    );

                    if signal.is_up {
                        last_rejected_up_ask = signal.winning_ask;
                        book.invalidate_side(true);
                    } else {
                        last_rejected_dn_ask = signal.winning_ask;
                        book.invalidate_side(false);
                    }
                    info!(
                        "GUARD ARMED: {} ask={:.4} — will skip until new price or book event",
                        signal.side, signal.winning_ask,
                    );

                    // Drain stale WS events that accumulated during the blocking
                    // FAK POST (typically 500ms–3s).  Without this, the next
                    // iteration reads the oldest buffered frame which can be seconds
                    // old, causing the bot to act on a stale book and waste retries.
                    //
                    // Loop exits when:
                    //   Timeout   → buffer empty (normal exit, typically <50ms)
                    //   Err       → WS disconnected (outer loop handles reconnect)
                    //   time cap  → safety valve (100ms hard limit)
                    let drain_start = Instant::now();
                    let max_drain = Duration::from_millis(100);

                    loop {
                        if drain_start.elapsed() > max_drain {
                            warn!(
                                "WS drain hit safety cap ({}ms, {} events processed)",
                                max_drain.as_millis(), drain_count,
                            );
                            break;
                        }
                        match ws.next_update(
                            &mut book,
                            &tokens.up_id_str,
                            &tokens.dn_id_str,
                            Duration::from_millis(2),
                        ).await {
                            Ok(WsOutcome::BookUpdated)  => { drain_count += 1; }
                            Ok(WsOutcome::MessageNoOp)  => {}
                            Ok(WsOutcome::Timeout)      => break,
                            Err(e) => {
                                warn!("WS error during post-rejection drain: {e}");
                                break;
                            }
                        }
                    }

                    drain_ms = drain_start.elapsed().as_secs_f64() * 1000.0;
                    if drain_count > 0 {
                        info!(
                            "POST-REJECTION DRAIN: {} book updates in {:.1}ms | book: up={:.4}/{:.2} dn={:.4}/{:.2}",
                            drain_count, drain_ms,
                            book.up_ask, book.up_ask_size, book.dn_ask, book.dn_ask_size,
                        );
                    }
                }

                redis.publish_event(
                    "trade",
                    &serde_json::json!({
                        "slug": tokens.slug,
                        "token_id": trade_token_id.to_string(),
                        "btc_price": btc.price,
                        "price_to_beat": price_to_beat,
                        "btc_gap": signal.btc_gap,
                        "side": signal.side,
                        "ask": signal.winning_ask,
                        "winning_worst": signal.winning_worst,
                        "order_value": signal.winning_worst * signal.shares,
                        "shares": signal.shares,
                        "seconds_remaining": signal.seconds_remaining,
                        "up_ask": book.up_ask,
                        "dn_ask": book.dn_ask,
                        "balance": balance,
                        "filled_price": result.filled_price,
                        "filled_shares": result.filled_shares,
                        "success": result.success,
                        "error": result.error,
                        "retry_count": round_retry_count,
                        "max_retries": MAX_RETRIES_PER_ROUND,
                        "latency_ms": latency.total_ms,
                        "build_ms": latency.build_ms,
                        "sign_ms": latency.sign_ms,
                        "post_ms": latency.post_ms,
                        "drain_count": drain_count,
                        "drain_ms": drain_ms,
                        "eval_ms": eval_ms,
                    }),
                );
            }
        }

        ws.close().await;
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}
