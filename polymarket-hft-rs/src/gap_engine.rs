use tracing::info;

use crate::config::ROUND_SECONDS;
use crate::orderbook::ShadowBook;
use crate::types::GapSignal;

/// Fixed slippage buffer: 2 ticks (0.02)
const SLIPPAGE_BUFFER: f64 = 0.30;
/// Check gap-based directional trade opportunity.
/// Returns Some(GapSignal) if all filters pass, None otherwise.
/// HOT PATH - zero allocation, no locks, inline everything.
#[inline]
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
) -> Option<GapSignal> {
    // Filter 1: Both prices must be available
    if book.up_ask <= 0.0 || book.dn_ask <= 0.0 {
        return None;
    }

    // Filter 2: BTC price must be fresh (< 10s old)
    if btc_price <= 0.0 || price_to_beat <= 0.0 || btc_age > 10.0 {
        return None;
    }

    // Filter 3: Timing window
    let seconds_remaining = ROUND_SECONDS as f64 - (current_time - round_start as f64);
    if seconds_remaining > window_start || seconds_remaining < window_end {
        return None;
    }

    // Filter 4: Gap filter (adaptive)
    let btc_gap = btc_price - price_to_beat;
    let min_gap = price_to_beat * gap_threshold_pct;

    if btc_gap.abs() < min_gap {
        return None;
    }

    // Determine side based on gap direction
    let (winning_side, winning_ask, winning_id_up) = if btc_gap > 0.0 {
        ("UP", book.up_ask, true)
    } else {
        ("DOWN", book.dn_ask, false)
    };

    // Filter 5: Value filter (winning side must be cheap enough)
    if winning_ask >= max_price {
        return None;
    }

    // Apply fixed slippage (2 ticks), but cap at $0.60 max.
    // Computed before the depth filter so depth can be checked against
    // the *actual* fillable price range (cap may be tighter than +0.30).
    let winning_worst = (winning_ask + SLIPPAGE_BUFFER).min(0.60);

    // Filter 6: Depth filter — only count liquidity within the worst-price cap.
    let winning_depth = book.depth_up_to(winning_id_up, winning_worst);
    if winning_depth < shares {
        info!(
            "SKIP(depth): {} ask={:.4} worst={:.4} depth={:.2} < required={:.1} | gap={:+.0}",
            winning_side, winning_ask, winning_worst, winning_depth, shares, btc_gap,
        );
        return None;
    }

    Some(GapSignal {
        side: winning_side,
        winning_ask,
        winning_worst,
        is_up: winning_id_up,
        shares,
        btc_gap,
        seconds_remaining,
    })
}
