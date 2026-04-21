use std::collections::HashMap;
use std::time::Instant;

/// Must match SLIPPAGE_BUFFER in gap_engine — depth beyond this range is unfillable.
const DEPTH_RANGE: f64 = 0.30;

/// Price stored as basis points (1 bps = 0.0001). Supports all Polymarket tick sizes.
type PriceBps = u32;

#[inline(always)]
fn price_to_bps(price: f64) -> PriceBps {
    (price * 10000.0).round() as u32
}

#[inline(always)]
fn bps_to_price(bps: PriceBps) -> f64 {
    bps as f64 / 10000.0
}

/// Full local orderbook for UP/DOWN tokens.
///
/// Maintains a HashMap of ask levels per side, updated incrementally by
/// `price_change` SELL-side deltas and reconciled by `book` full snapshots.
/// Cached `up_ask`/`up_ask_size`/`dn_ask`/`dn_ask_size` are recomputed after
/// every mutation so consumers (gap_engine, main) see the same public fields.
#[derive(Debug)]
pub struct ShadowBook {
    up_asks: HashMap<PriceBps, f64>,
    dn_asks: HashMap<PriceBps, f64>,

    pub up_ask: f64,
    pub up_ask_size: f64,
    pub dn_ask: f64,
    pub dn_ask_size: f64,
    pub last_book_event: Instant,
}

impl Default for ShadowBook {
    fn default() -> Self {
        Self {
            up_asks: HashMap::with_capacity(64),
            dn_asks: HashMap::with_capacity(64),
            up_ask: 0.0,
            up_ask_size: 0.0,
            dn_ask: 0.0,
            dn_ask_size: 0.0,
            last_book_event: Instant::now(),
        }
    }
}

impl ShadowBook {
    pub fn reset(&mut self) {
        self.up_asks.clear();
        self.dn_asks.clear();
        self.up_ask = 0.0;
        self.up_ask_size = 0.0;
        self.dn_ask = 0.0;
        self.dn_ask_size = 0.0;
        self.last_book_event = Instant::now();
    }

    /// Clear the ask book for one side (used after FAK rejection).
    pub fn invalidate_side(&mut self, is_up: bool) {
        if is_up {
            self.up_asks.clear();
            self.up_ask = 0.0;
            self.up_ask_size = 0.0;
        } else {
            self.dn_asks.clear();
            self.dn_ask = 0.0;
            self.dn_ask_size = 0.0;
        }
    }

    /// Process a `price_change` event.
    ///
    /// - SELL-side deltas update the local ask book for the matching token.
    /// - BUY-side events are ignored (no ask-side effect).
    /// - Empty/missing `best_ask` clears the ask book for that side.
    ///
    /// Returns `true` if the cached best-ask price or total depth changed.
    pub fn update_price_change(
        &mut self,
        asset_id: &str,
        up_id: &str,
        dn_id: &str,
        side: &str,
        price: Option<&str>,
        size: Option<&str>,
        best_ask: Option<&str>,
    ) -> bool {
        let is_up = asset_id == up_id;
        let is_dn = asset_id == dn_id;
        if !is_up && !is_dn {
            return false;
        }

        match best_ask {
            Some("") | None => {
                let already_empty = if is_up {
                    self.up_asks.is_empty() && self.up_ask == 0.0 && self.up_ask_size == 0.0
                } else {
                    self.dn_asks.is_empty() && self.dn_ask == 0.0 && self.dn_ask_size == 0.0
                };
                if already_empty {
                    return false;
                }
                if is_up { self.up_asks.clear(); } else { self.dn_asks.clear(); }
                return self.recompute(is_up);
            }
            _ => {}
        }

        if !side.eq_ignore_ascii_case("SELL") {
            return false;
        }

        if let (Some(p_str), Some(s_str)) = (price, size) {
            if let (Ok(p), Ok(s)) = (fast_parse_f64(p_str), fast_parse_f64(s_str)) {
                let asks = if is_up { &mut self.up_asks } else { &mut self.dn_asks };
                let key = price_to_bps(p);
                if s <= 0.0 {
                    asks.remove(&key);
                } else {
                    asks.insert(key, s);
                }
                return self.recompute(is_up);
            }
        }

        false
    }

    /// Process a full `book` snapshot (asks array). Always triggers re-evaluation.
    pub fn update_book_snapshot(
        &mut self,
        asset_id: &str,
        up_id: &str,
        dn_id: &str,
        asks: &[AskLevel],
    ) -> bool {
        let is_up = asset_id == up_id;
        let is_dn = asset_id == dn_id;
        if !is_up && !is_dn {
            return false;
        }

        let ask_book = if is_up { &mut self.up_asks } else { &mut self.dn_asks };
        ask_book.clear();
        for ask in asks {
            if ask.size > 0.0 {
                ask_book.insert(price_to_bps(ask.price), ask.size);
            }
        }

        self.last_book_event = Instant::now();
        self.recompute(is_up);
        true
    }

    /// Sum of ask sizes for levels with price ≤ `max_price` on one side.
    ///
    /// Used by gap_engine to check fillable liquidity against the *actual*
    /// worst-price cap (`winning_worst`), which may be tighter than the
    /// cached `DEPTH_RANGE` window when the 0.60 cap engages.
    #[inline]
    pub fn depth_up_to(&self, is_up: bool, max_price: f64) -> f64 {
        let asks = if is_up { &self.up_asks } else { &self.dn_asks };
        if asks.is_empty() {
            return 0.0;
        }
        let max_bps = price_to_bps(max_price);
        asks.iter()
            .filter(|&(&k, _)| k <= max_bps)
            .map(|(_, &v)| v)
            .sum()
    }

    /// Recompute cached best-ask and total-depth from the internal HashMap.
    /// Returns `true` if either value changed meaningfully.
    fn recompute(&mut self, is_up: bool) -> bool {
        let asks = if is_up { &self.up_asks } else { &self.dn_asks };

        let (new_ask, new_depth) = if asks.is_empty() {
            (0.0, 0.0)
        } else {
            let best_bps = *asks.keys().min().unwrap();
            let best_price = bps_to_price(best_bps);
            let max_bps = price_to_bps(best_price + DEPTH_RANGE);

            let depth: f64 = asks
                .iter()
                .filter(|&(&k, _)| k <= max_bps)
                .map(|(_, &v)| v)
                .sum();

            (best_price, depth)
        };

        let (old_ask, old_depth) = if is_up {
            (self.up_ask, self.up_ask_size)
        } else {
            (self.dn_ask, self.dn_ask_size)
        };

        if is_up {
            self.up_ask = new_ask;
            self.up_ask_size = new_depth;
        } else {
            self.dn_ask = new_ask;
            self.dn_ask_size = new_depth;
        }

        (new_ask - old_ask).abs() > 1e-10 || (new_depth - old_depth).abs() > 0.01
    }

}

#[derive(Debug, Clone, Copy)]
pub struct AskLevel {
    pub price: f64,
    pub size: f64,
}

#[inline(always)]
fn fast_parse_f64(s: &str) -> Result<f64, ()> {
    s.parse::<f64>().map_err(|_| ())
}
