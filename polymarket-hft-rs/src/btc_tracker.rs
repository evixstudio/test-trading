use std::collections::VecDeque;

const MAX_BOUNDARY_DELTA: f64 = 5.0;

/// Lookup result for boundary price
#[derive(Debug, Clone)]
pub struct BoundaryLookup {
    pub price: f64,
    pub source: String,
}

impl BoundaryLookup {
    pub fn valid(&self) -> bool {
        self.price > 0.0
    }
}

/// Find BTC price closest to round boundary from history.
/// Matches Python _lookup_boundary_price() exactly.
#[inline]
pub fn lookup_boundary_price(
    history: &VecDeque<(f64, f64)>,
    boundary_ts: f64,
) -> BoundaryLookup {
    if history.is_empty() {
        return BoundaryLookup {
            price: 0.0,
            source: "no_history".into(),
        };
    }

    let mut best_price = 0.0;
    let mut best_delta = f64::INFINITY;

    for &(ts, price) in history {
        let delta = (ts - boundary_ts).abs();
        if delta < best_delta {
            best_delta = delta;
            best_price = price;
        }
    }

    if best_delta <= 3.0 {
        BoundaryLookup {
            price: best_price,
            source: format!("exact(Δ{:.1}s)", best_delta),
        }
    } else if best_delta <= MAX_BOUNDARY_DELTA {
        BoundaryLookup {
            price: best_price,
            source: format!("approx(Δ{:.1}s)", best_delta),
        }
    } else {
        BoundaryLookup {
            price: 0.0,
            source: format!("stale(Δ{:.0}s)", best_delta),
        }
    }
}
