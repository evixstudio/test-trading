use std::fmt;

use polymarket_client_sdk::types::U256;

#[derive(Debug, Clone)]
pub struct TokenPair {
    pub slug: String,
    pub up_id: U256,
    pub dn_id: U256,
    pub up_id_str: String,
    pub dn_id_str: String,
    pub boundary: u64,
}

/// Gap-based directional signal (single-leg trade)
#[derive(Debug, Clone, Copy)]
pub struct GapSignal {
    pub side: &'static str,  // "UP" or "DOWN"
    pub winning_ask: f64,
    pub winning_worst: f64,
    pub is_up: bool,          // true if UP, false if DOWN
    pub shares: f64,
    pub btc_gap: f64,
    pub seconds_remaining: f64,
}

/// Open position tracking for P&L calculation at round boundary
#[derive(Debug, Clone)]
pub struct OpenPosition {
    pub side: String,          // "UP" or "DOWN"
    pub ordered_shares: f64,   // Shares ordered (for fee calc)
    pub cost: f64,             // Total cost paid
    pub ptb: f64,              // Price-to-beat at entry
    pub timestamp: f64,        // Entry timestamp
}

/// Full P&L breakdown returned by `calculate_pnl`.
/// Published to Redis so the Telegram dispatcher can display every field.
#[derive(Debug, Clone)]
pub struct PnlResult {
    pub won: bool,
    pub pnl: f64,
    pub fill_price: f64,
    pub fee_shares: f64,
    pub effective_shares: f64,
    pub payout: f64,
}

impl OpenPosition {
    /// Calculate actual P&L with Polymarket fee model.
    /// Fee = ordered * 0.072 * (1 - fill_price)
    /// Effective shares = ordered - fee_shares
    /// Win: $1.00 per effective share | Loss: shares worthless
    pub fn calculate_pnl(&self, final_btc: f64) -> PnlResult {
        let actual_outcome = if final_btc > self.ptb { "UP" } else { "DOWN" };
        let won = self.side == actual_outcome;

        let fill_price = if self.ordered_shares > 0.0 {
            self.cost / self.ordered_shares
        } else {
            0.0
        };
        let fee_shares = self.ordered_shares * 0.072 * (1.0 - fill_price);
        let effective_shares = self.ordered_shares - fee_shares;

        let payout = if won { 1.0 * effective_shares } else { 0.0 };
        let pnl = if won {
            payout - self.cost
        } else {
            -self.cost
        };

        PnlResult { won, pnl, fill_price, fee_shares, effective_shares, payout }
    }
}

#[derive(Debug, Clone)]
pub struct OrderResult {
    pub success: bool,
    pub order_id: String,
    pub error: String,
    pub raw_response: String,
    pub filled_price: f64,
    pub filled_shares: f64,
}

impl OrderResult {
    pub fn failed(error: impl Into<String>) -> Self {
        Self {
            success: false,
            order_id: String::new(),
            error: error.into(),
            raw_response: String::new(),
            filled_price: 0.0,
            filled_shares: 0.0,
        }
    }
}

impl fmt::Display for OrderResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.success {
            write!(f, "OK({})", self.order_id)
        } else {
            write!(f, "FAIL({})", self.error)
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TradeEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub timestamp: f64,
    #[serde(flatten)]
    pub data: serde_json::Value,
}
