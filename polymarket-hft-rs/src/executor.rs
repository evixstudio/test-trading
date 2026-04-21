use std::borrow::Cow;
use std::str::FromStr;
use std::sync::LazyLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use alloy::signers::SignerSync as _;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol_types::{Eip712Domain, SolStruct as _};
use polymarket_client_sdk::auth::Signer as _;
use polymarket_client_sdk::clob::types::{
    Order, OrderType, Side, SignedOrder, SignatureType,
};
use polymarket_client_sdk::clob::{Client, Config};
use polymarket_client_sdk::types::{Address, U256};
use polymarket_client_sdk::POLYGON;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use tracing::info;

use crate::config::{
    ApiConfig, CHAIN_ID, CLOB_HOST, FEE_RATE_BPS, LOT_SIZE_SCALE, TICK_SIZE_DECIMALS,
    USDC_DECIMALS,
};
use crate::direct_post::FastOrderClient;
use crate::types::OrderResult;

/// CTF Exchange on Polygon mainnet (neg_risk=false).
/// Source: polymarket-client-sdk CONFIG[137].exchange
static EXCHANGE_ADDR: LazyLock<Address> = LazyLock::new(|| {
    Address::from_str("0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E").unwrap()
});

/// Per-execution latency breakdown (milliseconds). Easy to remove later.
#[derive(Debug, Clone, Copy)]
pub struct ExecLatency {
    pub build_ms: f64,
    pub sign_ms: f64,
    pub post_ms: f64,
    pub total_ms: f64,
}

/// Build an Order struct directly -- zero API calls, hardcoded tick/fee.
/// Mirrors py-clob-client OrderBuilder.create_order() with TICK_SIZE=0.01, fee_rate_bps=1000.
fn build_limit_order(
    maker: Address,
    signer_addr: Address,
    token_id: U256,
    size: f64,
    price: f64,
    side: Side,
    sig_type: SignatureType,
) -> Order {
    // `from_f64` (not `from_f64_retain`) rounds to the shortest round-tripping
    // representation, so `0.6_f64` becomes Decimal("0.6") instead of the raw
    // binary expansion `0.5999999999999999778…`. Without this, a subsequent
    // `trunc_with_scale(2)` would floor 0.60 → 0.59 (and 0.30 → 0.29 etc.),
    // silently shipping orders 1 tick below the intended limit price.
    let dec_size = Decimal::from_f64(size)
        .expect("valid f64")
        .trunc_with_scale(LOT_SIZE_SCALE);
    let dec_price = Decimal::from_f64(price)
        .expect("valid f64")
        .trunc_with_scale(TICK_SIZE_DECIMALS);

    let combined_scale = TICK_SIZE_DECIMALS + LOT_SIZE_SCALE;

    let (taker_amount, maker_amount) = match side {
        Side::Buy => {
            let taker = dec_size;
            let maker = (dec_size * dec_price).trunc_with_scale(combined_scale);
            (taker, maker)
        }
        Side::Sell => {
            let taker = (dec_size * dec_price).trunc_with_scale(combined_scale);
            let maker = dec_size;
            (taker, maker)
        }
        _ => unreachable!(),
    };

    let taker_fixed = to_fixed_u128(taker_amount);
    let maker_fixed = to_fixed_u128(maker_amount);
    let salt = generate_salt();

    let mut order = Order::default();
    order.salt = U256::from(salt);
    order.maker = maker;
    order.signer = signer_addr;
    order.taker = Address::ZERO;
    order.tokenId = token_id;
    order.makerAmount = U256::from(maker_fixed);
    order.takerAmount = U256::from(taker_fixed);
    order.expiration = U256::ZERO;
    order.nonce = U256::ZERO;
    order.feeRateBps = U256::from(FEE_RATE_BPS);
    order.side = side as u8;
    order.signatureType = sig_type as u8;
    order
}

/// Convert a Decimal to USDC atomic units (6 decimals).
#[inline]
fn to_fixed_u128(d: Decimal) -> u128 {
    d.normalize()
        .trunc_with_scale(USDC_DECIMALS)
        .mantissa()
        .to_u128()
        .expect("positive decimal fits u128")
}

/// Generate a salt masked to fit IEEE 754 integer range (backend parses as JSON number).
fn generate_salt() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards");
    let seconds = now.as_secs_f64();
    let r: f64 = rand::random();
    let raw = (seconds * r).round() as u64;
    raw & ((1u64 << 53) - 1)
}

/// Sign an Order locally using the hardcoded EIP-712 domain for
/// Polygon mainnet, neg_risk=false, BTC 5-min markets.
/// Pure local cryptography -- zero HTTP calls.
fn sign_order_local(
    signer: &PrivateKeySigner,
    order: Order,
    order_type: OrderType,
    owner: polymarket_client_sdk::auth::ApiKey,
) -> anyhow::Result<SignedOrder> {
    let domain = Eip712Domain {
        name: Some(Cow::Borrowed("Polymarket CTF Exchange")),
        version: Some(Cow::Borrowed("1")),
        chain_id: Some(U256::from(CHAIN_ID)),
        verifying_contract: Some(*EXCHANGE_ADDR),
        ..Default::default()
    };

    let hash = order.eip712_signing_hash(&domain);
    let signature = signer
        .sign_hash_sync(&hash)
        .map_err(|e| anyhow::anyhow!("local sign failed: {e}"))?;

    Ok(SignedOrder::builder()
        .order(order)
        .signature(signature)
        .order_type(order_type)
        .owner(owner)
        .build())
}

pub struct Executor {
    pub signer: PrivateKeySigner,
    pub signer_address: Address,
    pub maker_address: Address,
    pub sig_type: SignatureType,
    pub dry_run: bool,
}

impl Executor {
    pub fn new(api_config: &ApiConfig, dry_run: bool) -> anyhow::Result<Self> {
        let signer = PrivateKeySigner::from_str(&api_config.private_key)?
            .with_chain_id(Some(POLYGON));
        let signer_address = signer.address();

        let maker_address = if api_config.proxy_address.is_empty() {
            signer_address
        } else {
            Address::from_str(&api_config.proxy_address)?
        };

        let sig_type = match api_config.signature_type {
            0 => SignatureType::Eoa,
            1 => SignatureType::Proxy,
            2 => SignatureType::GnosisSafe,
            _ => SignatureType::Eoa,
        };

        Ok(Self { signer, signer_address, maker_address, sig_type, dry_run })
    }

    /// Authenticate with the CLOB API and return the typed client.
    /// use_server_time(false) avoids an extra HTTP round-trip on every request.
    pub async fn authenticate(&self) -> anyhow::Result<AuthenticatedClient> {
        let config = Config::builder().use_server_time(false).build();
        let base = Client::new(CLOB_HOST, config)?;

        let authenticated = match self.sig_type {
            SignatureType::Proxy => {
                base.authentication_builder(&self.signer)
                    .funder(self.maker_address)
                    .signature_type(SignatureType::Proxy)
                    .authenticate()
                    .await?
            }
            SignatureType::GnosisSafe => {
                base.authentication_builder(&self.signer)
                    .funder(self.maker_address)
                    .signature_type(SignatureType::GnosisSafe)
                    .authenticate()
                    .await?
            }
            _ => {
                base.authentication_builder(&self.signer)
                    .authenticate()
                    .await?
            }
        };

        let _status = authenticated.ok().await?;

        info!("Executor initialized in LIVE mode, maker={}", self.maker_address);
        Ok(authenticated)
    }

    /// Single-leg market buy with FAK (Fill-And-Kill).
    /// Uses FastOrderClient for optimized direct POST (bypasses SDK's slow reqwest).
    pub async fn market_buy_fak(
        &self,
        fast_client: &FastOrderClient,
        token_id: U256,
        worst_price: f64,
        shares: f64,
    ) -> (OrderResult, ExecLatency) {
        let zero_latency = ExecLatency {
            build_ms: 0.0,
            sign_ms: 0.0,
            post_ms: 0.0,
            total_ms: 0.0,
        };

        if self.dry_run {
            return (
                OrderResult {
                    success: true,
                    order_id: "dry_buy".into(),
                    error: String::new(),
                    raw_response: String::new(),
                    filled_price: worst_price,
                    filled_shares: shares,
                },
                zero_latency,
            );
        }

        let t0 = Instant::now();
        let owner = fast_client.owner;

        let order = build_limit_order(
            self.maker_address,
            self.signer_address,
            token_id,
            shares,
            worst_price,
            Side::Buy,
            self.sig_type,
        );

        let t1 = Instant::now();
        let build_ms = (t1 - t0).as_secs_f64() * 1000.0;

        let signed = match sign_order_local(&self.signer, order, OrderType::FAK, owner) {
            Ok(s) => s,
            Err(e) => {
                return (OrderResult::failed(format!("sign: {e}")), zero_latency);
            }
        };

        let t2 = Instant::now();
        let sign_ms = (t2 - t1).as_secs_f64() * 1000.0;

        let result = fast_client.post_order(&signed).await;

        let t3 = Instant::now();
        let post_ms = (t3 - t2).as_secs_f64() * 1000.0;
        let total_ms = (t3 - t0).as_secs_f64() * 1000.0;

        let latency = ExecLatency {
            build_ms,
            sign_ms,
            post_ms,
            total_ms,
        };

        (parse_order_response(result), latency)
    }

    /// Aggressive dump for legging risk. FAK sell at deeply discounted price.
    pub async fn market_sell(
        &self,
        fast_client: &FastOrderClient,
        token_id: U256,
        shares: f64,
    ) -> OrderResult {
        if self.dry_run {
            return OrderResult {
                success: true,
                order_id: "dry_sell".into(),
                error: String::new(),
                raw_response: String::new(),
                filled_price: 0.0,
                filled_shares: shares,
            };
        }

        let dump_price = f64::max(0.01, 1.02 / shares);
        let dump_price = (dump_price * 100.0).ceil() / 100.0;

        let order = build_limit_order(
            self.maker_address,
            self.signer_address,
            token_id,
            shares,
            dump_price,
            Side::Sell,
            self.sig_type,
        );

        let owner = fast_client.owner;
        let signed = match sign_order_local(&self.signer, order, OrderType::FAK, owner) {
            Ok(s) => s,
            Err(e) => return OrderResult::failed(format!("sign sell: {e}")),
        };

        parse_order_response(fast_client.post_order(&signed).await)
    }
}

fn parse_post_response(
    r: &polymarket_client_sdk::clob::types::response::PostOrderResponse,
) -> OrderResult {
    let has_error = r.error_msg.as_ref().is_some_and(|m| !m.is_empty());
    let success = r.success && !has_error;

    // Extract filled price and shares from making/taking amounts
    // For BUY orders: taker_amount = shares, maker_amount = cost
    // filled_price = maker_amount / taker_amount
    let mut filled_price = 0.0;
    let mut filled_shares = 0.0;

    if success {
        // Convert Decimal to f64
        if let Some(making_f) = r.making_amount.to_f64() {
            if let Some(taking_f) = r.taking_amount.to_f64() {
                // For FAK BUY: taker=shares, maker=USDC cost
                // SDK returns amounts already in human-readable units (NOT atomic units)
                filled_shares = taking_f;
                let cost = making_f;
                if filled_shares > 0.0 {
                    filled_price = cost / filled_shares;
                }
            }
        }
    }

    OrderResult {
        success,
        order_id: r.order_id.clone(),
        error: if success {
            String::new()
        } else {
            r.error_msg.clone().unwrap_or_default()
        },
        raw_response: format!("{r:?}"),
        filled_price,
        filled_shares,
    }
}

fn parse_order_response<E: std::fmt::Display>(
    res: Result<polymarket_client_sdk::clob::types::response::PostOrderResponse, E>,
) -> OrderResult {
    match res {
        Ok(ref r) => parse_post_response(r),
        Err(e) => OrderResult::failed(format!("{e}")),
    }
}

/// Type alias for the authenticated CLOB client.
/// The SDK uses a type-state machine: Client<Authenticated<Normal>> after auth.
/// We define this alias so the rest of the codebase doesn't need to know the generics.
pub type AuthenticatedClient = Client<
    polymarket_client_sdk::auth::state::Authenticated<
        polymarket_client_sdk::auth::Normal,
    >,
>;
