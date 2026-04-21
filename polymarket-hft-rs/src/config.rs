use clap::Parser;

pub const CLOB_HOST: &str = "https://clob.polymarket.com";
pub const GAMMA_HOST: &str = "https://gamma-api.polymarket.com";
pub const WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
pub const CHAIN_ID: u64 = 137; // Polygon mainnet
pub const ROUND_SECONDS: u64 = 300; // 5 minutes

pub const TICK_SIZE_DECIMALS: u32 = 2; // tick_size = 0.01
pub const LOT_SIZE_SCALE: u32 = 2;
pub const USDC_DECIMALS: u32 = 6;
pub const FEE_RATE_BPS: u64 = 1000;
pub const NEG_RISK: bool = false;
pub const MIN_ORDER_VALUE: f64 = 1.01; // Polymarket min is $1.00; use $1.01 for truncation safety

pub const REDIS_CHANNEL: &str = "gap_bot:events";
pub const HEARTBEAT_KEY: &str = "gap_bot:heartbeat";
pub const HEARTBEAT_TTL_SECS: i64 = 15;

#[derive(Parser, Debug)]
#[command(name = "polymarket-hft", about = "Ultra-low latency Polymarket gap directional bot")]
pub struct Cli {
    #[arg(long, help = "Enable LIVE real-money execution")]
    pub live: bool,

    #[arg(long, default_value = "3.0", help = "Shares per trade")]
    pub shares: f64,

    #[arg(long, default_value = "0.0004", help = "Gap threshold as fraction of PTB (0.0004 = 0.04%)")]
    pub gap_threshold_pct: f64,

    #[arg(long, default_value = "0.55", help = "Max entry price")]
    pub max_price: f64,

    #[arg(long, default_value = "180.0", help = "Window start (seconds remaining)")]
    pub window_start: f64,

    #[arg(long, default_value = "30.0", help = "Window end (seconds remaining)")]
    pub window_end: f64,

    #[arg(long, default_value = "5.0", help = "Minimum USDC reserve")]
    pub min_reserve: f64,

    #[arg(long, default_value = "3.0", help = "Daily loss limit (USD)")]
    pub daily_loss_limit: f64,

    #[arg(long, default_value = "localhost", help = "Redis host")]
    pub redis_host: String,

    #[arg(long, default_value = "6379", help = "Redis port")]
    pub redis_port: u16,
}

pub struct ApiConfig {
    pub private_key: String,
    pub proxy_address: String,
    pub signature_type: u8,
}

impl ApiConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let private_key = std::env::var("POLYMARKET_PRIVATE_KEY")
            .map_err(|_| anyhow::anyhow!("POLYMARKET_PRIVATE_KEY not set"))?;
        let proxy_address = std::env::var("POLYMARKET_PROXY_ADDRESS").unwrap_or_default();
        let signature_type: u8 = std::env::var("POLYMARKET_SIGNATURE_TYPE")
            .unwrap_or_else(|_| "0".into())
            .parse()
            .unwrap_or(0);

        Ok(Self { private_key, proxy_address, signature_type })
    }
}
