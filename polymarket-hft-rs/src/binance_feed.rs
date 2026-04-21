use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use tokio::net::TcpStream;
use tokio::sync::{watch, Mutex};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};

const BINANCE_WS_URL: &str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade";
const STALE_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_BACKOFF: u64 = 15;
const HISTORY_SIZE: usize = 600; // 10 minutes at ~1 update/sec

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// BTC price history entry: (timestamp, price)
pub type PriceHistory = Arc<Mutex<VecDeque<(f64, f64)>>>;

/// Current BTC price state - zero-lock reads via watch channel
#[derive(Debug, Clone, Copy)]
pub struct BtcPrice {
    pub price: f64,
    pub timestamp: f64,
}

impl Default for BtcPrice {
    fn default() -> Self {
        Self {
            price: 0.0,
            timestamp: 0.0,
        }
    }
}

/// Spawn Binance BTC/USDT aggTrade WebSocket feed task.
/// Returns (price receiver, price history).
/// The task runs indefinitely with automatic reconnect.
pub fn spawn_binance_feed() -> (watch::Receiver<BtcPrice>, PriceHistory) {
    let (tx, rx) = watch::channel(BtcPrice::default());
    let history = Arc::new(Mutex::new(VecDeque::with_capacity(HISTORY_SIZE)));
    let history_clone = Arc::clone(&history);

    tokio::spawn(async move {
        run_binance_feed(tx, history_clone).await;
    });

    (rx, history)
}

async fn run_binance_feed(tx: watch::Sender<BtcPrice>, history: PriceHistory) {
    let mut backoff = 1u64;

    loop {
        match connect_and_stream(&tx, &history).await {
            Ok(_) => {
                // Clean disconnect - reset backoff
                backoff = 1;
            }
            Err(e) => {
                warn!("Binance WS error: {e}. Reconnecting in {backoff}s...");
            }
        }

        tokio::time::sleep(Duration::from_secs(backoff)).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

async fn connect_and_stream(
    tx: &watch::Sender<BtcPrice>,
    history: &PriceHistory,
) -> anyhow::Result<()> {
    let (mut ws, _) = connect_async(BINANCE_WS_URL).await?;
    info!("Binance WS connected");

    let mut last_update = Instant::now();
    let mut buf = Vec::with_capacity(2048);

    loop {
        // Stale data detection
        if last_update.elapsed() > STALE_TIMEOUT {
            let current_price = tx.borrow().price;
            if current_price > 0.0 {
                warn!("Binance: No update for 15s, reconnecting");
                return Ok(());
            }
        }

        // Read next message with timeout
        let raw = match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => text.to_string(),
            Ok(Some(Ok(Message::Binary(data)))) => String::from_utf8_lossy(&data).into_owned(),
            Ok(Some(Ok(Message::Ping(_))))
            | Ok(Some(Ok(Message::Pong(_))))
            | Ok(Some(Ok(Message::Frame(_)))) => {
                continue;
            }
            Ok(Some(Ok(Message::Close(_)))) => {
                return Err(anyhow::anyhow!("WS closed by server"));
            }
            Ok(Some(Err(e))) => {
                return Err(anyhow::anyhow!("WS error: {e}"));
            }
            Ok(None) => {
                return Err(anyhow::anyhow!("WS stream ended"));
            }
            Err(_) => {
                // Timeout - loop again for stale check
                continue;
            }
        };

        // Parse aggTrade message
        buf.clear();
        buf.extend_from_slice(raw.as_bytes());

        let parsed: serde_json::Value = match simd_json::from_slice(&mut buf) {
            Ok(v) => v,
            Err(_) => continue,
        };

        if let Some(price_str) = parsed.get("p").and_then(|v| v.as_str()) {
            if let Ok(price) = price_str.parse::<f64>() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs_f64();

                // Update current price (zero-lock broadcast)
                let _ = tx.send(BtcPrice {
                    price,
                    timestamp: now,
                });

                // Append to history (cold path, lock is fine)
                let mut hist = history.lock().await;
                if hist.len() >= HISTORY_SIZE {
                    hist.pop_front();
                }
                hist.push_back((now, price));
                drop(hist);

                last_update = Instant::now();
            }
        }
    }
}
