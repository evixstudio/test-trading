use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::Message,
    MaybeTlsStream, WebSocketStream,
};
use tracing::{debug, info, warn};

use crate::config::WS_URL;
use crate::orderbook::{AskLevel, ShadowBook};

/// Outcome of a single `next_update()` call.
///
/// Distinguishes "a frame was consumed from the buffer" (BookUpdated / MessageNoOp)
/// from "no frame was available" (Timeout).  The drain loop after FAK rejection
/// relies on this to keep consuming buffered frames until the buffer is empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsOutcome {
    /// A WS frame was consumed AND the ShadowBook was mutated.
    BookUpdated,
    /// A WS frame was consumed but the ShadowBook was NOT mutated
    /// (BUY-side event, ping/pong, unknown event, parse error, etc.).
    MessageNoOp,
    /// No frame was available within the timeout window.
    Timeout,
}

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

const STALE_TIMEOUT: Duration = Duration::from_secs(15);

pub struct WsFeed {
    ws: Option<WsStream>,
    buf: Vec<u8>,
    last_data: Instant,
    /// Set to true after we send a liveness ping.  If the *next* stale
    /// check still fires (no Pong / no data arrived), the connection is
    /// genuinely dead and we bail.
    ping_pending: bool,
}

impl WsFeed {
    pub fn new() -> Self {
        Self {
            ws: None,
            buf: Vec::with_capacity(4096),
            last_data: Instant::now(),
            ping_pending: false,
        }
    }

    pub async fn connect(&mut self) -> anyhow::Result<()> {
        let (ws, _) = connect_async(WS_URL).await?;
        self.ws = Some(ws);
        self.last_data = Instant::now();
        self.ping_pending = false;
        info!("WS connected to {}", WS_URL);
        Ok(())
    }

    /// Initial subscribe -- first message after connect.
    /// Uses `{"type": "market", ...}` format per Polymarket WS API.
    pub async fn subscribe_initial(&mut self, token_ids: &[&str]) -> anyhow::Result<()> {
        let ws = self.ws.as_mut().ok_or_else(|| anyhow::anyhow!("not connected"))?;
        let msg = serde_json::json!({
            "type": "market",
            "assets_ids": token_ids,
        });
        ws.send(Message::Text(msg.to_string().into())).await?;
        self.last_data = Instant::now();
        self.ping_pending = false;
        debug!("WS initial subscribe to {} assets", token_ids.len());
        Ok(())
    }

    /// Mid-connection subscribe -- add new tokens on an existing connection.
    /// Uses `{"assets_ids": ..., "operation": "subscribe"}` format.
    pub async fn subscribe(&mut self, token_ids: &[&str]) -> anyhow::Result<()> {
        let ws = self.ws.as_mut().ok_or_else(|| anyhow::anyhow!("not connected"))?;
        let msg = serde_json::json!({
            "assets_ids": token_ids,
            "operation": "subscribe",
        });
        ws.send(Message::Text(msg.to_string().into())).await?;
        self.last_data = Instant::now();
        self.ping_pending = false;
        debug!("WS subscribe to {} assets", token_ids.len());
        Ok(())
    }

    pub async fn unsubscribe(&mut self, token_ids: &[&str]) -> anyhow::Result<()> {
        let ws = self.ws.as_mut().ok_or_else(|| anyhow::anyhow!("not connected"))?;
        let msg = serde_json::json!({
            "assets_ids": token_ids,
            "operation": "unsubscribe",
        });
        ws.send(Message::Text(msg.to_string().into())).await?;
        debug!("WS unsubscribed {} assets", token_ids.len());
        Ok(())
    }

    /// Read and process the next WS message, updating the shadow book.
    ///
    /// Returns:
    /// - `Ok(BookUpdated)` — a frame was consumed and the book was mutated.
    /// - `Ok(MessageNoOp)` — a frame was consumed but did not mutate the book.
    /// - `Ok(Timeout)` — no frame was available within the timeout window.
    /// - `Err` — WS disconnected or fatally broken.
    pub async fn next_update(
        &mut self,
        book: &mut ShadowBook,
        up_id: &str,
        dn_id: &str,
        timeout: Duration,
    ) -> anyhow::Result<WsOutcome> {
        let ws = self.ws.as_mut().ok_or_else(|| anyhow::anyhow!("not connected"))?;

        if self.last_data.elapsed() > STALE_TIMEOUT {
            if self.ping_pending {
                anyhow::bail!(
                    "WS dead (no response after ping, {}s)",
                    STALE_TIMEOUT.as_secs(),
                );
            }
            warn!("WS stale ({}s), sending liveness ping", STALE_TIMEOUT.as_secs());
            ws.send(Message::Ping(vec![].into())).await
                .map_err(|e| anyhow::anyhow!("WS ping send failed: {e}"))?;
            self.ping_pending = true;
            self.last_data = Instant::now();
            return Ok(WsOutcome::Timeout);
        }

        let raw = match tokio::time::timeout(timeout, ws.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                self.last_data = Instant::now();
                self.ping_pending = false;
                text.to_string()
            }
            Ok(Some(Ok(Message::Binary(data)))) => {
                self.last_data = Instant::now();
                self.ping_pending = false;
                String::from_utf8_lossy(&data).into_owned()
            }
            Ok(Some(Ok(Message::Ping(_)))) | Ok(Some(Ok(Message::Pong(_)))) => {
                self.last_data = Instant::now();
                self.ping_pending = false;
                return Ok(WsOutcome::MessageNoOp);
            }
            Ok(Some(Ok(Message::Close(_)))) => anyhow::bail!("WS closed by server"),
            Ok(Some(Err(e))) => anyhow::bail!("WS error: {e}"),
            Ok(None) => anyhow::bail!("WS stream ended"),
            Err(_) => return Ok(WsOutcome::Timeout),
            _ => return Ok(WsOutcome::Timeout),
        };

        self.buf.clear();
        self.buf.extend_from_slice(raw.as_bytes());

        let parsed: serde_json::Value = match simd_json::from_slice(&mut self.buf) {
            Ok(v) => v,
            Err(_) => return Ok(WsOutcome::MessageNoOp),
        };

        let mut updated = false;
        let items = if parsed.is_array() {
            parsed.as_array().unwrap().as_slice()
        } else {
            std::slice::from_ref(&parsed)
        };

        for item in items {
            let ev = item.get("event_type").and_then(|v| v.as_str()).unwrap_or("");

            match ev {
                "price_change" => {
                    if let Some(changes) = item.get("price_changes").and_then(|v| v.as_array()) {
                        for change in changes {
                            let aid = change.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
                            let best_ask = change.get("best_ask").and_then(|v| v.as_str());
                            let size_str = change.get("size").and_then(|v| v.as_str());
                            let side = change.get("side").and_then(|v| v.as_str()).unwrap_or("?");
                            let price_str = change.get("price").and_then(|v| v.as_str());

                            let token_label = if aid == up_id { "UP" } else if aid == dn_id { "DOWN" } else { "?" };
                            debug!(
                                "WS price_change: {} | side={} price={} size={} best_ask={} | book: up_ask={:.4} dn_ask={:.4}",
                                token_label, side, price_str.unwrap_or("?"),
                                size_str.unwrap_or("none"), best_ask.unwrap_or("none"),
                                book.up_ask, book.dn_ask,
                            );

                            if book.update_price_change(aid, up_id, dn_id, side, price_str, size_str, best_ask) {
                                updated = true;
                            }
                        }
                    }
                }
                "book" => {
                    let aid = item.get("asset_id").and_then(|v| v.as_str()).unwrap_or("");
                    let has_asks_key = item.get("asks").is_some();
                    if let Some(asks_arr) = item.get("asks").and_then(|v| v.as_array()) {
                        let asks: Vec<AskLevel> = asks_arr
                            .iter()
                            .filter_map(|a| {
                                let price = a.get("price")?.as_str()?.parse::<f64>().ok()?;
                                let size = a.get("size")?.as_str()?.parse::<f64>().ok()?;
                                Some(AskLevel { price, size })
                            })
                            .collect();

                        let token_label = if aid == up_id { "UP" } else if aid == dn_id { "DOWN" } else { "?" };
                        let total_depth: f64 = asks.iter().map(|a| a.size).sum();
                        let best = asks.iter().min_by(|a, b| a.price.partial_cmp(&b.price).unwrap_or(std::cmp::Ordering::Equal));
                        debug!(
                            "WS book: {} | levels={} total_depth={:.2} best=({:.4}, {:.2}) | before: up_ask={:.4}/{:.2} dn_ask={:.4}/{:.2}",
                            token_label, asks.len(), total_depth,
                            best.map_or(0.0, |a| a.price), best.map_or(0.0, |a| a.size),
                            book.up_ask, book.up_ask_size, book.dn_ask, book.dn_ask_size,
                        );

                        if book.update_book_snapshot(aid, up_id, dn_id, &asks) {
                            updated = true;
                        }
                    } else if !has_asks_key {
                        let token_label = if aid == up_id { "UP" } else if aid == dn_id { "DOWN" } else { "?" };
                        warn!(
                            "WS book: {} | asks key MISSING (stale data persists) | up_ask={:.4}/{:.2} dn_ask={:.4}/{:.2}",
                            token_label, book.up_ask, book.up_ask_size, book.dn_ask, book.dn_ask_size,
                        );
                    }
                }
                _ => {}
            }
        }

        if updated {
            Ok(WsOutcome::BookUpdated)
        } else {
            Ok(WsOutcome::MessageNoOp)
        }
    }

    pub async fn close(&mut self) {
        if let Some(mut ws) = self.ws.take() {
            let _ = ws.close(None).await;
        }
    }
}
