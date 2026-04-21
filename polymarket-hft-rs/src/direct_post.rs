use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE;
use hmac::{Hmac, Mac};
use polymarket_client_sdk::auth::{ApiKey, ExposeSecret};
use polymarket_client_sdk::clob::types::SignedOrder;
use polymarket_client_sdk::clob::types::response::PostOrderResponse;
use reqwest::Client;
use reqwest::header::{HeaderMap, HeaderValue};
use sha2::Sha256;

use crate::config::CLOB_HOST;
use crate::executor::AuthenticatedClient;

/// Optimized HTTP client for order posting that bypasses the SDK's
/// bare-bones reqwest::Client (which has zero performance tuning).
///
/// Key differences from the SDK's internal client:
///   - `.no_proxy()` — avoids OS proxy config lookup (5-100ms savings)
///   - `.tcp_nodelay(true)` — disables Nagle's algorithm
///   - `.http2_adaptive_window(true)` — better HTTP/2 flow control
///   - `.http2_initial_stream_window_size(512KB)` — larger initial window
///   - `.http2_keep_alive_interval(10s)` — sends PING frames every 10s
///   - `.http2_keep_alive_timeout(5s)` — wait 5s for PONG response
///   - `.http2_keep_alive_while_idle(true)` — PINGs even when no active requests
///   - `.pool_max_idle_per_host(10)` — keeps more warm connections
///   - `.pool_idle_timeout(90s)` — longer connection reuse
///
/// The L2 HMAC header generation is identical to the SDK's implementation
/// (verified against rs-clob-client/src/auth.rs lines 398-421).
pub struct FastOrderClient {
    client: Client,
    order_url: String,
    prewarm_url: String,
    pub owner: ApiKey,
    address: String,
    api_key: String,
    secret: String,
    passphrase: String,
}

impl FastOrderClient {
    /// Build from an already-authenticated SDK client.
    /// Extracts credentials for direct HMAC header generation.
    pub fn from_authenticated(auth_client: &AuthenticatedClient) -> anyhow::Result<Self> {
        let creds = auth_client.credentials();
        let address = format!("{:#x}", auth_client.address());
        let api_key = creds.key().to_string();
        let secret = creds.secret().expose_secret().to_string();
        let passphrase = creds.passphrase().expose_secret().to_string();

        let mut default_headers = HeaderMap::new();
        default_headers.insert("User-Agent", HeaderValue::from_static("rs_clob_client"));
        default_headers.insert("Accept", HeaderValue::from_static("*/*"));
        default_headers.insert("Content-Type", HeaderValue::from_static("application/json"));

        let client = Client::builder()
            .default_headers(default_headers)
            .no_proxy()
            .pool_max_idle_per_host(10)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .http2_adaptive_window(true)
            .http2_initial_stream_window_size(512 * 1024)
            .http2_keep_alive_interval(Duration::from_secs(10))
            .http2_keep_alive_timeout(Duration::from_secs(5))
            .http2_keep_alive_while_idle(true)
            .connect_timeout(Duration::from_millis(5000))
            .timeout(Duration::from_millis(10000))
            .build()?;

        Ok(Self {
            client,
            order_url: format!("{CLOB_HOST}/order"),
            prewarm_url: CLOB_HOST.to_string(),
            owner: creds.key(),
            address,
            api_key,
            secret,
            passphrase,
        })
    }

    /// POST a signed order directly to the CLOB API with optimized HTTP/2.
    ///
    /// HMAC L2 auth algorithm (matches SDK's auth.rs):
    ///   message  = "{timestamp}{METHOD}{path}{body}"
    ///   key      = base64_url_decode(api_secret)
    ///   sig      = base64_url_encode(HMAC-SHA256(key, message))
    pub async fn post_order(
        &self,
        signed: &SignedOrder,
    ) -> Result<PostOrderResponse, anyhow::Error> {
        let t0 = std::time::Instant::now();

        // Serialize using the SDK's custom Serialize impl on SignedOrder.
        // This produces the exact JSON body the CLOB expects (with signature
        // folded into the order object, salt serialized correctly, etc.)
        let body = serde_json::to_string(signed)?;
        let t1 = std::time::Instant::now();

        let timestamp = chrono::Utc::now().timestamp();

        // HMAC message = "{timestamp}{method}{path}{body}"
        // Verified against: rs-clob-client/src/auth.rs:399-404
        let message = format!("{timestamp}POST/order{body}");

        let decoded_secret = URL_SAFE
            .decode(&self.secret)
            .map_err(|e| anyhow::anyhow!("base64 decode secret: {e}"))?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&decoded_secret)
            .map_err(|e| anyhow::anyhow!("HMAC key: {e}"))?;
        mac.update(message.as_bytes());
        let signature = URL_SAFE.encode(mac.finalize().into_bytes());
        let t2 = std::time::Instant::now();

        // Log auth headers for debugging (comment out in production)
        tracing::debug!(
            "POST /order auth headers:\n  POLY_ADDRESS: {}\n  POLY_API_KEY: {}\n  POLY_TIMESTAMP: {}\n  POLY_SIGNATURE: {}\n  Body length: {} bytes",
            &self.address,
            &self.api_key,
            timestamp,
            &signature,
            body.len()
        );

        let response = self
            .client
            .post(&self.order_url)
            .header("POLY_ADDRESS", &self.address)
            .header("POLY_API_KEY", &self.api_key)
            .header("POLY_PASSPHRASE", &self.passphrase)
            .header("POLY_SIGNATURE", &signature)
            .header("POLY_TIMESTAMP", timestamp.to_string())
            .body(body)
            .send()
            .await?;
        let t3 = std::time::Instant::now();

        let status = response.status();
        if !status.is_success() {
            let text = response.text().await.unwrap_or_default();
            let t4 = std::time::Instant::now();

            tracing::warn!(
                "POST /order FAILED | Timing: serialize={:.2}ms hmac={:.2}ms send={:.2}ms parse={:.2}ms total={:.2}ms | Status: {}",
                (t1-t0).as_secs_f64() * 1000.0,
                (t2-t1).as_secs_f64() * 1000.0,
                (t3-t2).as_secs_f64() * 1000.0,
                (t4-t3).as_secs_f64() * 1000.0,
                (t4-t0).as_secs_f64() * 1000.0,
                status
            );

            return Err(anyhow::anyhow!(
                "Status: error({status}) making POST call to /order with {text}"
            ));
        }

        let parsed: PostOrderResponse = response.json().await?;
        let t4 = std::time::Instant::now();

        tracing::info!(
            "POST /order SUCCESS | Timing: serialize={:.2}ms hmac={:.2}ms send={:.2}ms parse={:.2}ms total={:.2}ms",
            (t1-t0).as_secs_f64() * 1000.0,
            (t2-t1).as_secs_f64() * 1000.0,
            (t3-t2).as_secs_f64() * 1000.0,
            (t4-t3).as_secs_f64() * 1000.0,
            (t4-t0).as_secs_f64() * 1000.0
        );

        Ok(parsed)
    }

    /// Keep the optimized HTTP/2 connection warm.
    /// Call every 15-30s to avoid TLS/TCP handshake on the hot path.
    pub async fn prewarm(&self) -> anyhow::Result<Duration> {
        let t0 = std::time::Instant::now();
        let resp = self.client.get(&self.prewarm_url).send().await?;
        let _ = resp.bytes().await;
        Ok(t0.elapsed())
    }
}
