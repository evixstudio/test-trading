use std::time::Duration;

use fred::clients::RedisClient;
use fred::interfaces::{ClientLike, EventInterface, PubsubInterface};
use fred::types::RedisConfig;
use tracing::{info, warn, error};

const REDIS_CHANNEL: &str = "live_arb:events";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt::init();

    let bot_token = std::env::var("TELEGRAM_BOT_TOKEN")
        .expect("TELEGRAM_BOT_TOKEN env var required");
    let chat_id = std::env::var("TELEGRAM_CHAT_ID")
        .expect("TELEGRAM_CHAT_ID env var required");
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".into());

    info!("Telegram relay starting...");

    let config = RedisConfig::from_url(&redis_url)?;
    let subscriber = RedisClient::new(config, None, None, None);
    subscriber.init().await?;

    subscriber.subscribe(REDIS_CHANNEL.to_string()).await?;
    info!("Subscribed to Redis channel '{REDIS_CHANNEL}'");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let mut message_rx = subscriber.message_rx();

    loop {
        match message_rx.recv().await {
            Ok(msg) => {
                let payload = msg.value.convert::<String>().unwrap_or_default();
                let data: serde_json::Value = match serde_json::from_str(&payload) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                let msg_type = data.get("type").and_then(|v| v.as_str()).unwrap_or("");
                let text = match msg_type {
                    "trade_attempt" => format_trade_attempt(&data),
                    "trade_result" => format_trade_result(&data),
                    "legging_risk" => format_legging_risk(&data),
                    "dump_failed" => format_dump_failed(&data),
                    _ => continue,
                };

                send_telegram(&http, &bot_token, &chat_id, &text).await;
            }
            Err(e) => {
                warn!("Redis recv error: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn send_telegram(http: &reqwest::Client, token: &str, chat_id: &str, text: &str) {
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    for attempt in 0..3 {
        match http
            .post(&url)
            .json(&serde_json::json!({
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "Markdown",
            }))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                info!("Telegram alert sent");
                return;
            }
            Ok(r) => warn!("Telegram send failed (attempt {attempt}): {}", r.status()),
            Err(e) => warn!("Telegram send error (attempt {attempt}): {e}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    error!("Telegram send failed after 3 attempts");
}

fn format_trade_attempt(data: &serde_json::Value) -> String {
    let slug = data.get("slug").and_then(|v| v.as_str()).unwrap_or("?");
    let sum = data.get("sum").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let up = data.get("up_ask").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let up_sz = data.get("up_ask_size").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let dn = data.get("down_ask").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let dn_sz = data.get("down_ask_size").and_then(|v| v.as_f64()).unwrap_or(0.0);
    let shares = data.get("shares").and_then(|v| v.as_f64()).unwrap_or(0.0);

    format!(
        "ARB SPOTTED & FIRED\nRound: {slug}\nTarget Sum: {sum:.4}\n\
         UP: {up:.3} ({up_sz:.1} avail)\nDN: {dn:.3} ({dn_sz:.1} avail)\n\
         Shares: {shares}"
    )
}

fn format_trade_result(data: &serde_json::Value) -> String {
    let slug = data.get("slug").and_then(|v| v.as_str()).unwrap_or("?");
    let success = data.get("success").and_then(|v| v.as_bool()).unwrap_or(false);
    let latency = data.get("latency_ms").and_then(|v| v.as_f64()).unwrap_or(0.0);

    if success {
        format!("ARBITRAGE LOCKED\nRound: {slug}\nBoth legs filled!\nLatency: {latency:.1}ms")
    } else {
        let up_ok = data.get("up_success").and_then(|v| v.as_bool()).unwrap_or(false);
        let dn_ok = data.get("dn_success").and_then(|v| v.as_bool()).unwrap_or(false);
        let up_err = data.get("up_error").and_then(|v| v.as_str()).unwrap_or("");
        let dn_err = data.get("dn_error").and_then(|v| v.as_str()).unwrap_or("");
        let up_status = if up_ok { "Filled".to_string() } else { format!("Failed ({up_err})") };
        let dn_status = if dn_ok { "Filled".to_string() } else { format!("Failed ({dn_err})") };
        format!(
            "ARBITRAGE FAILED\nRound: {slug}\nUP: {up_status}\nDN: {dn_status}\n\
             Latency: {latency:.1}ms"
        )
    }
}

fn format_legging_risk(data: &serde_json::Value) -> String {
    let action = data.get("action").and_then(|v| v.as_str()).unwrap_or("");
    format!("LEGGING RISK DUMP\nAction: {action}")
}

fn format_dump_failed(data: &serde_json::Value) -> String {
    let side = data.get("side").and_then(|v| v.as_str()).unwrap_or("?");
    let err = data.get("error").and_then(|v| v.as_str()).unwrap_or("?");
    format!("DUMP FAILED\nSide: {side}\nReason: {err}\nUNHEDGED EXPOSURE - close manually!")
}
