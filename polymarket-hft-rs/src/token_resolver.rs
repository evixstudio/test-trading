use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use polymarket_client_sdk::types::U256;
use tracing::warn;

use crate::config::{GAMMA_HOST, ROUND_SECONDS};
use crate::types::TokenPair;

/// Resolve UP/DOWN token IDs for the current 5-min round via Gamma API.
/// Direct HTTP GET -- no SDK overhead on this cold path.
pub async fn resolve_tokens(
    http: &reqwest::Client,
    timestamp: Option<f64>,
) -> anyhow::Result<TokenPair> {
    let now = timestamp.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64()
    });

    let ts = (now as u64 / ROUND_SECONDS) * ROUND_SECONDS;
    let slugs = [
        format!("btc-updown-5m-{ts}"),
        format!("btc-updown-5m-{}", ts.wrapping_sub(ROUND_SECONDS)),
    ];

    for slug in &slugs {
        let url = format!("{GAMMA_HOST}/events");
        let resp = match http
            .get(&url)
            .query(&[("slug", slug.as_str()), ("limit", "1")])
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                warn!("Gamma API returned {}", r.status());
                continue;
            }
            Err(e) => {
                warn!("Gamma API error: {e}");
                continue;
            }
        };

        let events: serde_json::Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                warn!("Gamma JSON parse error: {e}");
                continue;
            }
        };

        let events = match events.as_array() {
            Some(a) if !a.is_empty() => a,
            _ => continue,
        };

        let market = match events[0].get("markets").and_then(|m| m.as_array()).and_then(|a| a.first()) {
            Some(m) => m,
            None => continue,
        };

        // Parse clobTokenIds -- can be a JSON array or a JSON string containing an array
        let clob_raw = market.get("clobTokenIds");
        let mut token_ids: Vec<String> = match clob_raw {
            Some(serde_json::Value::Array(arr)) => {
                arr.iter().filter_map(|v| v.as_str().map(String::from)).collect()
            }
            Some(serde_json::Value::String(s)) => {
                match serde_json::from_str::<Vec<String>>(s) {
                    Ok(v) => v,
                    Err(_) => continue,
                }
            }
            _ => {
                // Fallback: parse from tokens array
                let mut up_id = String::new();
                let mut dn_id = String::new();
                if let Some(tokens) = market.get("tokens").and_then(|t| t.as_array()) {
                    for tk in tokens {
                        let outcome = tk.get("outcome").and_then(|o| o.as_str()).unwrap_or("").to_uppercase();
                        let tid = tk.get("token_id").and_then(|t| t.as_str()).unwrap_or("");
                        if outcome.contains("UP") || outcome.contains("YES") {
                            up_id = tid.to_string();
                        } else if outcome.contains("DOWN") || outcome.contains("NO") {
                            dn_id = tid.to_string();
                        }
                    }
                }
                if up_id.is_empty() || dn_id.is_empty() {
                    continue;
                }
                vec![up_id, dn_id]
            }
        };

        if token_ids.len() < 2 {
            continue;
        }

        if let Some(tokens_arr) = market.get("tokens").and_then(|t| t.as_array()) {
            for tk in tokens_arr {
                let outcome = tk.get("outcome").and_then(|o| o.as_str()).unwrap_or("").to_uppercase();
                let tid = tk.get("token_id").and_then(|t| t.as_str()).unwrap_or("");
                if (outcome.contains("UP") || outcome.contains("YES")) && tid == token_ids[1] {
                    warn!(
                        "Token ID ordering SWAPPED in clobTokenIds — correcting: UP={} DN={}",
                        &token_ids[1][..8.min(token_ids[1].len())],
                        &token_ids[0][..8.min(token_ids[0].len())],
                    );
                    token_ids.swap(0, 1);
                    break;
                }
            }
        }

        let up_str = &token_ids[0];
        let dn_str = &token_ids[1];

        let up_u256 = match U256::from_str(up_str) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let dn_u256 = match U256::from_str(dn_str) {
            Ok(v) => v,
            Err(_) => continue,
        };

        return Ok(TokenPair {
            slug: slug.clone(),
            up_id: up_u256,
            dn_id: dn_u256,
            up_id_str: up_str.clone(),
            dn_id_str: dn_str.clone(),
            boundary: ts,
        });
    }

    anyhow::bail!("Could not resolve tokens for any slug")
}

/// Check if we are near a round boundary (within `window` seconds).
pub fn near_boundary(now_secs: u64, window: u64) -> bool {
    let next_boundary = (now_secs / ROUND_SECONDS + 1) * ROUND_SECONDS;
    let current_boundary = (now_secs / ROUND_SECONDS) * ROUND_SECONDS;
    (next_boundary - now_secs) < window || (now_secs - current_boundary) < window
}
