use std::time::{SystemTime, UNIX_EPOCH};

use fred::clients::RedisClient;
use fred::interfaces::{ClientLike, KeysInterface, PubsubInterface};
use fred::types::{Expiration, RedisConfig};

use crate::config::{HEARTBEAT_KEY, HEARTBEAT_TTL_SECS, REDIS_CHANNEL};

pub struct RedisBus {
    client: Option<RedisClient>,
}

impl RedisBus {
    pub fn new() -> Self {
        Self { client: None }
    }

    pub async fn connect(&mut self, host: &str, port: u16) -> anyhow::Result<()> {
        let config = RedisConfig::from_url(&format!("redis://{host}:{port}"))?;
        let client = RedisClient::new(config, None, None, None);
        client.init().await?;
        self.client = Some(client);
        Ok(())
    }

    /// Fire-and-forget publish to Redis Pub/Sub. Never blocks the hot path.
    pub fn publish_event(&self, event_type: &str, data: &serde_json::Value) {
        if let Some(client) = &self.client {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();

            let mut payload = data.clone();
            if let Some(obj) = payload.as_object_mut() {
                obj.insert("type".into(), serde_json::Value::String(event_type.into()));
                obj.insert("timestamp".into(), serde_json::json!(now));
            }

            let channel = REDIS_CHANNEL.to_string();
            let msg = serde_json::to_string(&payload).unwrap_or_default();
            let client = client.clone();

            tokio::spawn(async move {
                let _ = client.publish::<(), _, _>(channel, msg).await;
            });
        }
    }

    /// Write heartbeat key with TTL. Fire-and-forget.
    pub fn update_heartbeat(&self, balance: f64) {
        if let Some(client) = &self.client {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs_f64();

            let payload = serde_json::json!({ "timestamp": now, "balance": balance });
            let msg = serde_json::to_string(&payload).unwrap_or_default();
            let client = client.clone();

            tokio::spawn(async move {
                let _ = client
                    .set::<(), _, _>(
                        HEARTBEAT_KEY,
                        msg.as_str(),
                        Some(Expiration::EX(HEARTBEAT_TTL_SECS)),
                        None,
                        false,
                    )
                    .await;
            });
        }
    }

    pub async fn close(&mut self) {
        if let Some(client) = self.client.take() {
            let _ = client.quit().await;
        }
    }
}
