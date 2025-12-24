use crate::config::{Config, Secrets};
use crate::destination::{DestinationPool, RtmpClient};
use crate::metrics::MetricsCollector;
use std::sync::Arc;

#[derive(Clone)]
pub struct ConfigChange {
    pub destinations: Vec<DestinationConfig>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct DestinationConfig {
    pub name: String,
    pub enabled: bool,
    pub rtmp_url: String,
    pub stream_key: String,
}

pub struct ConfigWatcher {
    destinations: Vec<DestinationConfig>,
    pool: Arc<tokio::sync::Mutex<tokio::task::JoinHandle<()>>>,
}

impl ConfigWatcher {
    pub fn new() -> Self {
        let handle = tokio::task::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::MAX).await;
        });

        Self {
            destinations: Vec::new(),
            pool: Arc::new(tokio::sync::Mutex::new(handle)),
        }
    }

    pub async fn update_destinations(&mut self, config: &Config, secrets: &Secrets, pool: Arc<DestinationPool>, metrics: Arc<MetricsCollector>) {
        let mut new_destinations = Vec::new();

        if let Some(twitch) = &config.destinations.twitch {
            if twitch.enabled {
                if let Some(secret) = secrets.destinations.get("twitch") {
                    new_destinations.push(DestinationConfig {
                        name: "twitch".into(),
                        enabled: true,
                        rtmp_url: secret.rtmp_url.clone(),
                        stream_key: secret.stream_key.clone(),
                    });
                }
            }
        }

        if let Some(youtube) = &config.destinations.youtube {
            if youtube.enabled {
                if let Some(secret) = secrets.destinations.get("youtube") {
                    new_destinations.push(DestinationConfig {
                        name: "youtube".into(),
                        enabled: true,
                        rtmp_url: secret.rtmp_url.clone(),
                        stream_key: secret.stream_key.clone(),
                    });
                }
            }
        }

        if let Some(tiktok) = &config.destinations.tiktok {
            if tiktok.enabled {
                if let Some(secret) = secrets.destinations.get("tiktok") {
                    new_destinations.push(DestinationConfig {
                        name: "tiktok".into(),
                        enabled: true,
                        rtmp_url: secret.rtmp_url.clone(),
                        stream_key: secret.stream_key.clone(),
                    });
                }
            }
        }

        if self.destinations != new_destinations {
            tracing::info!("Destinations changed, updating pool");
            self.destinations = new_destinations.clone();

            let pool_arc = pool.clone();
            let metrics_arc = metrics.clone();
            let handle = tokio::spawn(async move {
                Self::update_pool(pool_arc, new_destinations, metrics_arc).await;
            });

            let mut pool_guard = self.pool.lock().await;
            *pool_guard = handle;
        }
    }

    async fn update_pool(pool: Arc<DestinationPool>, destinations: Vec<DestinationConfig>, metrics: Arc<MetricsCollector>) {
        let mut clients = Vec::new();

        for dest in &destinations {
            let client = Arc::new(tokio::sync::Mutex::new(RtmpClient::new(
                dest.name.clone(),
                dest.rtmp_url.clone(),
                dest.stream_key.clone(),
                metrics.clone(),
            )));
            clients.push(client);
        }

        let new_pool = DestinationPool::new(clients, pool.retry_policy.clone(), metrics.clone());
        new_pool.start_all().await.ok();
    }
}
