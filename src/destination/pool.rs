use crate::destination::{RtmpClient, RetryPolicy};
use crate::protocol::RtmpChunk;
use crate::error::RtmpError;
use crate::metrics::MetricsCollector;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct DestinationPool {
    pub clients: Vec<Arc<Mutex<RtmpClient>>>,
    pub retry_policy: RetryPolicy,
    metrics: Arc<MetricsCollector>,
}

impl DestinationPool {
    pub fn new(
        clients: Vec<Arc<Mutex<RtmpClient>>>,
        retry_policy: RetryPolicy,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            clients,
            retry_policy,
            metrics,
        }
    }

    pub async fn start_all(&self) -> Result<(), RtmpError> {
        for client in &self.clients {
            let mut c = client.lock().await;
            if !c.is_connected() {
                if let Err(e) = c.connect().await {
                    tracing::warn!("Failed to connect to {}: {}", c.name, e);
                    c.mark_failed(e.to_string());
                } else {
                    self.metrics.destinations_connected
                        .with_label_values(&[&c.name])
                        .set(1);
                }
            }
        }
        Ok(())
    }

    pub async fn replicate(&self, chunk: &RtmpChunk) {
        for client in &self.clients {
            let client = client.clone();
            let chunk = chunk.clone();

            tokio::spawn(async move {
                let mut c = client.lock().await;
                if let Err(e) = c.publish(&chunk.data, chunk.timestamp, chunk.message_type_id).await {
                    tracing::warn!("Failed to publish to {}: {}", c.name, e);
                    c.mark_failed(e.to_string());
                }
            });
        }
    }

    pub async fn retry_failed(&self) -> Result<(), RtmpError> {
        for client in &self.clients {
            let mut client = client.lock().await;

            if !client.state.is_failed() {
                continue;
            }

            if !self.retry_policy.should_retry(client.retry_count()) {
                client.mark_failed("Max retries exceeded".into());
                continue;
            }

            let delay = self.retry_policy.get_delay(client.retry_count());
            tokio::time::sleep(delay).await;

            client.increment_retry();
            if let Err(e) = client.connect().await {
                tracing::error!("Retry failed for {}: {}", client.name, e);
            } else {
                tracing::info!("Reconnected to destination: {}", client.name);
                self.metrics.destinations_connected
                    .with_label_values(&[&client.name])
                    .set(1);
            }
        }
        Ok(())
    }

    pub fn get_retry_policy(&self) -> &RetryPolicy {
        &self.retry_policy
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }

    pub fn send_to_all(&self, chunk: RtmpChunk) {
        for client in &self.clients {
            let client = client.clone();
            let chunk = chunk.clone();

            tokio::spawn(async move {
                let mut c = client.lock().await;
                if let Err(e) = c.publish(&chunk.data, chunk.timestamp, chunk.message_type_id).await {
                    tracing::warn!("Failed to publish to destination: {}", e);
                }
            });
        }
    }
}
