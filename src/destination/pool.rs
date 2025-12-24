use crate::destination::{RtmpClient, RetryPolicy};
use crate::protocol::RtmpChunk;
use crate::error::RtmpError;
use crate::metrics::MetricsCollector;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{broadcast, Mutex};

pub struct DestinationPool {
    pub clients: Vec<Arc<Mutex<RtmpClient>>>,
    pub retry_policy: RetryPolicy,
    metrics: Arc<MetricsCollector>,
    /// Counter for dropped chunks due to backpressure
    dropped_chunks: AtomicU64,
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
            dropped_chunks: AtomicU64::new(0),
        }
    }

    /// Get the number of dropped chunks
    pub fn dropped_chunks(&self) -> u64 {
        self.dropped_chunks.load(Ordering::Relaxed)
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

    /// Retry failed connections. This method does not block while holding locks.
    pub async fn retry_failed(&self) -> Result<(), RtmpError> {
        // First pass: collect clients that need retry
        let mut clients_to_retry = Vec::new();

        for client in &self.clients {
            let c = client.lock().await;
            if c.state.is_failed() && self.retry_policy.should_retry(c.retry_count()) {
                let delay = self.retry_policy.get_delay(c.retry_count());
                let name = c.name.clone();
                clients_to_retry.push((client.clone(), delay, name));
            } else if c.state.is_failed() && !self.retry_policy.should_retry(c.retry_count()) {
                // Mark as max retries exceeded
                drop(c);
                let mut c = client.lock().await;
                c.state = crate::destination::ClientState::MaxRetriesExceeded;
            }
        }

        // Second pass: retry with proper delays (without holding locks)
        for (client, delay, name) in clients_to_retry {
            tracing::debug!("Waiting {:?} before retrying {}", delay, name);
            tokio::time::sleep(delay).await;

            let mut c = client.lock().await;
            c.increment_retry();

            if let Err(e) = c.connect().await {
                tracing::error!("Retry failed for {}: {}", name, e);
                c.mark_failed(e.to_string());
            } else {
                tracing::info!("Reconnected to destination: {}", name);
                self.metrics.destinations_connected
                    .with_label_values(&[&name])
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

    /// Start a background task that receives chunks from a broadcast channel
    /// and replicates to all destinations. Handles RecvError::Lagged gracefully.
    pub fn start_replication_task(
        self: Arc<Self>,
        mut rx: broadcast::Receiver<RtmpChunk>,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = rx.recv() => {
                        match result {
                            Ok(chunk) => {
                                self.replicate(&chunk).await;
                            }
                            Err(broadcast::error::RecvError::Lagged(n)) => {
                                // We fell behind - log and continue
                                self.dropped_chunks.fetch_add(n, Ordering::Relaxed);
                                tracing::warn!(
                                    "Replication lagged, dropped {} chunks (total: {})",
                                    n,
                                    self.dropped_chunks.load(Ordering::Relaxed)
                                );
                            }
                            Err(broadcast::error::RecvError::Closed) => {
                                tracing::info!("Broadcast channel closed, stopping replication");
                                break;
                            }
                        }
                    }
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            tracing::info!("Shutdown signal received, stopping replication");
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Check if the pool is empty
    pub fn is_empty(&self) -> bool {
        self.clients.is_empty()
    }
}
