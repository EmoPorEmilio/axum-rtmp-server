use crate::config::Config;
use crate::destination::DestinationPool;
use crate::error::RtmpError;
use crate::metrics::MetricsCollector;
use crate::protocol::RtmpChunk;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tracing;

pub struct RtmpServer {
    listener: TcpListener,
    config: Arc<Config>,
    replicator: Arc<Replicator>,
    metrics: Arc<MetricsCollector>,
}

pub struct Replicator {
    destinations: Arc<DestinationPool>,
    tx: broadcast::Sender<RtmpChunk>,
}

impl Replicator {
    pub fn new(destinations: Arc<DestinationPool>) -> Self {
        let (tx, _) = broadcast::channel(1000);
        Self { destinations, tx }
    }

    pub fn publish(&self, chunk: RtmpChunk) {
        let _ = self.tx.send(chunk);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RtmpChunk> {
        self.tx.subscribe()
    }
}

pub struct ConnectionHandler {
    socket: TcpStream,
    // Connection handler would go here
    // For now, this is a placeholder that references the old RtmpConnection logic
}

impl RtmpServer {
    pub fn new(
        listener: TcpListener,
        config: Arc<Config>,
        replicator: Arc<Replicator>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            listener,
            config,
            replicator,
            metrics,
        }
    }

    pub async fn run(&self) -> Result<(), RtmpError> {
        loop {
            match self.listener.accept().await {
                Ok((socket, addr)) => {
                    tracing::info!("New connection from {}", addr);
                    let config = self.config.clone();
                    let replicator = self.replicator.clone();
                    let metrics = self.metrics.clone();

                    tokio::spawn(async move {
                        // TODO: Create and run ConnectionHandler with the actual RTMP logic
                        // For now, this is a placeholder
                    });
                }
                Err(e) => {
                    tracing::error!("Accept error: {}", e);
                }
            }
        }
    }
}
