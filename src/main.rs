use axum_rtmp_server::*;
use axum::{routing::get, Router};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let config = Config::load("config.toml")?;
    let metrics = Arc::new(MetricsCollector::new());

    tracing::info!("RTMP Multi-Destination Server starting...");

    let rtmp_addr: SocketAddr = config.rtmp.addr.parse()?;
    let rtmp_listener = TcpListener::bind(rtmp_addr).await?;
    tracing::info!("RTMP server listening on {}", rtmp_addr);

    let http_addr: SocketAddr = config.http.addr.parse()?;
    let http_listener = TcpListener::bind(http_addr).await?;
    tracing::info!("HTTP metrics server listening on {}", http_addr);

    let app = Router::new()
        .route("/", get(|| async { "RTMP Multi-Destination Server" }))
        .route("/metrics", get({
            let metrics = metrics.clone();
            move || async move { metrics.export() }
        }));

    tokio::spawn(async move {
        if let Err(e) = axum::serve(http_listener, app).await {
            tracing::error!("HTTP server error: {}", e);
        }
    });

    loop {
        match rtmp_listener.accept().await {
            Ok((mut socket, addr)) => {
                tracing::info!("New connection from {}", addr);
                tokio::spawn(async move {
                    if let Err(e) = crate::protocol::perform_server_handshake(&mut socket).await {
                        tracing::error!("Handshake error for {}: {}", addr, e);
                    } else {
                        tracing::info!("Handshake successful for {}", addr);
                    }
                });
            }
            Err(e) => {
                tracing::error!("Accept error: {}", e);
            }
        }
    }
}
