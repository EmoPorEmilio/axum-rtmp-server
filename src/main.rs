use axum_rtmp_server::*;
use axum_rtmp_server::protocol::perform_server_handshake;
use axum::{routing::get, Router};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, watch};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let config = Arc::new(Config::load("config.toml")?);
    let metrics = Arc::new(MetricsCollector::new());

    tracing::info!("RTMP Multi-Destination Server starting...");
    tracing::info!("Allowed stream keys: {:?}", config.auth.allowed_stream_keys);

    let rtmp_addr: SocketAddr = config.rtmp.addr.parse()?;
    let rtmp_listener = TcpListener::bind(rtmp_addr).await?;
    tracing::info!("RTMP server listening on {}", rtmp_addr);

    let http_addr: SocketAddr = config.http.addr.parse()?;
    let http_listener = TcpListener::bind(http_addr).await?;
    tracing::info!("HTTP metrics server listening on {}", http_addr);

    // Create broadcast channel for media chunks
    let (media_tx, _media_rx) = broadcast::channel::<RtmpChunk>(1000);

    // Create shutdown signal channel
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let shutdown_flag = Arc::new(AtomicBool::new(false));

    // Track active connections
    let active_connections = Arc::new(AtomicUsize::new(0));
    let max_connections = config.server.max_connections;

    let app = Router::new()
        .route("/", get(|| async { "RTMP Multi-Destination Server" }))
        .route("/metrics", get({
            let metrics = metrics.clone();
            move || async move { metrics.export() }
        }));

    // Start HTTP server
    tokio::spawn(async move {
        if let Err(e) = axum::serve(http_listener, app).await {
            tracing::error!("HTTP server error: {}", e);
        }
    });

    // Setup signal handlers for graceful shutdown
    let shutdown_flag_clone = shutdown_flag.clone();
    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut sigterm = signal(SignalKind::terminate()).expect("Failed to setup SIGTERM handler");
            let mut sigint = signal(SignalKind::interrupt()).expect("Failed to setup SIGINT handler");
            tokio::select! {
                _ = sigterm.recv() => {
                    tracing::info!("Received SIGTERM, initiating graceful shutdown...");
                }
                _ = sigint.recv() => {
                    tracing::info!("Received SIGINT, initiating graceful shutdown...");
                }
            }
        }
        #[cfg(windows)]
        {
            tokio::signal::ctrl_c().await.expect("Failed to setup Ctrl+C handler");
            tracing::info!("Received Ctrl+C, initiating graceful shutdown...");
        }
        shutdown_flag_clone.store(true, Ordering::SeqCst);
        let _ = shutdown_tx_clone.send(true);
    });

    // Main accept loop
    loop {
        // Check for shutdown
        if shutdown_flag.load(Ordering::SeqCst) {
            tracing::info!("Shutdown initiated, stopping accept loop");
            break;
        }

        // Check connection limit
        let current = active_connections.load(Ordering::SeqCst);
        if current >= max_connections {
            tracing::warn!("Max connections reached ({}), waiting...", current);
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            continue;
        }

        // Use a timeout on accept to allow checking shutdown flag
        let accept_result = tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            rtmp_listener.accept()
        ).await;

        match accept_result {
            Ok(Ok((mut socket, addr))) => {
                tracing::info!("New connection from {}", addr);

                active_connections.fetch_add(1, Ordering::SeqCst);

                let config = config.clone();
                let metrics = metrics.clone();
                let media_tx = media_tx.clone();
                let active_connections = active_connections.clone();
                let shutdown_rx = shutdown_rx.clone();

                tokio::spawn(async move {
                    // Perform handshake
                    if let Err(e) = perform_server_handshake(&mut socket).await {
                        tracing::error!("Handshake error for {}: {}", addr, e);
                        active_connections.fetch_sub(1, Ordering::SeqCst);
                        return;
                    }

                    tracing::info!("Handshake successful for {}", addr);

                    // Create and run connection handler
                    let handler = ConnectionHandler::new(socket, config, metrics, media_tx);

                    if let Err(e) = handler.run().await {
                        match e {
                            RtmpError::ConnectionClosed => {
                                tracing::debug!("Connection closed: {}", addr);
                            }
                            RtmpError::Timeout(msg) => {
                                tracing::debug!("Connection timeout for {}: {}", addr, msg);
                            }
                            RtmpError::AuthFailed(msg) => {
                                tracing::warn!("Auth failed for {}: {}", addr, msg);
                            }
                            _ => {
                                tracing::error!("Connection error for {}: {}", addr, e);
                            }
                        }
                    }

                    active_connections.fetch_sub(1, Ordering::SeqCst);
                    tracing::debug!("Connection closed: {}", addr);

                    // Check if this was a shutdown-induced close
                    let _ = shutdown_rx;
                });
            }
            Ok(Err(e)) => {
                tracing::error!("Accept error: {}", e);
            }
            Err(_) => {
                // Timeout on accept, loop again to check shutdown
                continue;
            }
        }
    }

    // Graceful shutdown: wait for active connections to drain
    tracing::info!("Waiting for {} active connections to close...", active_connections.load(Ordering::SeqCst));

    let drain_timeout = tokio::time::Duration::from_secs(30);
    let drain_start = tokio::time::Instant::now();

    while active_connections.load(Ordering::SeqCst) > 0 {
        if drain_start.elapsed() > drain_timeout {
            tracing::warn!(
                "Drain timeout exceeded, {} connections still active",
                active_connections.load(Ordering::SeqCst)
            );
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    tracing::info!("Shutdown complete");
    Ok(())
}
