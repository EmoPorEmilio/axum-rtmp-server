use axum::{routing::get, Router};
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

async fn handle_rtmp_connection(mut socket: tokio::net::TcpStream) {
    tracing::info!("New RTMP connection");

    let mut buffer = [0; 1024];
    loop {
        match socket.read(&mut buffer).await {
            Ok(0) => {
                tracing::info!("RTMP connection closed");
                break;
            }
            Ok(n) => {
                tracing::info!("Received {} bytes: {:?}", n, &buffer[..n]);
                // Here you would start implementing the RTMP handshake and chunk handling
            }
            Err(e) => {
                tracing::error!("Failed to read from socket: {}", e);
                break;
            }
        }
    }
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // RTMP listener
    let rtmp_addr = SocketAddr::from(([127, 0, 0, 1], 1935));
    let rtmp_listener = TcpListener::bind(rtmp_addr).await.unwrap();
    tracing::info!("RTMP listening on {}", rtmp_addr);

    // Spawn RTMP handler
    tokio::spawn(async move {
        loop {
            let (socket, _) = rtmp_listener.accept().await.unwrap();
            tokio::spawn(handle_rtmp_connection(socket));
        }
    });

    // HTTP server
    let app = Router::new().route("/", get(|| async { "RTMP Server is running!" }));

    let http_addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("HTTP listening on {}", http_addr);
    axum::Server::bind(&http_addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
