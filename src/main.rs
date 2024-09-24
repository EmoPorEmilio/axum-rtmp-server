use axum::{routing::get, Router};
use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Build our application with a route
    let app = Router::new().route("/", get(|| async { "Hello, World!" }));

    // Run it
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    tracing::info!("listening on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
