use axum::{
    extract::State,
    routing::get,
    Router,
};
use crate::metrics::MetricsCollector;

pub async fn metrics_handler(State(metrics): State<MetricsCollector>) -> String {
    metrics.export()
}

pub fn create_metrics_server_router(metrics: std::sync::Arc<MetricsCollector>) -> Router {
    Router::new()
        .route("/", get(|| async { "RTMP Multi-Destination Server" }))
        .route("/metrics", get({
            let metrics = metrics.clone();
            move || async move { metrics.export() }
        }))
}
