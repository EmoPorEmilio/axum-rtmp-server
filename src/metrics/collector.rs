use prometheus::{IntGauge, IntCounter, IntCounterVec, IntGaugeVec, Registry, TextEncoder, Opts};

pub struct MetricsCollector {
    pub connections_active: IntGauge,
    pub connections_total: IntCounter,
    pub connections_errors_total: IntCounter,
    pub streams_published: IntCounter,
    pub bytes_received: IntCounter,
    pub bytes_sent: IntCounter,
    pub chunks_received: IntCounter,
    pub auth_attempts_total: IntCounter,
    pub auth_failures_total: IntCounter,
    pub destinations_connected: IntGaugeVec,
    pub chunks_replicated_total: IntCounterVec,
    pub destination_errors_total: IntCounterVec,
    pub registry: Registry,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let registry = Registry::new();

        let connections_active = IntGauge::new(
            "rtmp_connections_active",
            "Current active RTMP connections"
        ).unwrap();
        let connections_total = IntCounter::new(
            "rtmp_connections_total",
            "Total RTMP connections"
        ).unwrap();
        let connections_errors_total = IntCounter::new(
            "rtmp_connections_errors_total",
            "Total connection errors"
        ).unwrap();
        let streams_published = IntCounter::new(
            "rtmp_streams_published",
            "Total streams published"
        ).unwrap();
        let bytes_received = IntCounter::new(
            "rtmp_bytes_received",
            "Total bytes received"
        ).unwrap();
        let bytes_sent = IntCounter::new(
            "rtmp_bytes_sent",
            "Total bytes sent"
        ).unwrap();
        let chunks_received = IntCounter::new(
            "rtmp_chunks_received",
            "Total chunks received"
        ).unwrap();
        let auth_attempts_total = IntCounter::new(
            "rtmp_auth_attempts_total",
            "Total auth attempts"
        ).unwrap();
        let auth_failures_total = IntCounter::new(
            "rtmp_auth_failures_total",
            "Total auth failures"
        ).unwrap();
        let destinations_connected = IntGaugeVec::new(
            Opts::new("rtmp_destinations_connected", "Current connected destinations"),
            &["destination"]
        ).unwrap();
        let chunks_replicated_total = IntCounterVec::new(
            Opts::new("rtmp_chunks_replicated_total", "Chunks replicated to destinations"),
            &["destination"]
        ).unwrap();
        let destination_errors_total = IntCounterVec::new(
            Opts::new("rtmp_destination_errors_total", "Destination errors"),
            &["destination"]
        ).unwrap();

        registry.register(Box::new(connections_active.clone())).unwrap();
        registry.register(Box::new(connections_total.clone())).unwrap();
        registry.register(Box::new(connections_errors_total.clone())).unwrap();
        registry.register(Box::new(streams_published.clone())).unwrap();
        registry.register(Box::new(bytes_received.clone())).unwrap();
        registry.register(Box::new(bytes_sent.clone())).unwrap();
        registry.register(Box::new(chunks_received.clone())).unwrap();
        registry.register(Box::new(auth_attempts_total.clone())).unwrap();
        registry.register(Box::new(auth_failures_total.clone())).unwrap();
        registry.register(Box::new(destinations_connected.clone())).unwrap();
        registry.register(Box::new(chunks_replicated_total.clone())).unwrap();
        registry.register(Box::new(destination_errors_total.clone())).unwrap();

        Self {
            connections_active,
            connections_total,
            connections_errors_total,
            streams_published,
            bytes_received,
            bytes_sent,
            chunks_received,
            auth_attempts_total,
            auth_failures_total,
            destinations_connected,
            chunks_replicated_total,
            destination_errors_total,
            registry,
        }
    }

    pub fn export(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        match encoder.encode_to_string(&metric_families) {
            Ok(s) => s,
            Err(e) => format!("# Error encoding metrics: {}", e),
        }
    }
}
