//! Integration tests for destination replication
//!
//! These tests verify that streams are correctly forwarded to destination servers.
//! Requires docker-compose environment with nginx-rtmp containers.

use std::time::Duration;
use tokio::time::sleep;

/// Check if nginx-rtmp server has active streams
async fn check_nginx_rtmp_stats(url: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let response = client.get(url).send().await?;
    let body = response.text().await?;

    // nginx-rtmp stats XML contains <publishing> when stream is active
    Ok(body.contains("<publishing>") || body.contains("publishing"))
}

/// Wait for nginx-rtmp to show an active stream
async fn wait_for_stream(
    stats_url: &str,
    timeout: Duration,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let start = tokio::time::Instant::now();

    while start.elapsed() < timeout {
        match check_nginx_rtmp_stats(stats_url).await {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(e) => {
                tracing::debug!("Error checking stats: {}", e);
            }
        }
        sleep(Duration::from_millis(500)).await;
    }

    Ok(false)
}

/// Test that stream replicates to primary destination
#[tokio::test]
#[ignore = "requires docker-compose environment"]
async fn test_replication_to_primary() {
    let primary_stats = std::env::var("PRIMARY_STATS_URL")
        .unwrap_or_else(|_| "http://localhost:8081/stat".to_string());

    // This test assumes FFmpeg is already streaming to our RTMP server
    // and we're checking if it gets replicated to nginx-rtmp-primary

    let has_stream = wait_for_stream(&primary_stats, Duration::from_secs(30))
        .await
        .expect("Failed to check stream status");

    assert!(has_stream, "Stream should be replicated to primary destination");
}

/// Test that stream replicates to backup destination
#[tokio::test]
#[ignore = "requires docker-compose environment"]
async fn test_replication_to_backup() {
    let backup_stats = std::env::var("BACKUP_STATS_URL")
        .unwrap_or_else(|_| "http://localhost:8082/stat".to_string());

    let has_stream = wait_for_stream(&backup_stats, Duration::from_secs(30))
        .await
        .expect("Failed to check stream status");

    assert!(has_stream, "Stream should be replicated to backup destination");
}

/// Test that both destinations receive stream simultaneously
#[tokio::test]
#[ignore = "requires docker-compose environment"]
async fn test_multi_destination_replication() {
    let primary_stats = std::env::var("PRIMARY_STATS_URL")
        .unwrap_or_else(|_| "http://localhost:8081/stat".to_string());
    let backup_stats = std::env::var("BACKUP_STATS_URL")
        .unwrap_or_else(|_| "http://localhost:8082/stat".to_string());

    // Wait for both destinations to receive stream
    let timeout = Duration::from_secs(30);
    let start = tokio::time::Instant::now();

    loop {
        if start.elapsed() > timeout {
            panic!("Timeout waiting for streams on both destinations");
        }

        let primary_ok = check_nginx_rtmp_stats(&primary_stats).await.unwrap_or(false);
        let backup_ok = check_nginx_rtmp_stats(&backup_stats).await.unwrap_or(false);

        if primary_ok && backup_ok {
            break;
        }

        sleep(Duration::from_millis(500)).await;
    }
}

/// Test metrics endpoint during active streaming
#[tokio::test]
#[ignore = "requires docker-compose environment"]
async fn test_metrics_during_stream() {
    let metrics_url = std::env::var("METRICS_URL")
        .unwrap_or_else(|_| "http://localhost:8080/metrics".to_string());

    let client = reqwest::Client::new();
    let response = client.get(&metrics_url).send().await
        .expect("Failed to fetch metrics");

    let body = response.text().await.expect("Failed to read metrics body");

    // Verify key metrics are present
    assert!(body.contains("bytes_received") || body.contains("rtmp_"),
        "Metrics should include stream statistics");
}

/// Test reconnection after destination failure
#[tokio::test]
#[ignore = "requires docker-compose environment with orchestration"]
async fn test_destination_reconnection() {
    // This test would require:
    // 1. Start streaming
    // 2. Stop nginx-rtmp-primary container
    // 3. Verify stream continues to backup
    // 4. Restart nginx-rtmp-primary
    // 5. Verify reconnection occurs

    // For now, this is a placeholder for manual testing
    // Full automation would require docker CLI access from test
}
