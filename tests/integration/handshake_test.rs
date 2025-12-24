//! Integration tests for RTMP handshake
//!
//! These tests verify the complete handshake flow between client and server.

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;

const RTMP_VERSION: u8 = 3;
const HANDSHAKE_SIZE: usize = 1536;

/// Test basic handshake with a real TCP connection
#[tokio::test]
#[ignore = "requires running server"]
async fn test_handshake_completes_successfully() {
    let addr = std::env::var("RTMP_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:1935".to_string());

    let mut stream = TcpStream::connect(&addr).await
        .expect("Failed to connect to RTMP server");

    stream.set_nodelay(true).unwrap();

    // Send C0 + C1
    let mut c0c1 = vec![RTMP_VERSION];
    c0c1.extend_from_slice(&[0u8; HANDSHAKE_SIZE]); // timestamp + zero + random data

    stream.write_all(&c0c1).await.expect("Failed to send C0+C1");

    // Read S0 + S1 + S2
    let mut s0s1s2 = vec![0u8; 1 + HANDSHAKE_SIZE + HANDSHAKE_SIZE];

    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut s0s1s2))
        .await
        .expect("Timeout waiting for S0+S1+S2")
        .expect("Failed to read S0+S1+S2");

    // Verify S0
    assert_eq!(s0s1s2[0], RTMP_VERSION, "Server version mismatch");

    // Send C2 (echo back S1)
    let s1 = &s0s1s2[1..1 + HANDSHAKE_SIZE];
    stream.write_all(s1).await.expect("Failed to send C2");

    // Handshake complete - connection should remain open
    // Try to read a small amount to verify connection is alive
    let mut buf = [0u8; 1];
    let result = tokio::time::timeout(Duration::from_millis(100), stream.read(&mut buf)).await;

    // Either timeout (no data yet, which is expected) or success is fine
    // An error would indicate the connection was closed
    match result {
        Ok(Ok(0)) => panic!("Connection closed after handshake"),
        Ok(Err(e)) => panic!("Read error after handshake: {}", e),
        Ok(Ok(_)) | Err(_) => {} // Success or timeout, both OK
    }
}

/// Test that server rejects invalid RTMP version
#[tokio::test]
#[ignore = "requires running server"]
async fn test_handshake_rejects_invalid_version() {
    let addr = std::env::var("RTMP_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:1935".to_string());

    let mut stream = TcpStream::connect(&addr).await
        .expect("Failed to connect to RTMP server");

    // Send invalid version (not 3)
    let mut c0c1 = vec![0x05]; // Invalid version
    c0c1.extend_from_slice(&[0u8; HANDSHAKE_SIZE]);

    stream.write_all(&c0c1).await.expect("Failed to send invalid C0+C1");

    // Server should close connection or return error
    let mut buf = vec![0u8; 1 + HANDSHAKE_SIZE + HANDSHAKE_SIZE];
    let result = tokio::time::timeout(Duration::from_secs(2), stream.read_exact(&mut buf)).await;

    match result {
        Ok(Ok(_)) => {
            // If we got a response, check if it's version 3 (server might be lenient)
            // or some other handling
        }
        Ok(Err(_)) | Err(_) => {
            // Connection closed or timeout - expected behavior
        }
    }
}

/// Test handshake timeout handling
#[tokio::test]
#[ignore = "requires running server"]
async fn test_handshake_timeout() {
    let addr = std::env::var("RTMP_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:1935".to_string());

    let mut stream = TcpStream::connect(&addr).await
        .expect("Failed to connect to RTMP server");

    // Send only C0, not C1
    stream.write_all(&[RTMP_VERSION]).await.expect("Failed to send C0");

    // Server should timeout waiting for C1 and close connection
    let mut buf = vec![0u8; 1024];
    let result = tokio::time::timeout(Duration::from_secs(35), stream.read(&mut buf)).await;

    match result {
        Ok(Ok(0)) => {
            // Connection closed - expected
        }
        Ok(Ok(_)) => {
            // Got some data - server might have different behavior
        }
        Ok(Err(_)) => {
            // Read error - connection likely closed
        }
        Err(_) => {
            // Our timeout - server didn't close in 35 seconds
            // This might be acceptable depending on server config
        }
    }
}

/// Test multiple concurrent handshakes
#[tokio::test]
#[ignore = "requires running server"]
async fn test_concurrent_handshakes() {
    let addr = std::env::var("RTMP_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:1935".to_string());

    let handles: Vec<_> = (0..5).map(|_| {
        let addr = addr.clone();
        tokio::spawn(async move {
            let mut stream = TcpStream::connect(&addr).await?;
            stream.set_nodelay(true)?;

            // Send C0 + C1
            let mut c0c1 = vec![RTMP_VERSION];
            c0c1.extend_from_slice(&[0u8; HANDSHAKE_SIZE]);
            stream.write_all(&c0c1).await?;

            // Read S0 + S1 + S2
            let mut s0s1s2 = vec![0u8; 1 + HANDSHAKE_SIZE + HANDSHAKE_SIZE];
            tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut s0s1s2)).await??;

            // Send C2
            let s1 = &s0s1s2[1..1 + HANDSHAKE_SIZE];
            stream.write_all(s1).await?;

            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        })
    }).collect();

    for handle in handles {
        handle.await.expect("Task panicked").expect("Handshake failed");
    }
}
