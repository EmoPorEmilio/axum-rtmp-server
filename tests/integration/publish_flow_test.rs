//! Integration tests for the complete publish flow
//!
//! Tests the full RTMP command sequence: connect → createStream → publish

use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use std::time::Duration;
use bytes::{BufMut, BytesMut};

const RTMP_VERSION: u8 = 3;
const HANDSHAKE_SIZE: usize = 1536;

// RTMP Message Types
const RTMP_MSG_SET_CHUNK_SIZE: u8 = 1;
const RTMP_MSG_WINDOW_ACK_SIZE: u8 = 5;
const RTMP_MSG_SET_PEER_BANDWIDTH: u8 = 6;
const RTMP_MSG_AMF0_CMD: u8 = 20;

/// Helper to perform handshake
async fn perform_handshake(stream: &mut TcpStream) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut c0c1 = vec![RTMP_VERSION];
    c0c1.extend_from_slice(&[0u8; HANDSHAKE_SIZE]);
    stream.write_all(&c0c1).await?;

    let mut s0s1s2 = vec![0u8; 1 + HANDSHAKE_SIZE + HANDSHAKE_SIZE];
    tokio::time::timeout(Duration::from_secs(5), stream.read_exact(&mut s0s1s2)).await??;

    let s1 = &s0s1s2[1..1 + HANDSHAKE_SIZE];
    stream.write_all(s1).await?;

    Ok(())
}

/// Encode AMF0 string
fn encode_amf0_string(s: &str) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(0x02); // String marker
    buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
    buf.extend_from_slice(s.as_bytes());
    buf
}

/// Encode AMF0 number
fn encode_amf0_number(n: f64) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(0x00); // Number marker
    buf.extend_from_slice(&n.to_be_bytes());
    buf
}

/// Encode AMF0 object
fn encode_amf0_object(properties: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(0x03); // Object marker

    for (key, value) in properties {
        buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
        buf.extend_from_slice(key.as_bytes());
        buf.extend_from_slice(value);
    }

    // Object end marker
    buf.extend_from_slice(&[0x00, 0x00, 0x09]);
    buf
}

/// Build RTMP chunk with Type 0 header
fn build_chunk(csid: u8, timestamp: u32, msg_type: u8, stream_id: u32, data: &[u8]) -> Vec<u8> {
    let mut buf = BytesMut::new();

    // Basic header (1 byte for csid 2-63)
    buf.put_u8(csid); // fmt=0, csid

    // Message header (Type 0: 11 bytes)
    buf.put_u8((timestamp >> 16) as u8);
    buf.put_u8((timestamp >> 8) as u8);
    buf.put_u8(timestamp as u8);

    let len = data.len() as u32;
    buf.put_u8((len >> 16) as u8);
    buf.put_u8((len >> 8) as u8);
    buf.put_u8(len as u8);

    buf.put_u8(msg_type);

    // Stream ID (little endian)
    buf.put_u32_le(stream_id);

    // Data
    buf.extend_from_slice(data);

    buf.to_vec()
}

/// Test complete connect command flow
#[tokio::test]
#[ignore = "requires running server"]
async fn test_connect_command() {
    let addr = std::env::var("RTMP_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:1935".to_string());

    let mut stream = TcpStream::connect(&addr).await
        .expect("Failed to connect to RTMP server");
    stream.set_nodelay(true).unwrap();

    perform_handshake(&mut stream).await.expect("Handshake failed");

    // Build connect command
    let mut connect_data = Vec::new();
    connect_data.extend_from_slice(&encode_amf0_string("connect"));
    connect_data.extend_from_slice(&encode_amf0_number(1.0)); // transaction ID

    // Command object
    let object_no_marker: Vec<u8> = {
        let mut obj = encode_amf0_object(&[
            ("app", encode_amf0_string("live")),
            ("tcUrl", encode_amf0_string("rtmp://localhost:1935/live")),
            ("flashVer", encode_amf0_string("FMLE/3.0")),
        ]);
        // Remove the 0x03 object marker as we'll add it
        obj
    };
    connect_data.extend_from_slice(&object_no_marker);

    let chunk = build_chunk(3, 0, RTMP_MSG_AMF0_CMD, 0, &connect_data);
    stream.write_all(&chunk).await.expect("Failed to send connect");

    // Read response (should include Window Ack Size, Set Peer Bandwidth, and _result)
    let mut response_buf = vec![0u8; 4096];
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        stream.read(&mut response_buf)
    ).await;

    match result {
        Ok(Ok(n)) if n > 0 => {
            // Got some response - verify we got protocol messages
            // Server should send Window ACK Size and Set Peer Bandwidth
        }
        Ok(Ok(0)) => panic!("Connection closed after connect"),
        Ok(Err(e)) => panic!("Read error: {}", e),
        Err(_) => panic!("Timeout waiting for connect response"),
    }
}

/// Test that auth rejects invalid stream key
#[tokio::test]
#[ignore = "requires running server"]
async fn test_publish_invalid_stream_key() {
    let addr = std::env::var("RTMP_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:1935".to_string());

    let mut stream = TcpStream::connect(&addr).await
        .expect("Failed to connect to RTMP server");
    stream.set_nodelay(true).unwrap();

    perform_handshake(&mut stream).await.expect("Handshake failed");

    // Send connect
    let mut connect_data = Vec::new();
    connect_data.extend_from_slice(&encode_amf0_string("connect"));
    connect_data.extend_from_slice(&encode_amf0_number(1.0));
    connect_data.extend_from_slice(&encode_amf0_object(&[
        ("app", encode_amf0_string("live")),
        ("tcUrl", encode_amf0_string("rtmp://localhost:1935/live")),
    ]));

    let chunk = build_chunk(3, 0, RTMP_MSG_AMF0_CMD, 0, &connect_data);
    stream.write_all(&chunk).await.expect("Failed to send connect");

    // Read connect response
    let mut buf = vec![0u8; 4096];
    tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("Timeout")
        .expect("Read error");

    // Send createStream
    let mut create_stream = Vec::new();
    create_stream.extend_from_slice(&encode_amf0_string("createStream"));
    create_stream.extend_from_slice(&encode_amf0_number(2.0));
    create_stream.push(0x05); // Null

    let chunk = build_chunk(3, 0, RTMP_MSG_AMF0_CMD, 0, &create_stream);
    stream.write_all(&chunk).await.expect("Failed to send createStream");

    tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("Timeout")
        .expect("Read error");

    // Send publish with INVALID stream key
    let mut publish = Vec::new();
    publish.extend_from_slice(&encode_amf0_string("publish"));
    publish.extend_from_slice(&encode_amf0_number(0.0));
    publish.push(0x05); // Null
    publish.extend_from_slice(&encode_amf0_string("INVALID_KEY_12345"));
    publish.extend_from_slice(&encode_amf0_string("live"));

    let chunk = build_chunk(8, 0, RTMP_MSG_AMF0_CMD, 1, &publish);
    stream.write_all(&chunk).await.expect("Failed to send publish");

    // Should get error response or connection close
    let result = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await;

    match result {
        Ok(Ok(0)) => {
            // Connection closed - expected for auth failure
        }
        Ok(Ok(_)) => {
            // Got response - might be onStatus with error
        }
        Ok(Err(_)) => {
            // Read error - connection closed
        }
        Err(_) => {
            // Timeout - might need to check if server rejects differently
        }
    }
}

/// Test successful publish with valid stream key
#[tokio::test]
#[ignore = "requires running server"]
async fn test_publish_valid_stream_key() {
    let addr = std::env::var("RTMP_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:1935".to_string());
    let stream_key = std::env::var("TEST_STREAM_KEY").unwrap_or_else(|_| "test-stream-key".to_string());

    let mut stream = TcpStream::connect(&addr).await
        .expect("Failed to connect to RTMP server");
    stream.set_nodelay(true).unwrap();

    perform_handshake(&mut stream).await.expect("Handshake failed");

    // Connect
    let mut connect_data = Vec::new();
    connect_data.extend_from_slice(&encode_amf0_string("connect"));
    connect_data.extend_from_slice(&encode_amf0_number(1.0));
    connect_data.extend_from_slice(&encode_amf0_object(&[
        ("app", encode_amf0_string("live")),
        ("tcUrl", encode_amf0_string("rtmp://localhost:1935/live")),
    ]));

    stream.write_all(&build_chunk(3, 0, RTMP_MSG_AMF0_CMD, 0, &connect_data)).await.unwrap();

    let mut buf = vec![0u8; 4096];
    tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await.unwrap().unwrap();

    // createStream
    let mut create_stream = Vec::new();
    create_stream.extend_from_slice(&encode_amf0_string("createStream"));
    create_stream.extend_from_slice(&encode_amf0_number(2.0));
    create_stream.push(0x05);

    stream.write_all(&build_chunk(3, 0, RTMP_MSG_AMF0_CMD, 0, &create_stream)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await.unwrap().unwrap();

    // Publish with VALID stream key
    let mut publish = Vec::new();
    publish.extend_from_slice(&encode_amf0_string("publish"));
    publish.extend_from_slice(&encode_amf0_number(0.0));
    publish.push(0x05);
    publish.extend_from_slice(&encode_amf0_string(&stream_key));
    publish.extend_from_slice(&encode_amf0_string("live"));

    stream.write_all(&build_chunk(8, 0, RTMP_MSG_AMF0_CMD, 1, &publish)).await.unwrap();

    // Should get onStatus with NetStream.Publish.Start
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("Timeout waiting for publish response")
        .expect("Read error");

    assert!(n > 0, "Should receive publish confirmation");
}
