use axum::{routing::get, Router};
use bytes::{Buf, BytesMut};
use rand::Rng;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing_subscriber;

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

async fn handle_rtmp_connection(mut socket: tokio::net::TcpStream) {
    tracing::info!("New RTMP connection");

    if let Err(e) = handle_rtmp_handshake(&mut socket).await {
        tracing::error!("RTMP handshake failed: {}", e);
        return;
    }
    tracing::info!("RTMP handshake completed successfully");

    let mut buffer = BytesMut::with_capacity(4096);
    loop {
        match socket.read_buf(&mut buffer).await {
            Ok(0) => {
                tracing::info!(
                    "RTMP connection closed by client. Total bytes received: {}",
                    buffer.len()
                );
                break;
            }
            Ok(n) => {
                tracing::info!("Received {} bytes. Total: {}.", n, buffer.len());
                while let Some(chunk) = parse_rtmp_chunk(&mut buffer) {
                    if let Err(e) = handle_rtmp_chunk(&mut socket, chunk).await {
                        tracing::error!("Error handling RTMP chunk: {}", e);
                        return;
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    "Failed to read from socket: {}. Total bytes received: {}",
                    e,
                    buffer.len()
                );
                break;
            }
        }
    }
}

async fn handle_rtmp_handshake(socket: &mut tokio::net::TcpStream) -> Result<(), std::io::Error> {
    // Read C0 and C1
    let mut c0c1 = [0u8; 1537];
    socket.read_exact(&mut c0c1).await?;

    // Verify RTMP version
    if c0c1[0] != 3 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unsupported RTMP version",
        ));
    }

    // Extract C1
    let c1 = &c0c1[1..];

    // Send S0, S1, and S2
    let s0 = [3u8]; // RTMP version 3
    let mut s1 = [0u8; 1536];
    let mut s2 = [0u8; 1536];

    // Fill S1 with timestamp and random data
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    s1[..4].copy_from_slice(&time.to_be_bytes());
    s1[4..8].copy_from_slice(&[0, 0, 0, 0]); // 4 zero bytes
    rand::thread_rng().fill(&mut s1[8..]); // Fill the rest with random data

    // S2 is a copy of C1 with the first 4 bytes replaced by the current timestamp
    s2.copy_from_slice(c1);
    s2[..4].copy_from_slice(&time.to_be_bytes());

    socket.write_all(&s0).await?;
    socket.write_all(&s1).await?;
    socket.write_all(&s2).await?;

    // Read C2
    let mut c2 = [0u8; 1536];
    socket.read_exact(&mut c2).await?;

    Ok(())
}

#[derive(Debug)]
struct RtmpChunk {
    message_type_id: u8,
    // Add other fields as needed
}

fn parse_rtmp_chunk(buffer: &mut BytesMut) -> Option<RtmpChunk> {
    if buffer.len() < 12 {
        return None;
    }

    // This is a very basic parser and needs to be expanded
    let message_type_id = buffer[7];

    // Remove the parsed data from the buffer
    buffer.advance(12);

    Some(RtmpChunk { message_type_id })
}

async fn handle_rtmp_chunk(
    socket: &mut tokio::net::TcpStream,
    chunk: RtmpChunk,
) -> Result<(), std::io::Error> {
    match chunk.message_type_id {
        20 => handle_connect_command(socket).await?,
        // Handle other message types...
        _ => tracing::warn!("Unhandled RTMP message type: {}", chunk.message_type_id),
    }
    Ok(())
}

async fn handle_connect_command(socket: &mut tokio::net::TcpStream) -> Result<(), std::io::Error> {
    // Send Window Acknowledgement Size
    let window_ack_size = [2, 0, 0, 0, 0, 0, 4, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    socket.write_all(&window_ack_size).await?;

    // Send Set Peer Bandwidth
    let set_peer_bandwidth = [
        2, 0, 0, 0, 0, 0, 5, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
    ];
    socket.write_all(&set_peer_bandwidth).await?;

    // Send Stream Begin
    let stream_begin = [
        2, 0, 0, 0, 0, 0, 6, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    socket.write_all(&stream_begin).await?;

    // Send _result for connect
    let connect_result = [
        3, 0, 0, 0, 0, 0, 99, 20, 0, 0, 0, 0, 2, 0, 7, 95, 114, 101, 115, 117, 108, 116, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 3, 0, 6, 102, 109, 115, 86, 101, 114, 2, 0, 10, 70, 77, 83, 47, 51, 44,
        53, 44, 51, 44, 56, 56, 56, 0, 12, 99, 97, 112, 97, 98, 105, 108, 105, 116, 105, 101, 115,
        0, 64, 47, 0, 0, 0, 0, 0, 0, 0, 9, 3, 0, 5, 108, 101, 118, 101, 108, 2, 0, 6, 115, 116, 97,
        116, 117, 115, 0, 4, 99, 111, 100, 101, 0, 64, 28, 0, 0, 0, 0, 0, 0, 0, 11, 100, 101, 115,
        99, 114, 105, 112, 116, 105, 111, 110, 2, 0, 21, 67, 111, 110, 110, 101, 99, 116, 105, 111,
        110, 32, 115, 117, 99, 99, 101, 101, 100, 101, 100, 46, 0, 10, 111, 98, 106, 101, 99, 116,
        69, 110, 99, 111, 100, 105, 110, 103, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 9,
    ];
    socket.write_all(&connect_result).await?;

    tracing::info!("Sent connect response");
    Ok(())
}
