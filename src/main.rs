use axum::{routing::get, Router};
use bytes::{Buf, BytesMut};
use hex;
use rand::Rng;
use std::net::SocketAddr;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing_subscriber;

fn create_rtmp_header(
    chunk_type: u8,
    chunk_stream_id: u8,
    timestamp: u32,
    message_length: u32,
    message_type_id: u8,
    message_stream_id: u32,
) -> Vec<u8> {
    let mut header = Vec::new();
    header.push(chunk_type << 6 | chunk_stream_id);
    if chunk_type == 0 {
        header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
        header.extend_from_slice(&message_length.to_be_bytes()[1..]);
        header.push(message_type_id);
        header.extend_from_slice(&message_stream_id.to_le_bytes());
    }
    header
}

fn create_amf0_string(s: &str) -> Vec<u8> {
    let mut data = Vec::new();
    data.push(2); // AMF0 string marker
    data.extend_from_slice(&(s.len() as u16).to_be_bytes());
    data.extend_from_slice(s.as_bytes());
    data
}

fn create_amf0_number(n: f64) -> Vec<u8> {
    let mut data = Vec::new();
    data.push(0); // AMF0 number marker
    data.extend_from_slice(&n.to_be_bytes());
    data
}

fn create_amf0_object_start() -> Vec<u8> {
    vec![3] // AMF0 object start marker
}

fn create_amf0_object_end() -> Vec<u8> {
    vec![0, 0, 9] // AMF0 object end marker
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
                tracing::info!(
                    "Received {} bytes. Data: {}",
                    n,
                    hex::encode(&buffer[buffer.len() - n..])
                );
                while let Some(chunk) = parse_rtmp_chunk(&mut buffer) {
                    tracing::info!("Parsed chunk: {:?}", chunk);
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
    tracing::info!("Received C0C1: {}", hex::encode(&c0c1));

    // Verify RTMP version
    if c0c1[0] != 3 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unsupported RTMP version",
        ));
    }

    // Extract C1
    let c1 = &c0c1[1..];

    // Send S0 and S1 immediately
    let s0 = [3u8]; // RTMP version 3
    let mut s1 = [0u8; 1536];
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    s1[..4].copy_from_slice(&time.to_be_bytes());
    s1[4..8].copy_from_slice(&[0, 0, 0, 0]); // 4 zero bytes
    rand::thread_rng().fill(&mut s1[8..]); // Fill the rest with random data

    socket.write_all(&s0).await?;
    socket.write_all(&s1).await?;
    tracing::info!("Sent S0S1: {}", hex::encode(&s0) + &hex::encode(&s1));

    // Read C2
    let mut c2 = [0u8; 1536];
    socket.read_exact(&mut c2).await?;
    tracing::info!("Received C2: {}", hex::encode(&c2));

    // Send S2
    let mut s2 = [0u8; 1536];
    s2.copy_from_slice(c1); // Copy C1 to S2
    s2[..4].copy_from_slice(&time.to_be_bytes()); // Replace first 4 bytes with current timestamp
    socket.write_all(&s2).await?;
    tracing::info!("Sent S2: {}", hex::encode(&s2));

    Ok(())
}

fn read_u24(buffer: &mut BytesMut) -> u32 {
    ((buffer.get_u16() as u32) << 8) | (buffer.get_u8() as u32)
}

#[derive(Debug)]
struct RtmpChunk {
    chunk_type: u8,
    chunk_stream_id: u32,
    timestamp: u32,
    message_length: u32,
    message_type_id: u8,
    message_stream_id: u32,
    data: Vec<u8>,
}

fn parse_rtmp_chunk(buffer: &mut BytesMut) -> Option<RtmpChunk> {
    if buffer.len() < 1 {
        return None;
    }

    let first_byte = buffer[0];
    let chunk_type = first_byte >> 6;
    let chunk_stream_id = (first_byte & 0x3F) as u32;

    let mut chunk = RtmpChunk {
        chunk_type,
        chunk_stream_id,
        timestamp: 0,
        message_length: 0,
        message_type_id: 0,
        message_stream_id: 0,
        data: Vec::new(),
    };

    let header_size = match chunk_type {
        0 => 11,
        1 => 7,
        2 => 3,
        3 => 0,
        _ => return None,
    };

    if buffer.len() < 1 + header_size {
        return None;
    }

    buffer.advance(1); // Skip the first byte we've already processed

    if chunk_type == 0 {
        chunk.timestamp = read_u24(buffer);
        chunk.message_length = read_u24(buffer);
        chunk.message_type_id = buffer.get_u8();
        chunk.message_stream_id = buffer.get_u32_le();
    } else if chunk_type == 1 {
        chunk.timestamp = read_u24(buffer);
        chunk.message_length = read_u24(buffer);
        chunk.message_type_id = buffer.get_u8();
    } else if chunk_type == 2 {
        chunk.timestamp = read_u24(buffer);
    }

    // For simplicity, we're assuming the chunk data follows immediately
    // In a full implementation, you'd need to handle chunking properly
    if buffer.len() >= chunk.message_length as usize {
        chunk.data = buffer.split_to(chunk.message_length as usize).to_vec();
        Some(chunk)
    } else {
        None
    }
}

async fn handle_rtmp_chunk(
    socket: &mut tokio::net::TcpStream,
    chunk: RtmpChunk,
) -> Result<(), std::io::Error> {
    tracing::info!("Handling RTMP chunk: {:?}", chunk);
    match chunk.message_type_id {
        1 => {
            tracing::info!("Received Set Chunk Size: {:?}", chunk.data);
            // Update chunk size if needed
        }
        4 => tracing::info!("Received User Control Message"),
        5 => tracing::info!("Received Window Acknowledgement Size"),
        6 => tracing::info!("Received Set Peer Bandwidth"),
        20 => handle_amf0_command(socket, &chunk.data).await?,
        _ => tracing::warn!("Unhandled RTMP message type: {}", chunk.message_type_id),
    }
    Ok(())
}

async fn handle_amf0_command(
    socket: &mut tokio::net::TcpStream,
    data: &[u8],
) -> Result<(), std::io::Error> {
    if data.starts_with(b"\x02\x00\x07connect") {
        tracing::info!("Received connect command: {}", hex::encode(data));
        // Extract transaction ID (it's the second parameter after the command name)
        let transaction_id = f64::from_be_bytes(data[11..19].try_into().unwrap());
        handle_connect_command(socket, transaction_id).await?;
    } else if data.starts_with(b"\x02\x00\x0ccreateStream") {
        tracing::info!("Received createStream command: {}", hex::encode(data));
        handle_create_stream_command(socket).await?;
    } else if data.starts_with(b"\x02\x00\x07publish") {
        tracing::info!("Received publish command: {}", hex::encode(data));
        handle_publish_command(socket).await?;
    } else {
        tracing::warn!("Unknown AMF0 command: {}", hex::encode(data));
    }
    Ok(())
}

async fn handle_connect_command(
    socket: &mut tokio::net::TcpStream,
    transaction_id: f64,
) -> Result<(), std::io::Error> {
    tracing::info!(
        "Handling connect command with transaction ID: {}",
        transaction_id
    );

    // Send Window Acknowledgement Size
    let mut window_ack_size = create_rtmp_header(2, 2, 0, 4, 5, 0);
    window_ack_size.extend_from_slice(&(2500000u32).to_be_bytes());
    socket.write_all(&window_ack_size).await?;
    tracing::info!("Sent Window Ack Size: {}", hex::encode(&window_ack_size));

    // Send Set Peer Bandwidth
    let mut set_peer_bandwidth = create_rtmp_header(2, 2, 0, 5, 6, 0);
    set_peer_bandwidth.extend_from_slice(&(2500000u32).to_be_bytes());
    set_peer_bandwidth.push(2); // Dynamic
    socket.write_all(&set_peer_bandwidth).await?;
    tracing::info!(
        "Sent Set Peer Bandwidth: {}",
        hex::encode(&set_peer_bandwidth)
    );

    // Construct _result for connect
    let mut result = Vec::new();
    result.extend(create_amf0_string("_result"));
    result.extend(create_amf0_number(transaction_id));
    result.extend(create_amf0_object_start());
    result.extend(create_amf0_string("fmsVer"));
    result.extend(create_amf0_string("FMS/3,5,3,888"));
    result.extend(create_amf0_string("capabilities"));
    result.extend(create_amf0_number(31.0));
    result.extend(create_amf0_object_end());
    result.extend(create_amf0_object_start());
    result.extend(create_amf0_string("level"));
    result.extend(create_amf0_string("status"));
    result.extend(create_amf0_string("code"));
    result.extend(create_amf0_string("NetConnection.Connect.Success"));
    result.extend(create_amf0_string("description"));
    result.extend(create_amf0_string("Connection succeeded."));
    result.extend(create_amf0_string("objectEncoding"));
    result.extend(create_amf0_number(0.0));
    result.extend(create_amf0_object_end());

    // Send _result for connect
    let mut connect_result = create_rtmp_header(0, 3, 0, result.len() as u32, 20, 0);
    connect_result.extend(result);
    socket.write_all(&connect_result).await?;
    tracing::info!("Sent Connect Result: {}", hex::encode(&connect_result));

    // Send Stream Begin (User Control Message)
    let mut stream_begin = create_rtmp_header(2, 0, 0, 6, 4, 0);
    stream_begin.extend_from_slice(&[0, 0, 0, 0]); // Event type (Stream Begin)
    stream_begin.extend_from_slice(&[0, 0, 0, 0]); // Stream ID (0 for now)
    socket.write_all(&stream_begin).await?;
    tracing::info!("Sent Stream Begin: {}", hex::encode(&stream_begin));

    tracing::info!("Completed connect command handling");
    Ok(())
}

async fn handle_create_stream_command(
    socket: &mut tokio::net::TcpStream,
) -> Result<(), std::io::Error> {
    tracing::info!("Handling createStream command");
    let mut result = Vec::new();
    result.extend(create_amf0_string("_result"));
    result.extend(create_amf0_number(2.0)); // Transaction ID
    result.extend(create_amf0_number(0.0)); // NULL
    result.extend(create_amf0_number(1.0)); // Stream ID

    let mut create_stream_result = create_rtmp_header(0, 3, 0, result.len() as u32, 20, 0);
    create_stream_result.extend(result);
    tracing::info!(
        "Sending CreateStream Result: {}",
        hex::encode(&create_stream_result)
    );
    socket.write_all(&create_stream_result).await?;
    Ok(())
}

async fn handle_publish_command(socket: &mut tokio::net::TcpStream) -> Result<(), std::io::Error> {
    tracing::info!("Handling publish command");

    // Send Stream Begin
    let mut stream_begin = create_rtmp_header(2, 2, 0, 6, 4, 1);
    stream_begin.extend_from_slice(&[0, 0, 0, 0, 0, 1]);
    socket.write_all(&stream_begin).await?;

    // Send onStatus
    let mut result = Vec::new();
    result.extend(create_amf0_string("onStatus"));
    result.extend(create_amf0_number(0.0)); // Transaction ID
    result.extend(create_amf0_number(0.0)); // NULL
    result.extend(create_amf0_object_start());
    result.extend(create_amf0_string("level"));
    result.extend(create_amf0_string("status"));
    result.extend(create_amf0_string("code"));
    result.extend(create_amf0_string("NetStream.Publish.Start"));
    result.extend(create_amf0_string("description"));
    result.extend(create_amf0_string("Stream is now published."));
    result.extend(create_amf0_object_end());

    let mut publish_result = create_rtmp_header(0, 5, 0, result.len() as u32, 20, 1);
    publish_result.extend(result);
    tracing::info!("Sending Publish Result: {}", hex::encode(&publish_result));
    socket.write_all(&publish_result).await?;
    Ok(())
}
