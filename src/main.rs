use axum::{routing::get, Router};
use bytes::{Buf, BufMut, BytesMut};
use dashmap::DashMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

const RTMP_VERSION: u8 = 3;
const DEFAULT_CHUNK_SIZE: u32 = 128;
const MAX_CHUNK_SIZE: u32 = 65536;
const INITIAL_BUFFER_SIZE: usize = 4096;

const RTMP_MSG_WINDOW_ACK_SIZE: u8 = 0x5;
const RTMP_MSG_SET_CHUNK_SIZE: u8 = 0x1;
const RTMP_MSG_ACK: u8 = 0x3;
const RTMP_MSG_USER_CONTROL: u8 = 0x4;
const RTMP_MSG_SET_PEER_BANDWIDTH: u8 = 0x6;
const RTMP_MSG_AUDIO: u8 = 0x8;
const RTMP_MSG_VIDEO: u8 = 0x9;
const RTMP_MSG_AMF3_CMD: u8 = 0x11;
const RTMP_MSG_AMF3_DATA: u8 = 0x0F;
const RTMP_MSG_AMF0_CMD: u8 = 0x14;
const RTMP_MSG_AMF0_DATA: u8 = 0x12;

#[derive(Error, Debug)]
pub enum RtmpError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Invalid chunk size: {0}")]
    InvalidChunkSize(u32),
    #[error("Invalid AMF data: {0}")]
    InvalidAmf(String),
}

#[derive(Debug, Clone)]
struct StreamMetadata {
    app_name: String,
    stream_key: String,
    chunk_size: u32,
    window_size: u32,
    bandwidth: u32,
}

impl StreamMetadata {
    fn new(app_name: String, stream_key: String) -> Self {
        Self {
            app_name,
            stream_key,
            chunk_size: DEFAULT_CHUNK_SIZE,
            window_size: 2500000,
            bandwidth: 2500000,
        }
    }
}

#[derive(Debug, Clone)]
struct RtmpChunk {
    chunk_type: u8,
    chunk_stream_id: u32,
    timestamp: u32,
    message_length: u32,
    message_type_id: u8,
    message_stream_id: u32,
    data: Vec<u8>,
}
// Add this struct to store previous chunk information
#[derive(Debug, Clone, Default)]
struct ChunkStreamState {
    timestamp: u32,
    message_length: u32,
    message_type_id: u8,
    message_stream_id: u32,
    bytes_left: u32,
    data: Vec<u8>,
}

struct RtmpConnection {
    socket: tokio::net::TcpStream,
    buffer: BytesMut,
    chunk_size: u32,
    streams: Arc<DashMap<u32, StreamMetadata>>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    chunk_states: HashMap<u32, ChunkStreamState>,
}

impl RtmpConnection {
    fn new(socket: tokio::net::TcpStream, streams: Arc<DashMap<u32, StreamMetadata>>) -> Self {
        let (tx, _) = broadcast::channel(100);
        Self {
            socket,
            buffer: BytesMut::with_capacity(INITIAL_BUFFER_SIZE),
            chunk_size: DEFAULT_CHUNK_SIZE,
            streams,
            broadcast_tx: tx,
            chunk_states: HashMap::new(),
        }
    }

    async fn handle_connection(mut self) -> Result<(), RtmpError> {
        self.perform_handshake().await?;

        while let Some(chunk) = self.read_chunk().await? {
            self.process_chunk(chunk).await?;
        }
        Ok(())
    }

    async fn perform_handshake(&mut self) -> Result<(), RtmpError> {
        // Read C0+C1 (1+1536 bytes)
        let mut c0c1 = [0u8; 1537];
        self.socket.read_exact(&mut c0c1).await?;

        // Validate RTMP version
        if c0c1[0] != RTMP_VERSION {
            return Err(RtmpError::Protocol(format!(
                "Unsupported RTMP version: {}",
                c0c1[0]
            )));
        }

        tracing::debug!("Received C0+C1");

        // Prepare S0+S1+S2 response (1 + 1536 + 1536 bytes)
        let mut response = Vec::with_capacity(3073);

        // S0 - Version
        response.push(RTMP_VERSION);

        // S1 - Timestamp + Zero + Random
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        response.extend_from_slice(&time.to_be_bytes());
        response.extend_from_slice(&[0u8; 4]); // zeros

        // Generate random bytes for S1
        let random_bytes: Vec<u8> = (0..1528).map(|_| rand::random::<u8>()).collect();
        response.extend_from_slice(&random_bytes);

        // S2 - Echo client's C1
        response.extend_from_slice(&c0c1[1..1537]);

        // Send S0+S1+S2
        self.socket.write_all(&response).await?;
        tracing::debug!("Sent S0+S1+S2");

        // Read C2
        let mut c2 = [0u8; 1536];
        self.socket.read_exact(&mut c2).await?;
        tracing::debug!("Received C2");

        tracing::info!("Handshake completed successfully");
        Ok(())
    }

    async fn read_chunk(&mut self) -> Result<Option<RtmpChunk>, RtmpError> {
        if self.buffer.len() < 1 {
            tracing::debug!("Reading more data into buffer");
            let bytes_read = self.socket.read_buf(&mut self.buffer).await?;
            if bytes_read == 0 {
                tracing::debug!("Connection closed by peer");
                return Ok(None);
            }
            tracing::debug!("Read {} bytes into buffer", bytes_read);
        }

        tracing::debug!("Current buffer content: {:02X?}", self.buffer);

        let first_byte = self.buffer[0];
        let chunk_type = first_byte >> 6;
        let chunk_stream_id = (first_byte & 0x3F) as u32;

        let header_size = match chunk_type {
            0 => 11,
            1 => 7,
            2 => 3,
            3 => 0,
            _ => return Err(RtmpError::Protocol("Invalid chunk type".into())),
        };

        if self.buffer.len() < header_size + 1 {
            return Ok(None);
        }

        let mut timestamp = 0u32;
        let mut message_length = 0u32;
        let mut message_type_id = 0u8;
        let mut message_stream_id = 0u32;

        let previous_state = self.chunk_states.get(&chunk_stream_id).cloned();
        let mut pos = 1;

        match chunk_type {
            0 => {
                timestamp = u32::from_be_bytes([
                    0,
                    self.buffer[pos],
                    self.buffer[pos + 1],
                    self.buffer[pos + 2],
                ]);
                pos += 3;

                message_length = u32::from_be_bytes([
                    0,
                    self.buffer[pos],
                    self.buffer[pos + 1],
                    self.buffer[pos + 2],
                ]);
                pos += 3;

                message_type_id = self.buffer[pos];
                pos += 1;

                message_stream_id = u32::from_le_bytes([
                    self.buffer[pos],
                    self.buffer[pos + 1],
                    self.buffer[pos + 2],
                    self.buffer[pos + 3],
                ]);
                pos += 4;
            }
            1 => {
                timestamp = u32::from_be_bytes([
                    0,
                    self.buffer[pos],
                    self.buffer[pos + 1],
                    self.buffer[pos + 2],
                ]);
                pos += 3;

                message_length = u32::from_be_bytes([
                    0,
                    self.buffer[pos],
                    self.buffer[pos + 1],
                    self.buffer[pos + 2],
                ]);
                pos += 3;

                message_type_id = self.buffer[pos];
                pos += 1;

                if let Some(state) = &previous_state {
                    message_stream_id = state.message_stream_id;
                }
            }
            2 => {
                timestamp = u32::from_be_bytes([
                    0,
                    self.buffer[pos],
                    self.buffer[pos + 1],
                    self.buffer[pos + 2],
                ]);
                pos += 3;

                if let Some(state) = &previous_state {
                    message_length = state.message_length;
                    message_type_id = state.message_type_id;
                    message_stream_id = state.message_stream_id;
                }
            }
            3 => {
                if let Some(state) = &previous_state {
                    timestamp = state.timestamp;
                    message_length = state.message_length;
                    message_type_id = state.message_type_id;
                    message_stream_id = state.message_stream_id;
                } else {
                    return Err(RtmpError::Protocol(
                        "Received type 3 chunk without previous state".into(),
                    ));
                }
            }
            _ => unreachable!(),
        }

        // Calculate how much data we need to read
        let mut data = Vec::new();
        let bytes_left = if let Some(state) = previous_state {
            state.bytes_left
        } else {
            message_length
        };

        let data_size = std::cmp::min(bytes_left, self.chunk_size);

        if self.buffer.len() < (header_size + 1 + data_size as usize) {
            return Ok(None);
        }

        // Read available data
        if data_size > 0 {
            data.extend_from_slice(&self.buffer[pos..pos + data_size as usize]);
        }

        // Update or create chunk state
        let new_state = ChunkStreamState {
            timestamp,
            message_length,
            message_type_id,
            message_stream_id,
            bytes_left: bytes_left - data_size,
            data: data.clone(),
        };
        self.chunk_states.insert(chunk_stream_id, new_state);

        // Remove processed bytes from buffer
        self.buffer.split_to(pos + data_size as usize);

        tracing::debug!(
            "Read chunk - type: {}, stream_id: {}, msg_type: {}, data size: {}",
            chunk_type,
            chunk_stream_id,
            message_type_id,
            data.len()
        );

        Ok(Some(RtmpChunk {
            chunk_type,
            chunk_stream_id,
            timestamp,
            message_length,
            message_type_id,
            message_stream_id,
            data,
        }))
    }
    async fn process_chunk(&mut self, chunk: RtmpChunk) -> Result<(), RtmpError> {
        tracing::debug!(
            "Processing chunk - type: {}, stream_id: {}, msg_type: 0x{:02X}, msg_len: {}, timestamp: {}, data: {:02X?}",
            chunk.chunk_type,
            chunk.chunk_stream_id,
            chunk.message_type_id,
            chunk.message_length,
            chunk.timestamp,
            chunk.data
        );

        match chunk.message_type_id {
            RTMP_MSG_SET_CHUNK_SIZE => {
                tracing::debug!("Handling SET_CHUNK_SIZE");
                self.handle_set_chunk_size(&chunk).await?
            }
            RTMP_MSG_ACK => {
                tracing::debug!("Handling ACK");
                self.handle_acknowledgement(&chunk).await?
            }
            RTMP_MSG_USER_CONTROL => {
                tracing::debug!("Handling USER_CONTROL");
                self.handle_user_control(&chunk).await?
            }
            RTMP_MSG_WINDOW_ACK_SIZE => {
                tracing::debug!("Handling WINDOW_ACK_SIZE");
                self.handle_window_acknowledgement_size(&chunk).await?
            }
            RTMP_MSG_SET_PEER_BANDWIDTH => {
                tracing::debug!("Handling SET_PEER_BANDWIDTH");
                self.handle_set_peer_bandwidth(&chunk).await?
            }
            RTMP_MSG_AUDIO => {
                tracing::debug!("Handling AUDIO");
                self.handle_audio(&chunk).await?
            }
            RTMP_MSG_VIDEO => {
                tracing::debug!("Handling VIDEO");
                self.handle_video(&chunk).await?
            }
            0x14 | 0x11 | 0x61 => {
                tracing::debug!("Handling AMF command");
                self.handle_amf_command(&chunk).await?
            }
            0x12 | 0x0F => {
                tracing::debug!("Handling AMF data");
                self.handle_amf_data(&chunk).await?
            }
            _ => {
                tracing::warn!(
                    "Unknown message type: 0x{:02X} (chunk type: {}), data: {:02X?}",
                    chunk.message_type_id,
                    chunk.chunk_type,
                    chunk.data
                );
            }
        }
        Ok(())
    }

    async fn handle_set_chunk_size(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        if chunk.data.len() < 4 {
            return Err(RtmpError::Protocol("Invalid set chunk size message".into()));
        }

        let new_chunk_size =
            u32::from_be_bytes([chunk.data[0], chunk.data[1], chunk.data[2], chunk.data[3]]);

        if new_chunk_size > MAX_CHUNK_SIZE {
            return Err(RtmpError::InvalidChunkSize(new_chunk_size));
        }

        tracing::debug!("Setting chunk size to: {}", new_chunk_size);
        self.chunk_size = new_chunk_size;
        Ok(())
    }

    async fn handle_user_control(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        if chunk.data.len() < 2 {
            return Err(RtmpError::Protocol("Invalid user control message".into()));
        }

        let event_type = u16::from_be_bytes([chunk.data[0], chunk.data[1]]);
        tracing::debug!("Received user control message, event type: {}", event_type);

        match event_type {
            0 => {
                // Stream Begin
                if chunk.data.len() < 6 {
                    return Err(RtmpError::Protocol("Invalid stream begin message".into()));
                }
                let stream_id = u32::from_be_bytes([
                    chunk.data[2],
                    chunk.data[3],
                    chunk.data[4],
                    chunk.data[5],
                ]);
                tracing::debug!("Stream begin: {}", stream_id);
            }
            1 => {
                // Stream EOF
                tracing::debug!("Stream EOF");
            }
            2 => {
                // Stream Dry
                tracing::debug!("Stream Dry");
            }
            4 => {
                // Stream Is Recorded
                tracing::debug!("Stream Is Recorded");
            }
            6 => {
                // Client Buffer Time
                if chunk.data.len() < 10 {
                    return Err(RtmpError::Protocol("Invalid buffer time message".into()));
                }
                let stream_id = u32::from_be_bytes([
                    chunk.data[2],
                    chunk.data[3],
                    chunk.data[4],
                    chunk.data[5],
                ]);
                let buffer_time = u32::from_be_bytes([
                    chunk.data[6],
                    chunk.data[7],
                    chunk.data[8],
                    chunk.data[9],
                ]);
                tracing::debug!(
                    "Client buffer time: {}ms for stream {}",
                    buffer_time,
                    stream_id
                );
            }
            _ => {
                tracing::warn!("Unknown user control message event type: {}", event_type);
            }
        }
        Ok(())
    }

    async fn handle_acknowledgement(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        if chunk.data.len() < 4 {
            return Err(RtmpError::Protocol(
                "Invalid acknowledgement message".into(),
            ));
        }

        let sequence_number =
            u32::from_be_bytes([chunk.data[0], chunk.data[1], chunk.data[2], chunk.data[3]]);

        tracing::debug!(
            "Received acknowledgement: sequence number {}",
            sequence_number
        );

        // Send back an acknowledgement
        let mut response = Vec::new();
        response.extend_from_slice(&sequence_number.to_be_bytes());

        let header = create_rtmp_header(0, 2, 0, 4, RTMP_MSG_ACK, 0);

        self.socket.write_all(&header).await?;
        self.socket.write_all(&response).await?;

        Ok(())
    }

    async fn handle_window_acknowledgement_size(
        &mut self,
        chunk: &RtmpChunk,
    ) -> Result<(), RtmpError> {
        if chunk.data.len() < 4 {
            return Err(RtmpError::Protocol(
                "Invalid window ack size message".into(),
            ));
        }

        let window_size =
            u32::from_be_bytes([chunk.data[0], chunk.data[1], chunk.data[2], chunk.data[3]]);

        tracing::debug!("Window acknowledgement size set to: {}", window_size);

        // Send back our window size
        let mut response = Vec::new();
        response.extend_from_slice(&window_size.to_be_bytes());

        let header = create_rtmp_header(0, 2, 0, 4, RTMP_MSG_WINDOW_ACK_SIZE, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(&response).await?;

        Ok(())
    }

    async fn handle_set_peer_bandwidth(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        if chunk.data.len() < 5 {
            return Err(RtmpError::Protocol("Invalid peer bandwidth message".into()));
        }

        let window_size =
            u32::from_be_bytes([chunk.data[0], chunk.data[1], chunk.data[2], chunk.data[3]]);
        let limit_type = chunk.data[4];

        tracing::debug!(
            "Peer bandwidth set to: {} (limit type: {})",
            window_size,
            limit_type
        );

        // Send acknowledgement
        let mut response = Vec::new();
        response.extend_from_slice(&window_size.to_be_bytes());
        response.push(limit_type);

        let header = create_rtmp_header(0, 2, 0, 5, RTMP_MSG_SET_PEER_BANDWIDTH, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(&response).await?;

        Ok(())
    }

    async fn handle_audio(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        // Forward audio data to all subscribers
        self.broadcast_tx.send(chunk.data.clone()).ok();

        tracing::debug!(
            "Received audio packet - size: {} bytes, timestamp: {}",
            chunk.data.len(),
            chunk.timestamp
        );
        Ok(())
    }

    async fn handle_video(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        // Forward video data to all subscribers
        self.broadcast_tx.send(chunk.data.clone()).ok();

        tracing::debug!(
            "Received video packet - size: {} bytes, timestamp: {}",
            chunk.data.len(),
            chunk.timestamp
        );
        Ok(())
    }

    async fn handle_amf_data(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        if chunk.data.is_empty() {
            return Ok(());
        }

        tracing::debug!("Received AMF data - length: {}", chunk.data.len());
        // For now, just log AMF data
        tracing::debug!("AMF data: {:?}", chunk.data);
        Ok(())
    }

    async fn handle_amf_command(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        tracing::debug!("Received AMF command - data length: {}", chunk.data.len());
        if chunk.data.is_empty() {
            return Ok(());
        }

        // Log raw data for debugging
        tracing::debug!("AMF command raw data: {:02X?}", chunk.data);

        // Send connect response
        let response = vec![
            0x02, // String marker
            0x00, 0x07, // String length (7)
            b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
            0x00, // Number marker
            0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (1.0)
            0x03, // Object marker
            0x00, 0x05, // Property name length (5)
            b'f', b'm', b's', b'V', b'r', // "fmsVr"
            0x02, // String marker
            0x00, 0x0D, // String length (13)
            b'F', b'M', b'S', b'/', b'3', b',', b'0', b',', b'0', b',', b'0', b'0',
            b'0', // "FMS/3,0,0,000"
            0x00, 0x00, 0x09, // Object end marker
            0x03, // Object marker
            0x00, 0x0C, // Property name length (12)
            b'c', b'a', b'p', b'a', b'b', b'i', b'l', b'i', b't', b'i', b'e',
            b's', // "capabilities"
            0x00, // Number marker
            0x40, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Number value (31.0)
            0x00, 0x00, 0x09, // Object end marker
        ];

        let header = create_rtmp_header(0, 3, 0, response.len() as u32, RTMP_MSG_AMF0_CMD, 0);

        tracing::debug!("Sending AMF response");
        self.socket.write_all(&header).await?;
        self.socket.write_all(&response).await?;

        // Set initial window acknowledgement size
        let window_ack = vec![0x00, 0x26, 0x25, 0xA0]; // 2500000 bytes
        let header = create_rtmp_header(0, 2, 0, 4, RTMP_MSG_WINDOW_ACK_SIZE, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(&window_ack).await?;

        // Set peer bandwidth
        let peer_bandwidth = vec![0x00, 0x26, 0x25, 0xA0, 0x02]; // 2500000 bytes, dynamic
        let header = create_rtmp_header(0, 2, 0, 5, RTMP_MSG_SET_PEER_BANDWIDTH, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(&peer_bandwidth).await?;

        // Set chunk size
        let chunk_size = vec![0x00, 0x00, 0x10, 0x00]; // 4096 bytes
        let header = create_rtmp_header(0, 2, 0, 4, RTMP_MSG_SET_CHUNK_SIZE, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(&chunk_size).await?;

        Ok(())
    }
}

fn create_rtmp_header(
    chunk_type: u8,
    chunk_stream_id: u8,
    timestamp: u32,
    message_length: u32,
    message_type_id: u8,
    message_stream_id: u32,
) -> Vec<u8> {
    let mut header = Vec::new();

    // Format and chunk stream ID
    header.push((chunk_type << 6) | (chunk_stream_id & 0x3F));

    match chunk_type {
        0 => {
            // Timestamp (3 bytes)
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
            // Message length (3 bytes)
            header.extend_from_slice(&message_length.to_be_bytes()[1..]);
            // Message type ID
            header.push(message_type_id);
            // Message stream ID (4 bytes, little-endian)
            header.extend_from_slice(&message_stream_id.to_le_bytes());
        }
        1 => {
            // Timestamp delta (3 bytes)
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
            // Message length (3 bytes)
            header.extend_from_slice(&message_length.to_be_bytes()[1..]);
            // Message type ID
            header.push(message_type_id);
        }
        2 => {
            // Timestamp delta (3 bytes)
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
        }
        _ => {}
    }

    header
}

async fn run_rtmp_server(
    listener: TcpListener,
    streams: Arc<DashMap<u32, StreamMetadata>>,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let (socket, addr) = listener.accept().await?;
        let streams = streams.clone();

        tokio::spawn(async move {
            tracing::info!("New connection from {}", addr);
            let connection = RtmpConnection::new(socket, streams);
            if let Err(e) = connection.handle_connection().await {
                tracing::error!("Connection error: {}", e);
            }
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let streams = Arc::new(DashMap::new());

    // RTMP server setup
    let rtmp_addr: SocketAddr = "127.0.0.1:1935".parse()?;
    let rtmp_listener = TcpListener::bind(rtmp_addr).await?;
    tracing::info!("RTMP server listening on {}", rtmp_addr);

    // HTTP metrics server setup
    let app = Router::new()
        .route("/", get(|| async { "RTMP Server" }))
        .route("/metrics", get(|| async { "Metrics coming soon" }));

    let http_addr: SocketAddr = "127.0.0.1:3000".parse()?;
    let http_listener = TcpListener::bind(http_addr).await?;
    tracing::info!("HTTP metrics server listening on {}", http_addr);

    tokio::select! {
        result = run_rtmp_server(rtmp_listener, streams.clone()) => {
            if let Err(e) = result {
                tracing::error!("RTMP server error: {}", e);
            }
        }
        result = axum::serve(http_listener, app) => {
            if let Err(e) = result {
                tracing::error!("HTTP server error: {}", e);
            }
        }
    }

    Ok(())
}
