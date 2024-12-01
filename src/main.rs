use axum::{routing::get, Router};
use bytes::{Buf, BytesMut};
use dashmap::DashMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing;

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
const RTMP_MSG_AMF0_METADATA: u8 = 0x12;
const RTMP_MSG_AMF3_METADATA: u8 = 0x0F;

// User control message event types
const USER_CONTROL_STREAM_BEGIN: u16 = 0;
const USER_CONTROL_STREAM_EOF: u16 = 1;
const USER_CONTROL_STREAM_DRY: u16 = 2;
const USER_CONTROL_SET_BUFFER_LENGTH: u16 = 3;
const USER_CONTROL_STREAM_IS_RECORDED: u16 = 4;
const USER_CONTROL_PING_REQUEST: u16 = 6;
const USER_CONTROL_PING_RESPONSE: u16 = 7;

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
    #[error("Timeout error: {0}")]
    Timeout(String),
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

#[derive(Debug, Clone, Default)]
struct ChunkStreamState {
    timestamp: u32,
    message_length: u32,
    message_type_id: u8,
    message_stream_id: u32,
    bytes_left: u32,
    data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
enum StreamState {
    Initial,
    Connected,
    Ready,
    Publishing,
    Error(String),
}

#[derive(Debug)]
struct ConnectionState {
    transaction_id: f64,
    stream_id: u32,
    is_connected: bool,
    app_name: Option<String>,
    stream_key: Option<String>,
    stream_state: StreamState, // Add this field
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            transaction_id: 0.0,
            stream_id: 0,
            is_connected: false,
            app_name: None,
            stream_key: None,
            stream_state: StreamState::Initial,
        }
    }
}

struct RtmpConnection {
    socket: tokio::net::TcpStream,
    buffer: BytesMut,
    chunk_size: u32,
    streams: Arc<DashMap<u32, StreamMetadata>>,
    broadcast_tx: broadcast::Sender<Vec<u8>>,
    chunk_states: HashMap<u32, ChunkStreamState>,
    state: ConnectionState,
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
            state: ConnectionState::default(),
        }
    }

    fn parse_stream_name(&self, data: &[u8]) -> Option<String> {
        // Skip command name
        let mut pos = 3 + u16::from_be_bytes([data[1], data[2]]) as usize;

        // Skip transaction ID
        if data.len() < pos + 9 {
            return None;
        }
        pos += 9;

        // Skip NULL marker if present
        if data[pos] == 0x05 {
            pos += 1;
        }

        // Get stream name
        if data.len() < pos + 3 {
            return None;
        }

        if data[pos] != 0x02 {
            // String marker
            return None;
        }
        pos += 1;

        let name_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2;

        if pos + name_len > data.len() {
            return None;
        }

        String::from_utf8_lossy(&data[pos..pos + name_len])
            .into_owned()
            .into()
    }

    fn parse_app_name_from_connect(&self, data: &[u8]) -> Option<String> {
        let mut pos = 3 + u16::from_be_bytes([data[1], data[2]]) as usize;

        // Skip transaction ID (number marker + 8 bytes)
        if data.len() < pos + 9 {
            return None;
        }
        pos += 9;

        // Must have object marker (0x03)
        if data.len() <= pos || data[pos] != 0x03 {
            return None;
        }
        pos += 1;

        // Parse command object properties
        while pos + 2 < data.len() {
            // Check for object end marker
            if data[pos] == 0x00 && data[pos + 1] == 0x00 && data[pos + 2] == 0x09 {
                break;
            }

            // Get property name length
            let name_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;

            if pos + name_len >= data.len() {
                return None;
            }

            // Get property name
            let prop_name = std::str::from_utf8(&data[pos..pos + name_len]).ok()?;
            pos += name_len;

            if prop_name == "app" {
                // Must have string marker (0x02)
                if pos >= data.len() || data[pos] != 0x02 {
                    return None;
                }
                pos += 1;

                // Get string length
                if pos + 2 > data.len() {
                    return None;
                }
                let str_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                pos += 2;

                if pos + str_len > data.len() {
                    return None;
                }

                return Some(String::from_utf8_lossy(&data[pos..pos + str_len]).into_owned());
            }

            // Skip value based on type marker
            if pos >= data.len() {
                return None;
            }

            match data[pos] {
                0x02 => {
                    // String
                    pos += 1;
                    if pos + 2 > data.len() {
                        return None;
                    }
                    let skip_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2 + skip_len;
                }
                0x00 => {
                    // Number
                    pos += 9; // Skip type marker + 8 bytes
                }
                0x05 => {
                    // Null
                    pos += 1;
                }
                _ => return None, // Unknown type marker
            }
        }

        None
    }

    fn log_connection_state(&self) {
        tracing::debug!("Connection state:");
        tracing::debug!("  Chunk size: {}", self.chunk_size);
        tracing::debug!("  Is connected: {}", self.state.is_connected);
        tracing::debug!("  Stream ID: {}", self.state.stream_id);
        tracing::debug!("  App name: {:?}", self.state.app_name);
        tracing::debug!("  Stream key: {:?}", self.state.stream_key);
        tracing::debug!("  Active chunks: {}", self.chunk_states.len());
        tracing::debug!("  Buffer size: {}", self.buffer.len());
        tracing::debug!("  Transaction ID: {}", self.state.transaction_id);
    }

    async fn handle_connection(mut self) -> Result<(), RtmpError> {
        self.perform_handshake().await?;

        loop {
            match self.read_chunk().await? {
                Some(chunk) => {
                    self.process_chunk(chunk).await?;
                    self.log_connection_state();
                }
                None => {
                    tracing::info!("Connection closed");
                    break;
                }
            }
        }
        Ok(())
    }

    async fn perform_handshake(&mut self) -> Result<(), RtmpError> {
        // Read C0+C1 (1+1536 bytes)
        let mut c0c1 = [0u8; 1537];
        self.socket.read_exact(&mut c0c1).await?;

        if c0c1[0] != RTMP_VERSION {
            return Err(RtmpError::Protocol(format!(
                "Unsupported RTMP version: {}",
                c0c1[0]
            )));
        }

        tracing::debug!("Received C0+C1");

        // Prepare S0+S1+S2 response
        let mut response = Vec::with_capacity(3073);
        response.push(RTMP_VERSION);

        // S1 - Timestamp + Zero + Random
        let time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as u32;
        response.extend_from_slice(&time.to_be_bytes());
        response.extend_from_slice(&[0u8; 4]);

        // Random bytes for S1
        let random_bytes: Vec<u8> = (0..1528).map(|_| rand::random::<u8>()).collect();
        response.extend_from_slice(&random_bytes);

        // S2 - Echo client's C1
        response.extend_from_slice(&c0c1[1..1537]);

        self.socket.write_all(&response).await?;
        tracing::debug!("Sent S0+S1+S2");

        let mut c2 = [0u8; 1536];
        self.socket.read_exact(&mut c2).await?;
        tracing::debug!("Received C2");
        tracing::info!("Handshake completed successfully");

        Ok(())
    }

    async fn read_chunk(&mut self) -> Result<Option<RtmpChunk>, RtmpError> {
        if self.buffer.len() < 1 {
            let read_future = self.socket.read_buf(&mut self.buffer);
            match tokio::time::timeout(Duration::from_secs(5), read_future).await {
                Ok(Ok(0)) => {
                    tracing::debug!("Connection closed by peer");
                    return Ok(None);
                }
                Ok(Ok(n)) => {
                    tracing::debug!("Read {} bytes into buffer", n);
                    if n > 0 {
                        // Add hexdump of received data
                        tracing::debug!("Raw bytes received: {:02X?}", &self.buffer[..n]);
                    }
                }
                Ok(Err(e)) => return Err(RtmpError::Io(e)),
                Err(_) => return Err(RtmpError::Timeout("Read timeout".into())),
            }
        }

        let first_byte = self.buffer[0];
        let fmt = first_byte >> 6;
        let csid = first_byte & 0x3F;

        let (mut pos, chunk_stream_id) = match csid {
            0 => {
                if self.buffer.len() < 2 {
                    return Ok(None);
                }
                (2, self.buffer[1] as u32 + 64)
            }
            1 => {
                if self.buffer.len() < 3 {
                    return Ok(None);
                }
                (
                    3,
                    (self.buffer[2] as u32) * 256 + (self.buffer[1] as u32) + 64,
                )
            }
            _ => (1, csid as u32),
        };

        let previous_chunk = self.chunk_states.get(&chunk_stream_id).cloned();

        let header_size = match fmt {
            0 => 11,
            1 => 7,
            2 => 3,
            3 => 0,
            _ => return Err(RtmpError::Protocol("Invalid chunk type".into())),
        };

        if self.buffer.len() < pos + header_size {
            return Ok(None);
        }

        let mut timestamp = 0u32;
        let mut message_length = 0u32;
        let mut message_type_id = 0u8;
        let mut message_stream_id = 0u32;

        match fmt {
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
                if let Some(prev) = &previous_chunk {
                    message_stream_id = prev.message_stream_id;
                }

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
            }
            2 => {
                if let Some(prev) = &previous_chunk {
                    message_length = prev.message_length;
                    message_type_id = prev.message_type_id;
                    message_stream_id = prev.message_stream_id;
                }

                timestamp = u32::from_be_bytes([
                    0,
                    self.buffer[pos],
                    self.buffer[pos + 1],
                    self.buffer[pos + 2],
                ]);
                pos += 3;
            }
            3 => {
                if let Some(prev) = &previous_chunk {
                    timestamp = prev.timestamp;
                    message_length = prev.message_length;
                    message_type_id = prev.message_type_id;
                    message_stream_id = prev.message_stream_id;
                } else {
                    return Err(RtmpError::Protocol(
                        "Type 3 chunk without previous state".into(),
                    ));
                }
            }
            _ => unreachable!(),
        }

        if timestamp == 0xFFFFFF {
            if self.buffer.len() < pos + 4 {
                return Ok(None);
            }
            timestamp = u32::from_be_bytes([
                self.buffer[pos],
                self.buffer[pos + 1],
                self.buffer[pos + 2],
                self.buffer[pos + 3],
            ]);
            pos += 4;
        }

        let chunk_data_size = std::cmp::min(message_length, self.chunk_size);
        if self.buffer.len() < pos + chunk_data_size as usize {
            return Ok(None);
        }

        let data = self.buffer[pos..pos + chunk_data_size as usize].to_vec();
        self.buffer.advance(pos + chunk_data_size as usize);

        let chunk = RtmpChunk {
            chunk_type: fmt,
            chunk_stream_id,
            timestamp,
            message_length,
            message_type_id,
            message_stream_id,
            data,
        };

        let state = ChunkStreamState {
            timestamp: chunk.timestamp,
            message_length: chunk.message_length,
            message_type_id: chunk.message_type_id,
            message_stream_id: chunk.message_stream_id,
            bytes_left: chunk.message_length - chunk.data.len() as u32,
            data: chunk.data.clone(),
        };
        self.chunk_states.insert(chunk_stream_id, state);

        Ok(Some(chunk))
    }

    async fn process_chunk(&mut self, chunk: RtmpChunk) -> Result<(), RtmpError> {
        tracing::debug!(
        "Processing chunk - type: {}, stream_id: {}, msg_type: 0x{:02X}, msg_len: {}, timestamp: {}",
        chunk.chunk_type,
        chunk.chunk_stream_id,
        chunk.message_type_id,
        chunk.message_length,
        chunk.timestamp
    );

        match chunk.message_type_id {
            RTMP_MSG_AMF0_METADATA | RTMP_MSG_AMF3_METADATA => {
                tracing::debug!("Received metadata packet: {:02X?}", chunk.data);
                // Just acknowledge - no response needed
            }
            RTMP_MSG_SET_CHUNK_SIZE => {
                if chunk.data.len() < 4 {
                    return Err(RtmpError::Protocol("Invalid set chunk size message".into()));
                }
                let new_chunk_size = u32::from_be_bytes([
                    chunk.data[0],
                    chunk.data[1],
                    chunk.data[2],
                    chunk.data[3],
                ]);
                if new_chunk_size > MAX_CHUNK_SIZE {
                    return Err(RtmpError::InvalidChunkSize(new_chunk_size));
                }
                tracing::debug!("Setting chunk size to: {}", new_chunk_size);
                self.chunk_size = new_chunk_size;
            }

            RTMP_MSG_WINDOW_ACK_SIZE => {
                if chunk.data.len() < 4 {
                    return Err(RtmpError::Protocol(
                        "Invalid window ack size message".into(),
                    ));
                }
                let window_size = u32::from_be_bytes([
                    chunk.data[0],
                    chunk.data[1],
                    chunk.data[2],
                    chunk.data[3],
                ]);
                tracing::debug!("Window acknowledgement size set to: {}", window_size);

                // Send back window ack size
                let response = window_size.to_be_bytes().to_vec();
                let header = create_rtmp_header(0, 2, 0, 4, RTMP_MSG_WINDOW_ACK_SIZE, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&response).await?;
            }

            RTMP_MSG_USER_CONTROL => {
                if chunk.data.len() < 2 {
                    return Err(RtmpError::Protocol("Invalid user control message".into()));
                }
                let event_type = u16::from_be_bytes([chunk.data[0], chunk.data[1]]);
                match event_type {
                    USER_CONTROL_PING_REQUEST => {
                        // Send ping response
                        let mut response = vec![
                            0x00,
                            USER_CONTROL_PING_RESPONSE as u8, // Event type
                        ];
                        response.extend_from_slice(&chunk.data[2..]); // Echo timestamp
                        let header = create_rtmp_header(
                            0,
                            2,
                            0,
                            response.len() as u32,
                            RTMP_MSG_USER_CONTROL,
                            0,
                        );
                        self.socket.write_all(&header).await?;
                        self.socket.write_all(&response).await?;
                    }
                    _ => {
                        tracing::debug!("Unhandled user control message: {}", event_type);
                    }
                }
            }

            RTMP_MSG_AMF0_CMD | RTMP_MSG_AMF3_CMD => {
                self.handle_amf_command(&chunk).await?;
            }

            RTMP_MSG_AMF0_DATA | RTMP_MSG_AMF3_DATA => {
                tracing::debug!("Received metadata: {:02X?}", chunk.data);
                // Handle metadata packets
                self.handle_amf_command(&chunk).await?;
            }

            RTMP_MSG_VIDEO => {
                if self.state.stream_state == StreamState::Publishing {
                    if chunk.data.len() > 0 {
                        let frame_type = chunk.data[0] >> 4;
                        let codec_id = chunk.data[0] & 0x0F;
                        tracing::debug!(
                            "Received video data: {} bytes, frame_type: {}, codec_id: {}",
                            chunk.data.len(),
                            frame_type,
                            codec_id
                        );
                        self.broadcast_tx.send(chunk.data).ok();
                    }
                }
            }

            RTMP_MSG_AUDIO => {
                if self.state.stream_state == StreamState::Publishing {
                    if chunk.data.len() > 0 {
                        let format = (chunk.data[0] >> 4) & 0x0F;
                        let rate = (chunk.data[0] >> 2) & 0x03;
                        let size = (chunk.data[0] >> 1) & 0x01;
                        let type_ = chunk.data[0] & 0x01;
                        tracing::debug!(
                        "Received audio data: {} bytes, format: {}, rate: {}, size: {}, type: {}",
                        chunk.data.len(),
                        format,
                        rate,
                        size,
                        type_
                    );
                        self.broadcast_tx.send(chunk.data).ok();
                    }
                }
            }

            RTMP_MSG_AMF0_CMD | RTMP_MSG_AMF3_CMD => {
                self.handle_amf_command(&chunk).await?;
            }

            _ => {
                tracing::debug!("Unhandled message type: 0x{:02X}", chunk.message_type_id);
            }
        }
        Ok(())
    }
    fn parse_connect_params(&self, data: &[u8]) -> Option<HashMap<String, String>> {
        let mut params = HashMap::new();
        let mut pos = 3 + u16::from_be_bytes([data[1], data[2]]) as usize;

        // Skip transaction ID (number marker + 8 bytes)
        if data.len() < pos + 9 {
            return None;
        }
        pos += 9;

        // Must have object marker
        if data.len() <= pos || data[pos] != 0x03 {
            return None;
        }
        pos += 1;

        // Add better error handling and logging here
        while pos + 2 < data.len() {
            if data[pos] == 0x00 && data[pos + 1] == 0x00 && data[pos + 2] == 0x09 {
                break;
            }

            // Get property name length with bounds checking
            if pos + 2 > data.len() {
                tracing::warn!(
                    "Malformed connect parameters: insufficient data for property name length"
                );
                break;
            }

            let name_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
            pos += 2;

            if pos + name_len > data.len() {
                tracing::warn!("Malformed connect parameters: insufficient data for property name");
                break;
            }

            // Get property name
            let prop_name = match std::str::from_utf8(&data[pos..pos + name_len]) {
                Ok(s) => s.to_string(),
                Err(_) => {
                    tracing::warn!("Invalid UTF-8 in property name");
                    break;
                }
            };
            pos += name_len;

            // Add debugging for property parsing
            tracing::debug!("Parsing property: {}", prop_name);

            if pos >= data.len() {
                break;
            }

            let value = match data[pos] {
                0x02 => {
                    // String
                    pos += 1;
                    if pos + 2 > data.len() {
                        break;
                    }
                    let str_len = u16::from_be_bytes([data[pos], data[pos + 1]]) as usize;
                    pos += 2;
                    if pos + str_len > data.len() {
                        break;
                    }
                    let value = std::str::from_utf8(&data[pos..pos + str_len])
                        .ok()?
                        .to_string();
                    pos += str_len;
                    value
                }
                0x00 => {
                    // Number
                    pos += 1;
                    if pos + 8 > data.len() {
                        break;
                    }
                    let value = format!(
                        "{}",
                        f64::from_be_bytes([
                            data[pos],
                            data[pos + 1],
                            data[pos + 2],
                            data[pos + 3],
                            data[pos + 4],
                            data[pos + 5],
                            data[pos + 6],
                            data[pos + 7]
                        ])
                    );
                    pos += 8;
                    value
                }
                marker => {
                    tracing::warn!("Unknown property value marker: 0x{:02X}", marker);
                    break;
                }
            };

            tracing::debug!("Property value: {}", value);
            params.insert(prop_name, value);
        }

        Some(params)
    }

    async fn handle_amf_command(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        let data = if chunk.message_type_id == RTMP_MSG_AMF3_CMD {
            &chunk.data[1..]
        } else {
            &chunk.data[..]
        };

        if data.len() < 3 {
            return Err(RtmpError::Protocol("Invalid AMF command message".into()));
        }

        let command_name_length = u16::from_be_bytes([data[1], data[2]]) as usize;
        if data.len() < 3 + command_name_length {
            return Err(RtmpError::Protocol("Invalid AMF command length".into()));
        }

        let command_name = String::from_utf8_lossy(&data[3..3 + command_name_length]);
        tracing::debug!(
            "Processing AMF command: {} (hex: {:02X?})",
            command_name,
            data
        );

        match command_name.as_ref() {
            "connect" => {
                tracing::debug!("Connect command data: {:02X?}", data);
                if let Some(params) = self.parse_connect_params(data) {
                    tracing::debug!("Connect parameters: {:?}", params);
                    self.state.app_name = params.get("app").cloned();
                    tracing::debug!("Using app name: {:?}", self.state.app_name);
                }

                self.state.is_connected = true;
                // Parse app name from connect command
                if let Some(app_name) = self.parse_app_name_from_connect(data) {
                    self.state.app_name = Some(app_name);
                    tracing::debug!(
                        "Connected to application: {}",
                        self.state.app_name.as_ref().unwrap()
                    );
                }

                self.state.is_connected = true;

                // Send Window Acknowledgement Size
                let window_ack = 2500000u32.to_be_bytes().to_vec();
                let header = create_rtmp_header(0, 2, 0, 4, RTMP_MSG_WINDOW_ACK_SIZE, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&window_ack).await?;

                // Set Peer Bandwidth
                let mut peer_bandwidth = 2500000u32.to_be_bytes().to_vec();
                peer_bandwidth.push(2); // Dynamic type
                let header = create_rtmp_header(0, 2, 0, 5, RTMP_MSG_SET_PEER_BANDWIDTH, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&peer_bandwidth).await?;

                // Set Chunk Size
                let chunk_size = 4096u32.to_be_bytes().to_vec();
                let header = create_rtmp_header(0, 2, 0, 4, RTMP_MSG_SET_CHUNK_SIZE, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&chunk_size).await?;

                // Send connect result
                let result = create_amf0_connect_response();
                let header = create_rtmp_header(0, 3, 0, result.len() as u32, RTMP_MSG_AMF0_CMD, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&result).await?;

                // Send onBWDone
                let bw_done = create_amf0_on_bw_done();
                let header =
                    create_rtmp_header(0, 3, 0, bw_done.len() as u32, RTMP_MSG_AMF0_CMD, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&bw_done).await?;

                self.state.transaction_id = 1.0;
                tracing::debug!("Connect sequence completed");
            }
            "_checkbw" => {
                tracing::debug!("Handling _checkbw command");
                let response = create_amf0_checkbw_response();
                let header =
                    create_rtmp_header(0, 3, 0, response.len() as u32, RTMP_MSG_AMF0_CMD, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&response).await?;
                self.state.transaction_id += 1.0;
            }

            "createStream" => {
                self.state.stream_id += 1;
                let response = create_amf0_create_stream_response(self.state.stream_id);
                let header =
                    create_rtmp_header(0, 3, 0, response.len() as u32, RTMP_MSG_AMF0_CMD, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&response).await?;

                // Send Stream Begin event
                let mut stream_begin = vec![0x00, USER_CONTROL_STREAM_BEGIN as u8];
                stream_begin.extend_from_slice(&self.state.stream_id.to_be_bytes());
                let header = create_rtmp_header(0, 2, 0, 6, RTMP_MSG_USER_CONTROL, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&stream_begin).await?;

                tracing::debug!("Created stream with ID: {}", self.state.stream_id);
                self.state.transaction_id += 1.0;
            }

            "@setDataFrame" => {
                tracing::debug!("Handling setDataFrame command");
                // Just acknowledge receipt - no response needed
            }

            "onMetaData" => {
                tracing::debug!("Handling onMetaData command");
                // Store metadata if needed
            }

            "publish" => {
                tracing::debug!("Handling publish command with data: {:02X?}", data);

                // Extract stream key
                if let Some(stream_name) = self.parse_stream_name(data) {
                    tracing::debug!("Publishing to stream: {}", stream_name);
                    self.state.stream_key = Some(stream_name.clone());

                    // 1. First Stream Begin event
                    let mut stream_begin = vec![
                        0x00,
                        USER_CONTROL_STREAM_BEGIN as u8,
                        0x00,
                        0x00,
                        0x00,
                        0x01, // Stream ID = 1
                    ];
                    let header = create_rtmp_header(0, 2, 0, 6, RTMP_MSG_USER_CONTROL, 0);
                    self.socket.write_all(&header).await?;
                    self.socket.write_all(&stream_begin).await?;

                    // 2. Second Stream Begin event
                    self.socket.write_all(&header).await?;
                    self.socket.write_all(&stream_begin).await?;

                    // 3. Stream Is Recorded event
                    let mut stream_recorded = vec![
                        0x00,
                        USER_CONTROL_STREAM_IS_RECORDED as u8,
                        0x00,
                        0x00,
                        0x00,
                        0x01, // Stream ID = 1
                    ];
                    let header = create_rtmp_header(0, 2, 0, 6, RTMP_MSG_USER_CONTROL, 0);
                    self.socket.write_all(&header).await?;
                    self.socket.write_all(&stream_recorded).await?;

                    // 4. Finally send the publish status
                    let response = create_amf0_publish_response();
                    let header =
                        create_rtmp_header(0, 5, 0, response.len() as u32, RTMP_MSG_AMF0_CMD, 1);
                    self.socket.write_all(&header).await?;
                    self.socket.write_all(&response).await?;

                    tracing::debug!("Publish setup completed");
                    self.state.stream_state = StreamState::Publishing;
                }
                self.state.transaction_id += 1.0;
            }

            "releaseStream" => {
                tracing::debug!("Handling releaseStream command with data: {:02X?}", data);
                if let Some(stream_name) = self.parse_stream_name(data) {
                    tracing::debug!("Release stream name: {}", stream_name);
                    self.state.stream_key = Some(stream_name);
                }

                let response = create_amf0_release_stream_response();
                let header =
                    create_rtmp_header(0, 3, 0, response.len() as u32, RTMP_MSG_AMF0_CMD, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&response).await?;

                tracing::debug!("Sent releaseStream response");
            }

            "FCPublish" => {
                tracing::debug!("Handling FCPublish command");
                let response = create_amf0_fcpublish_response();
                let header =
                    create_rtmp_header(0, 3, 0, response.len() as u32, RTMP_MSG_AMF0_CMD, 0);
                self.socket.write_all(&header).await?;
                self.socket.write_all(&response).await?;
            }

            // Catch-all pattern for unhandled commands
            cmd => {
                tracing::debug!("Unhandled command: {}", cmd);
            }
        }

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
    if chunk_stream_id < 64 {
        header.push((chunk_type << 6) | chunk_stream_id);
    } else {
        header.push(chunk_type << 6);
        header.push(chunk_stream_id - 64);
    }

    match chunk_type {
        0 => {
            // Timestamp
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
            // Message length
            header.extend_from_slice(&message_length.to_be_bytes()[1..]);
            // Message type ID
            header.push(message_type_id);
            // Message stream ID (little-endian)
            header.extend_from_slice(&message_stream_id.to_le_bytes());
        }
        1 => {
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
            header.extend_from_slice(&message_length.to_be_bytes()[1..]);
            header.push(message_type_id);
        }
        2 => {
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
        }
        _ => {}
    }

    header
}

fn create_amf0_release_stream_response() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x40, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (3.0)
        0x05, // NULL marker
        0x05, // NULL marker for result
    ]
}

fn create_amf0_fcpublish_response() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x40, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (4.0)
        0x05, // NULL marker
        0x05, // NULL marker for result
    ]
}

fn create_amf0_create_stream_response(stream_id: u32) -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x40, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (5.0)
        0x05, // NULL marker
        0x00, // Number marker
        // Convert stream_id to f64 and encode as 8 bytes
        0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Stream ID as double (1.0)
    ]
}

fn create_amf0_connect_response() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (1.0)
        0x03, // Object marker
        0x00, 0x05, // Property name length (5)
        b'l', b'e', b'v', b'e', b'l', // "level"
        0x02, // String marker
        0x00, 0x06, // String length (6)
        b's', b't', b'a', b't', b'u', b's', // "status"
        0x00, 0x04, // Property name length (4)
        b'c', b'o', b'd', b'e', // "code"
        0x02, // String marker
        0x00, 0x17, // String length (23)
        b'N', b'e', b't', b'C', b'o', b'n', b'n', b'e', b'c', b't', b'i', b'o', b'n', b'.', b'C',
        b'o', b'n', b'n', b'e', b'c', b't', b'.', b'O', b'K', 0x00, 0x00,
        0x09, // Object end marker
        0x03, // Object marker (properties)
        0x00, 0x0C, // Property name length (12)
        b'c', b'a', b'p', b'a', b'b', b'i', b'l', b'i', b't', b'i', b'e', b's',
        0x00, // Number marker
        0x40, 0x3F, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 31.0
        0x00, 0x00, 0x09, // Object end marker
    ]
}

fn create_amf0_on_bw_done() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x08, // String length (8)
        b'o', b'n', b'B', b'W', b'D', b'o', b'n', b'e', 0x00, // Number marker
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (0.0)
        0x05, // NULL marker
    ]
}

fn create_amf0_publish_response() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x08, // String length (8)
        b'o', b'n', b'S', b't', b'a', b't', b'u', b's', // "onStatus"
        0x00, // Number marker
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (0)
        0x05, // NULL marker
        0x03, // Object marker
        // level
        0x00, 0x05, // Property name length (5)
        b'l', b'e', b'v', b'e', b'l', 0x02, // String marker
        0x00, 0x06, // String length (6)
        b's', b't', b'a', b't', b'u', b's', // code
        0x00, 0x04, // Property name length (4)
        b'c', b'o', b'd', b'e', 0x02, // String marker
        0x00, 0x17, // String length (23)
        b'N', b'e', b't', b'S', b't', b'r', b'e', b'a', b'm', b'.', b'P', b'u', b'b', b'l', b'i',
        b's', b'h', b'.', b'S', b't', b'a', b'r', b't', // description
        0x00, 0x0B, // Property name length (11)
        b'd', b'e', b's', b'c', b'r', b'i', b'p', b't', b'i', b'o', b'n',
        0x02, // String marker
        0x00, 0x14, // String length (20)
        b'S', b't', b'a', b'r', b't', b' ', b'p', b'u', b'b', b'l', b'i', b's', b'h', b'i', b'n',
        b'g', b' ', b'l', b'i', b'v', b'e', 0x00, 0x00, 0x09, // Object end marker
    ]
}

fn create_amf0_checkbw_response() -> Vec<u8> {
    vec![
        0x02, // String marker
        0x00, 0x07, // String length (7)
        b'_', b'r', b'e', b's', b'u', b'l', b't', // "_result"
        0x00, // Number marker
        0x3F, 0xF0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Transaction ID (1.0)
        0x05, // NULL marker
        0x02, // String marker
        0x00, 0x0A, // String length (10)
        b'_', b'c', b'h', b'e', b'c', b'k', b'b', b'w', b'_', b'r', // "_checkbw_r"
    ]
}

// Main function implementation
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
                tracing::error!("Connection error for {}: {}", addr, e);
            }
            tracing::info!("Connection closed for {}", addr);
        });
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let streams = Arc::new(DashMap::new());

    // RTMP server setup (default RTMP port is 1935)
    let rtmp_addr: SocketAddr = "127.0.0.1:1935".parse()?;
    let rtmp_listener = TcpListener::bind(rtmp_addr).await?;
    tracing::info!("RTMP server listening on {}", rtmp_addr);

    // HTTP metrics server setup
    let app = Router::new()
        .route("/", get(|| async { "RTMP Server" }))
        .route(
            "/metrics",
            get(|| async {
                let metrics = "Metrics endpoint - Coming soon\n";
                metrics
            }),
        );

    let http_addr: SocketAddr = "127.0.0.1:3000".parse()?;
    let http_listener = TcpListener::bind(http_addr).await?;
    tracing::info!("HTTP metrics server listening on {}", http_addr);

    // Run both servers concurrently
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
