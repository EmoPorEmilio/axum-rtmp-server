use crate::error::RtmpError;
use crate::protocol::{
    perform_client_handshake, ChunkReader, ChunkWriter, MessageAssembler,
    RTMP_MSG_AMF0_CMD, RTMP_MSG_USER_CONTROL, RTMP_MSG_SET_CHUNK_SIZE,
    RTMP_MSG_WINDOW_ACK_SIZE, USER_CONTROL_STREAM_BEGIN,
};
use crate::amf::{AmfValue, Amf0Codec, create_rtmp_header};
use crate::metrics::MetricsCollector;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use std::sync::Arc;

/// Timeout for waiting for server responses
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, PartialEq)]
pub enum ClientState {
    Disconnected,
    Connecting,
    Handshaking,
    Publishing,
    Failed(String),
    MaxRetriesExceeded,
}

impl ClientState {
    pub fn is_failed(&self) -> bool {
        matches!(self, ClientState::Failed(_) | ClientState::MaxRetriesExceeded)
    }
}

pub struct RtmpClient {
    pub name: String,
    pub url: String,
    pub stream_key: String,
    pub socket: Option<TcpStream>,
    pub state: ClientState,
    pub retry_count: u32,
    pub metrics: Arc<MetricsCollector>,
    /// ChunkWriter for proper chunk splitting on outbound messages
    chunk_writer: ChunkWriter,
    /// ChunkReader for parsing incoming messages
    chunk_reader: ChunkReader,
    /// MessageAssembler for assembling multi-chunk messages
    message_assembler: MessageAssembler,
    /// AMF codec for encoding/decoding
    codec: Amf0Codec,
    /// Negotiated outbound chunk size
    chunk_size_out: u32,
}

impl RtmpClient {
    /// Default chunk size for outbound messages (4KB is common)
    const DEFAULT_CHUNK_SIZE: u32 = 4096;

    pub fn new(
        name: String,
        url: String,
        stream_key: String,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        let mut chunk_writer = ChunkWriter::new();
        chunk_writer.set_chunk_size(Self::DEFAULT_CHUNK_SIZE);

        Self {
            name,
            url,
            stream_key,
            socket: None,
            state: ClientState::Disconnected,
            retry_count: 0,
            metrics,
            chunk_writer,
            chunk_reader: ChunkReader::new(),
            message_assembler: MessageAssembler::new(),
            codec: Amf0Codec::new(),
            chunk_size_out: Self::DEFAULT_CHUNK_SIZE,
        }
    }

    pub async fn connect(&mut self) -> Result<(), RtmpError> {
        self.state = ClientState::Connecting;

        // Reset state for new connection
        self.chunk_writer.clear_state();
        self.chunk_writer.set_chunk_size(self.chunk_size_out);
        self.chunk_reader = ChunkReader::new();
        self.message_assembler = MessageAssembler::new();

        let parsed_url = parse_rtmp_url(&self.url)?;
        tracing::debug!("Connecting to {} ({})", self.name, parsed_url.addr);

        let socket = TcpStream::connect(&parsed_url.addr).await?;
        self.socket = Some(socket);

        self.state = ClientState::Handshaking;
        perform_client_handshake(self.socket.as_mut().unwrap()).await?;
        tracing::debug!("Handshake complete for {}", self.name);

        // Send Window ACK Size (protocol control message)
        self.send_window_ack_size(2_500_000).await?;

        // Send Set Chunk Size (tell server our outbound chunk size)
        self.send_set_chunk_size(self.chunk_size_out).await?;

        let app_name = parsed_url.app.as_deref().unwrap_or("live");

        // Send connect command and wait for response
        let mut connect_cmd = Vec::new();
        connect_cmd.extend_from_slice(&self.codec.encode(&AmfValue::String("connect".into())));
        connect_cmd.extend_from_slice(&self.codec.encode(&AmfValue::Number(1.0)));
        let mut connect_obj = vec![0x03]; // AMF0 Object marker
        connect_obj.extend_from_slice(&encode_object_property("app", app_name));
        connect_obj.extend_from_slice(&encode_object_property("type", "nonprivate"));
        connect_obj.extend_from_slice(&encode_object_property("flashVer", "FMLE/3.0"));
        connect_obj.extend_from_slice(&encode_object_property("tcUrl", &self.url));
        connect_obj.extend_from_slice(&[0x00, 0x00, 0x09]); // Object end marker
        connect_cmd.extend_from_slice(&connect_obj);
        self.send_chunked_message(3, 0, RTMP_MSG_AMF0_CMD, 0, &connect_cmd).await?;

        // Wait for connect response (_result or _error)
        self.wait_for_response(1.0, "connect").await?;
        tracing::debug!("Connect response received for {}", self.name);

        // Send releaseStream (Twitch/platform compatibility)
        let mut release_stream_cmd = Vec::new();
        release_stream_cmd.extend_from_slice(&self.codec.encode(&AmfValue::String("releaseStream".into())));
        release_stream_cmd.extend_from_slice(&self.codec.encode(&AmfValue::Number(2.0)));
        release_stream_cmd.extend_from_slice(&self.codec.encode(&AmfValue::Null));
        release_stream_cmd.extend_from_slice(&self.codec.encode(&AmfValue::String(self.stream_key.clone())));
        self.send_chunked_message(3, 0, RTMP_MSG_AMF0_CMD, 0, &release_stream_cmd).await?;

        // Send FCPublish (Twitch/platform compatibility)
        let mut fcpublish_cmd = Vec::new();
        fcpublish_cmd.extend_from_slice(&self.codec.encode(&AmfValue::String("FCPublish".into())));
        fcpublish_cmd.extend_from_slice(&self.codec.encode(&AmfValue::Number(3.0)));
        fcpublish_cmd.extend_from_slice(&self.codec.encode(&AmfValue::Null));
        fcpublish_cmd.extend_from_slice(&self.codec.encode(&AmfValue::String(self.stream_key.clone())));
        self.send_chunked_message(3, 0, RTMP_MSG_AMF0_CMD, 0, &fcpublish_cmd).await?;

        // Send createStream and wait for response
        let mut create_stream_cmd = Vec::new();
        create_stream_cmd.extend_from_slice(&self.codec.encode(&AmfValue::String("createStream".into())));
        create_stream_cmd.extend_from_slice(&self.codec.encode(&AmfValue::Number(4.0)));
        create_stream_cmd.extend_from_slice(&self.codec.encode(&AmfValue::Null));
        self.send_chunked_message(3, 0, RTMP_MSG_AMF0_CMD, 0, &create_stream_cmd).await?;

        // Wait for createStream response
        self.wait_for_response(4.0, "createStream").await?;
        tracing::debug!("createStream response received for {}", self.name);

        // Send publish on message stream 1
        let mut publish_cmd = Vec::new();
        publish_cmd.extend_from_slice(&self.codec.encode(&AmfValue::String("publish".into())));
        publish_cmd.extend_from_slice(&self.codec.encode(&AmfValue::Number(5.0)));
        publish_cmd.extend_from_slice(&self.codec.encode(&AmfValue::Null));
        publish_cmd.extend_from_slice(&self.codec.encode(&AmfValue::String(self.stream_key.clone())));
        publish_cmd.extend_from_slice(&self.codec.encode(&AmfValue::String("live".into())));
        self.send_chunked_message(4, 0, RTMP_MSG_AMF0_CMD, 1, &publish_cmd).await?;

        // Wait for onStatus (NetStream.Publish.Start)
        self.wait_for_publish_response().await?;
        tracing::debug!("Publish response received for {}", self.name);

        // Send Stream Begin event
        let stream_begin = vec![
            0x00,
            USER_CONTROL_STREAM_BEGIN as u8,
            0x00,
            0x00,
            0x00,
            0x01, // Stream ID = 1
        ];
        let stream_begin_header = create_rtmp_header(0, 2, 0, stream_begin.len() as u32, RTMP_MSG_USER_CONTROL, 0);
        self.send_raw(stream_begin_header, &stream_begin).await?;

        self.state = ClientState::Publishing;
        self.retry_count = 0;

        self.metrics.destinations_connected
            .with_label_values(&[&self.name])
            .set(1);

        tracing::info!("Connected to destination: {}", self.name);
        Ok(())
    }

    /// Wait for a response to a command with the given transaction ID
    async fn wait_for_response(&mut self, expected_txn_id: f64, command_name: &str) -> Result<(), RtmpError> {
        let deadline = tokio::time::Instant::now() + RESPONSE_TIMEOUT;

        loop {
            // Check timeout
            if tokio::time::Instant::now() >= deadline {
                return Err(RtmpError::Timeout(format!(
                    "Timeout waiting for {} response from {}",
                    command_name, self.name
                )));
            }

            // Read more data
            let socket = self.socket.as_mut().ok_or_else(|| RtmpError::DestinationFailed {
                dest: self.name.clone(),
                reason: "Socket closed while waiting for response".into(),
            })?;

            match timeout(Duration::from_millis(100), self.chunk_reader.read_from(socket)).await {
                Ok(Ok(_)) => {}
                Ok(Err(RtmpError::ConnectionClosed)) => {
                    return Err(RtmpError::DestinationFailed {
                        dest: self.name.clone(),
                        reason: "Connection closed while waiting for response".into(),
                    });
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => continue, // Timeout on read, try again
            }

            // Process chunks
            while let Some(chunk) = self.chunk_reader.try_read_chunk()? {
                // Handle protocol control messages
                if chunk.chunk_stream_id == 2 {
                    self.handle_protocol_control(&chunk)?;
                    continue;
                }

                // Try to assemble message
                if let Some(message) = self.message_assembler.add_chunk(chunk.chunk_stream_id, &chunk)? {
                    if message.message_type_id == RTMP_MSG_AMF0_CMD {
                        let values = self.codec.decode_all(&message.data)
                            .map_err(|e| RtmpError::InvalidAmf(e))?;

                        if values.is_empty() {
                            continue;
                        }

                        let cmd = values[0].as_string().unwrap_or("");
                        let txn_id = values.get(1).and_then(|v| v.as_number()).unwrap_or(0.0);

                        tracing::trace!("Received command: {} (txn={})", cmd, txn_id);

                        match cmd {
                            "_result" if (txn_id - expected_txn_id).abs() < 0.001 => {
                                return Ok(());
                            }
                            "_error" if (txn_id - expected_txn_id).abs() < 0.001 => {
                                let error_info = values.get(3)
                                    .and_then(|v| v.get("description"))
                                    .and_then(|v| v.as_string())
                                    .unwrap_or("Unknown error");
                                return Err(RtmpError::DestinationFailed {
                                    dest: self.name.clone(),
                                    reason: format!("{} failed: {}", command_name, error_info),
                                });
                            }
                            "onBWDone" | "_result" | "_error" => {
                                // Other responses, continue waiting
                            }
                            _ => {
                                tracing::trace!("Ignoring command while waiting: {}", cmd);
                            }
                        }
                    }
                }
            }
        }
    }

    /// Wait for publish response (onStatus with NetStream.Publish.Start)
    async fn wait_for_publish_response(&mut self) -> Result<(), RtmpError> {
        let deadline = tokio::time::Instant::now() + RESPONSE_TIMEOUT;

        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(RtmpError::Timeout(format!(
                    "Timeout waiting for publish response from {}",
                    self.name
                )));
            }

            let socket = self.socket.as_mut().ok_or_else(|| RtmpError::DestinationFailed {
                dest: self.name.clone(),
                reason: "Socket closed while waiting for publish response".into(),
            })?;

            match timeout(Duration::from_millis(100), self.chunk_reader.read_from(socket)).await {
                Ok(Ok(_)) => {}
                Ok(Err(RtmpError::ConnectionClosed)) => {
                    return Err(RtmpError::DestinationFailed {
                        dest: self.name.clone(),
                        reason: "Connection closed while waiting for publish response".into(),
                    });
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => continue,
            }

            while let Some(chunk) = self.chunk_reader.try_read_chunk()? {
                if chunk.chunk_stream_id == 2 {
                    self.handle_protocol_control(&chunk)?;
                    continue;
                }

                if let Some(message) = self.message_assembler.add_chunk(chunk.chunk_stream_id, &chunk)? {
                    if message.message_type_id == RTMP_MSG_AMF0_CMD {
                        let values = self.codec.decode_all(&message.data)
                            .map_err(|e| RtmpError::InvalidAmf(e))?;

                        if values.is_empty() {
                            continue;
                        }

                        let cmd = values[0].as_string().unwrap_or("");

                        if cmd == "onStatus" {
                            // Check the code in the info object
                            let code = values.get(3)
                                .and_then(|v| v.get("code"))
                                .and_then(|v| v.as_string())
                                .unwrap_or("");

                            if code == "NetStream.Publish.Start" {
                                return Ok(());
                            } else if code.contains("Failed") || code.contains("Rejected") || code.contains("Error") {
                                let desc = values.get(3)
                                    .and_then(|v| v.get("description"))
                                    .and_then(|v| v.as_string())
                                    .unwrap_or("Unknown error");
                                return Err(RtmpError::DestinationFailed {
                                    dest: self.name.clone(),
                                    reason: format!("Publish failed: {} - {}", code, desc),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// Handle protocol control messages
    fn handle_protocol_control(&mut self, chunk: &crate::protocol::RtmpChunk) -> Result<(), RtmpError> {
        match chunk.message_type_id {
            RTMP_MSG_SET_CHUNK_SIZE => {
                if chunk.data.len() >= 4 {
                    let size = u32::from_be_bytes([
                        chunk.data[0],
                        chunk.data[1],
                        chunk.data[2],
                        chunk.data[3],
                    ]);
                    self.chunk_reader.set_chunk_size(size);
                    tracing::debug!("{} set chunk size to {}", self.name, size);
                }
            }
            RTMP_MSG_WINDOW_ACK_SIZE => {
                tracing::trace!("Received Window ACK Size from {}", self.name);
            }
            _ => {
                tracing::trace!("Protocol control message type 0x{:02X} from {}", chunk.message_type_id, self.name);
            }
        }
        Ok(())
    }

    /// Publish media data (audio/video) to the destination.
    /// Uses ChunkWriter for proper chunk splitting of large messages.
    pub async fn publish(&mut self, data: &[u8], timestamp: u32, msg_type: u8) -> Result<(), RtmpError> {
        // Check connection state first
        if !self.is_connected() {
            return Err(RtmpError::DestinationFailed {
                dest: self.name.clone(),
                reason: format!("Not connected: {:?}", self.state),
            });
        }

        // Ensure we have a socket
        if self.socket.is_none() {
            return Err(RtmpError::DestinationFailed {
                dest: self.name.clone(),
                reason: "Socket not available".to_string(),
            });
        }

        // Use ChunkWriter for proper chunking
        // Audio uses csid 4, video uses csid 6 by convention
        let csid = if msg_type == 0x08 { 4 } else { 6 };
        let chunked_data = self.chunk_writer.write_message(
            csid,
            timestamp,
            msg_type,
            1, // message stream id
            data,
        );

        // Now we can safely get mutable borrow
        self.socket.as_mut().unwrap().write_all(&chunked_data).await?;

        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.state == ClientState::Publishing
    }

    pub fn mark_failed(&mut self, reason: String) {
        self.state = ClientState::Failed(reason);
        self.socket = None;

        self.metrics.destinations_connected
            .with_label_values(&[&self.name])
            .set(0);
        self.metrics.destination_errors_total
            .with_label_values(&[&self.name])
            .inc();
    }

    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }

    /// Send a message using the ChunkWriter for proper chunking
    async fn send_chunked_message(
        &mut self,
        csid: u32,
        timestamp: u32,
        msg_type: u8,
        stream_id: u32,
        data: &[u8],
    ) -> Result<(), RtmpError> {
        let chunked_data = self.chunk_writer.write_message(csid, timestamp, msg_type, stream_id, data);
        self.socket.as_mut().unwrap().write_all(&chunked_data).await?;
        self.socket.as_mut().unwrap().flush().await?;
        Ok(())
    }

    /// Send raw data with a pre-built header (for protocol control messages)
    async fn send_raw(&mut self, header: Vec<u8>, data: &[u8]) -> Result<(), RtmpError> {
        self.socket.as_mut().unwrap().write_all(&header).await?;
        self.socket.as_mut().unwrap().write_all(data).await?;
        self.socket.as_mut().unwrap().flush().await?;
        Ok(())
    }

    /// Send Window ACK Size protocol control message
    async fn send_window_ack_size(&mut self, size: u32) -> Result<(), RtmpError> {
        let header = create_rtmp_header(0, 2, 0, 4, RTMP_MSG_WINDOW_ACK_SIZE, 0);
        self.socket.as_mut().unwrap().write_all(&header).await?;
        self.socket.as_mut().unwrap().write_all(&size.to_be_bytes()).await?;
        self.socket.as_mut().unwrap().flush().await?;
        Ok(())
    }

    /// Send Set Chunk Size protocol control message
    async fn send_set_chunk_size(&mut self, size: u32) -> Result<(), RtmpError> {
        let header = create_rtmp_header(0, 2, 0, 4, RTMP_MSG_SET_CHUNK_SIZE, 0);
        self.socket.as_mut().unwrap().write_all(&header).await?;
        self.socket.as_mut().unwrap().write_all(&size.to_be_bytes()).await?;
        self.socket.as_mut().unwrap().flush().await?;
        Ok(())
    }
}

/// Parsed RTMP URL components
#[derive(Debug, Clone)]
pub struct ParsedRtmpUrl {
    /// Socket address (host:port)
    pub addr: String,
    /// Hostname
    pub host: String,
    /// Port number
    pub port: u16,
    /// Application name (e.g., "live", "app")
    pub app: Option<String>,
    /// Stream key or additional path
    pub stream_key: Option<String>,
}

/// Parse an RTMP URL into its components.
///
/// Supports formats:
/// - rtmp://host/app
/// - rtmp://host:port/app
/// - rtmp://host/app/stream_key
/// - rtmp://host:port/app/stream_key
/// - rtmp://[ipv6]:port/app
pub fn parse_rtmp_url(url: &str) -> Result<ParsedRtmpUrl, RtmpError> {
    let url = url.strip_prefix("rtmp://")
        .ok_or_else(|| RtmpError::Protocol("Invalid RTMP URL: missing rtmp:// prefix".into()))?;

    // Split host:port from path
    let (host_port, path) = match url.find('/') {
        Some(idx) => (&url[..idx], Some(&url[idx + 1..])),
        None => (url, None),
    };

    // Parse host and port, handling IPv6 addresses
    let (host, port) = parse_host_port(host_port)?;

    // Parse application name and stream key from path
    let (app, stream_key) = if let Some(path) = path {
        let path_parts: Vec<&str> = path.splitn(2, '/').collect();
        let app = if path_parts[0].is_empty() { None } else { Some(path_parts[0].to_string()) };
        let stream_key = path_parts.get(1).filter(|s| !s.is_empty()).map(|s| s.to_string());
        (app, stream_key)
    } else {
        (None, None)
    };

    Ok(ParsedRtmpUrl {
        addr: format!("{}:{}", host, port),
        host: host.to_string(),
        port,
        app,
        stream_key,
    })
}

fn parse_host_port(host_port: &str) -> Result<(&str, u16), RtmpError> {
    // Check for IPv6 address (enclosed in brackets)
    if host_port.starts_with('[') {
        // IPv6: [::1]:1935 or [::1]
        let bracket_end = host_port.find(']')
            .ok_or_else(|| RtmpError::Protocol("Invalid IPv6 address: missing closing bracket".into()))?;

        let host = &host_port[1..bracket_end];
        let rest = &host_port[bracket_end + 1..];

        let port = if rest.starts_with(':') {
            rest[1..].parse::<u16>()
                .map_err(|_| RtmpError::Protocol(format!("Invalid port: {}", &rest[1..])))?
        } else if rest.is_empty() {
            1935
        } else {
            return Err(RtmpError::Protocol(format!("Invalid IPv6 URL format: {}", host_port)));
        };

        Ok((host, port))
    } else {
        // IPv4 or hostname: host:port or host
        match host_port.rfind(':') {
            Some(colon_idx) => {
                let host = &host_port[..colon_idx];
                let port_str = &host_port[colon_idx + 1..];
                let port = port_str.parse::<u16>()
                    .map_err(|_| RtmpError::Protocol(format!("Invalid port: {}", port_str)))?;
                Ok((host, port))
            }
            None => Ok((host_port, 1935)),
        }
    }
}

/// Legacy function for backwards compatibility - returns just the socket address
#[allow(dead_code)]
fn parse_rtmp_url_simple(url: &str) -> Result<String, RtmpError> {
    parse_rtmp_url(url).map(|p| p.addr)
}

/// Encode an object property (key-value pair) for AMF0
/// Format: key_length (2 bytes) + key + value
fn encode_object_property(key: &str, value: &str) -> Vec<u8> {
    let codec = Amf0Codec::new();
    let mut result = Vec::new();
    // Key: length (2 bytes BE) + string bytes (no type marker for object keys)
    result.extend_from_slice(&(key.len() as u16).to_be_bytes());
    result.extend_from_slice(key.as_bytes());
    // Value: full AMF encoding with type marker
    result.extend_from_slice(&codec.encode(&AmfValue::String(value.to_string())));
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_url() {
        let parsed = parse_rtmp_url("rtmp://localhost/live").unwrap();
        assert_eq!(parsed.addr, "localhost:1935");
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 1935);
        assert_eq!(parsed.app, Some("live".to_string()));
        assert_eq!(parsed.stream_key, None);
    }

    #[test]
    fn test_parse_url_with_port() {
        let parsed = parse_rtmp_url("rtmp://localhost:1936/live").unwrap();
        assert_eq!(parsed.addr, "localhost:1936");
        assert_eq!(parsed.port, 1936);
        assert_eq!(parsed.app, Some("live".to_string()));
    }

    #[test]
    fn test_parse_twitch_url() {
        let parsed = parse_rtmp_url("rtmp://live.twitch.tv/app").unwrap();
        assert_eq!(parsed.addr, "live.twitch.tv:1935");
        assert_eq!(parsed.host, "live.twitch.tv");
        assert_eq!(parsed.app, Some("app".to_string()));
    }

    #[test]
    fn test_parse_youtube_url() {
        let parsed = parse_rtmp_url("rtmp://a.rtmp.youtube.com/live2").unwrap();
        assert_eq!(parsed.addr, "a.rtmp.youtube.com:1935");
        assert_eq!(parsed.app, Some("live2".to_string()));
    }

    #[test]
    fn test_parse_url_with_stream_key() {
        let parsed = parse_rtmp_url("rtmp://ingest.example.com/live/stream_key_123").unwrap();
        assert_eq!(parsed.addr, "ingest.example.com:1935");
        assert_eq!(parsed.app, Some("live".to_string()));
        assert_eq!(parsed.stream_key, Some("stream_key_123".to_string()));
    }

    #[test]
    fn test_parse_url_with_port_and_stream_key() {
        let parsed = parse_rtmp_url("rtmp://server.com:1936/app/my_stream").unwrap();
        assert_eq!(parsed.addr, "server.com:1936");
        assert_eq!(parsed.port, 1936);
        assert_eq!(parsed.app, Some("app".to_string()));
        assert_eq!(parsed.stream_key, Some("my_stream".to_string()));
    }

    #[test]
    fn test_parse_ipv6_url() {
        let parsed = parse_rtmp_url("rtmp://[::1]:1935/live").unwrap();
        assert_eq!(parsed.addr, "::1:1935");
        assert_eq!(parsed.host, "::1");
        assert_eq!(parsed.port, 1935);
        assert_eq!(parsed.app, Some("live".to_string()));
    }

    #[test]
    fn test_parse_ipv6_url_default_port() {
        let parsed = parse_rtmp_url("rtmp://[::1]/live").unwrap();
        assert_eq!(parsed.port, 1935);
    }

    #[test]
    fn test_parse_url_no_app() {
        let parsed = parse_rtmp_url("rtmp://localhost").unwrap();
        assert_eq!(parsed.addr, "localhost:1935");
        assert_eq!(parsed.app, None);
    }

    #[test]
    fn test_parse_invalid_url_no_prefix() {
        let result = parse_rtmp_url("http://localhost/live");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_invalid_port() {
        let result = parse_rtmp_url("rtmp://localhost:invalid/live");
        assert!(result.is_err());
    }
}
