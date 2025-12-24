use crate::amf::{AmfValue, Amf0Codec};
use crate::config::Config;
use crate::connection::{ConnectionState, StreamState};
use crate::error::RtmpError;
use crate::metrics::MetricsCollector;
use crate::protocol::{
    create_chunk_header, AssembledMessage, ChunkReader, MessageAssembler, RtmpChunk,
    RTMP_MSG_ACK, RTMP_MSG_AMF0_CMD, RTMP_MSG_AMF0_DATA, RTMP_MSG_AUDIO,
    RTMP_MSG_SET_CHUNK_SIZE, RTMP_MSG_SET_PEER_BANDWIDTH, RTMP_MSG_USER_CONTROL,
    RTMP_MSG_VIDEO, RTMP_MSG_WINDOW_ACK_SIZE, USER_CONTROL_STREAM_BEGIN,
};
use bytes::Bytes;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio::time::{timeout, Duration};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_WINDOW_ACK_SIZE: u32 = 2_500_000;
const DEFAULT_PEER_BANDWIDTH: u32 = 2_500_000;

/// Handles a single RTMP connection from a publisher (e.g., OBS)
pub struct ConnectionHandler {
    socket: TcpStream,
    state: ConnectionState,
    chunk_reader: ChunkReader,
    message_assembler: MessageAssembler,
    chunk_size_in: u32,
    chunk_size_out: u32,
    codec: Amf0Codec,
    config: Arc<Config>,
    metrics: Arc<MetricsCollector>,
    /// Channel to send media chunks to destinations
    media_tx: broadcast::Sender<RtmpChunk>,
    /// Next transaction ID for outgoing commands
    next_transaction_id: f64,
}

impl ConnectionHandler {
    pub fn new(
        socket: TcpStream,
        config: Arc<Config>,
        metrics: Arc<MetricsCollector>,
        media_tx: broadcast::Sender<RtmpChunk>,
    ) -> Self {
        Self {
            socket,
            state: ConnectionState::default(),
            chunk_reader: ChunkReader::new(),
            message_assembler: MessageAssembler::new(),
            chunk_size_in: 128,
            chunk_size_out: 4096, // We'll use a larger chunk size for sending
            codec: Amf0Codec::new(),
            config,
            metrics,
            media_tx,
            next_transaction_id: 1.0,
        }
    }

    /// Run the connection handler main loop
    pub async fn run(mut self) -> Result<(), RtmpError> {
        // Send initial protocol messages
        self.send_window_ack_size(DEFAULT_WINDOW_ACK_SIZE).await?;
        self.send_set_peer_bandwidth(DEFAULT_PEER_BANDWIDTH, 2).await?;
        self.send_set_chunk_size(self.chunk_size_out).await?;

        self.state.stream_state = StreamState::Connected;
        self.metrics.connections_active.inc();

        let result = self.run_loop().await;

        // Cleanup
        self.metrics.connections_active.dec();
        if self.state.stream_state == StreamState::Publishing {
            tracing::info!(
                "Publisher disconnected: app={:?} key={:?}",
                self.state.app_name,
                self.state.stream_key.as_ref().map(|_| "***")
            );
        }

        result
    }

    async fn run_loop(&mut self) -> Result<(), RtmpError> {
        loop {
            // Read data with timeout
            match timeout(CONNECTION_TIMEOUT, self.chunk_reader.read_from(&mut self.socket)).await {
                Ok(Ok(_)) => {}
                Ok(Err(RtmpError::ConnectionClosed)) => {
                    tracing::debug!("Connection closed gracefully");
                    return Ok(());
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(RtmpError::Timeout("Connection idle timeout".into()));
                }
            }

            // Process all available chunks
            while let Some(chunk) = self.chunk_reader.try_read_chunk()? {
                self.process_chunk(chunk).await?;
            }
        }
    }

    async fn process_chunk(&mut self, chunk: RtmpChunk) -> Result<(), RtmpError> {
        // Handle protocol control messages immediately (csid 2)
        if chunk.chunk_stream_id == 2 {
            return self.handle_protocol_control(&chunk).await;
        }

        // Assemble multi-chunk messages
        if let Some(message) = self.message_assembler.add_chunk(chunk.chunk_stream_id, &chunk)? {
            self.handle_message(message).await?;
        }

        Ok(())
    }

    async fn handle_protocol_control(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        match chunk.message_type_id {
            RTMP_MSG_SET_CHUNK_SIZE => {
                if chunk.data.len() >= 4 {
                    let size = u32::from_be_bytes([
                        chunk.data[0],
                        chunk.data[1],
                        chunk.data[2],
                        chunk.data[3],
                    ]);
                    self.chunk_size_in = size;
                    self.chunk_reader.set_chunk_size(size);
                    tracing::debug!("Client set chunk size to {}", size);
                }
            }
            RTMP_MSG_WINDOW_ACK_SIZE => {
                tracing::debug!("Received Window ACK Size");
            }
            RTMP_MSG_ACK => {
                tracing::trace!("Received ACK");
            }
            RTMP_MSG_USER_CONTROL => {
                tracing::debug!("Received User Control message");
            }
            _ => {
                // Other control messages on csid 2 should still be assembled
                if let Some(message) = self.message_assembler.add_chunk(chunk.chunk_stream_id, chunk)? {
                    self.handle_message(message).await?;
                }
            }
        }
        Ok(())
    }

    async fn handle_message(&mut self, message: AssembledMessage) -> Result<(), RtmpError> {
        match message.message_type_id {
            RTMP_MSG_AMF0_CMD => {
                self.handle_amf0_command(&message.data).await?;
            }
            RTMP_MSG_AMF0_DATA => {
                // Metadata - forward to destinations
                if self.state.stream_state == StreamState::Publishing {
                    self.forward_media(message).await?;
                }
            }
            RTMP_MSG_AUDIO => {
                if self.state.stream_state == StreamState::Publishing {
                    let data_len = message.data.len() as u64;
                    self.forward_media(message).await?;
                    self.metrics.bytes_received.inc_by(data_len);
                }
            }
            RTMP_MSG_VIDEO => {
                if self.state.stream_state == StreamState::Publishing {
                    let data_len = message.data.len() as u64;
                    self.forward_media(message).await?;
                    self.metrics.bytes_received.inc_by(data_len);
                }
            }
            _ => {
                tracing::trace!("Unhandled message type: 0x{:02X}", message.message_type_id);
            }
        }
        Ok(())
    }

    async fn handle_amf0_command(&mut self, data: &[u8]) -> Result<(), RtmpError> {
        let values = self.codec.decode_all(data)
            .map_err(|e| RtmpError::InvalidAmf(e))?;

        if values.is_empty() {
            return Ok(());
        }

        let command = values[0].as_string().unwrap_or("");
        let transaction_id = values.get(1).and_then(|v| v.as_number()).unwrap_or(0.0);

        tracing::debug!("Received command: {} (txn={})", command, transaction_id);

        match command {
            "connect" => {
                self.handle_connect(&values, transaction_id).await?;
            }
            "releaseStream" => {
                // Twitch sends this before createStream - acknowledge it
                self.send_result(transaction_id, AmfValue::Null, AmfValue::Undefined).await?;
            }
            "FCPublish" => {
                // Twitch sends this before publish - acknowledge it
                self.send_result(transaction_id, AmfValue::Null, AmfValue::Undefined).await?;
            }
            "createStream" => {
                self.handle_create_stream(transaction_id).await?;
            }
            "publish" => {
                self.handle_publish(&values).await?;
            }
            "FCUnpublish" => {
                tracing::info!("Client unpublishing stream");
                self.state.stream_state = StreamState::Ready;
            }
            "deleteStream" => {
                tracing::info!("Client deleting stream");
                self.state.stream_state = StreamState::Connected;
            }
            "@setDataFrame" => {
                // Metadata - forward to destinations
                tracing::debug!("Received metadata");
            }
            _ => {
                tracing::debug!("Unknown command: {}", command);
            }
        }

        Ok(())
    }

    async fn handle_connect(&mut self, values: &[AmfValue], transaction_id: f64) -> Result<(), RtmpError> {
        // Extract app name from connect object
        if let Some(connect_obj) = values.get(2) {
            if let Some(app) = connect_obj.get("app").and_then(|v| v.as_string()) {
                self.state.app_name = Some(app.to_string());
                tracing::info!("Client connecting to app: {}", app);
            }
        }

        self.state.is_connected = true;

        // Send _result response
        let properties = AmfValue::Object(vec![
            ("fmsVer".into(), AmfValue::String("FMS/3,5,7,7009".into())),
            ("capabilities".into(), AmfValue::Number(31.0)),
            ("mode".into(), AmfValue::Number(1.0)),
        ]);

        let info = AmfValue::Object(vec![
            ("level".into(), AmfValue::String("status".into())),
            ("code".into(), AmfValue::String("NetConnection.Connect.Success".into())),
            ("description".into(), AmfValue::String("Connection succeeded".into())),
            ("objectEncoding".into(), AmfValue::Number(0.0)),
        ]);

        self.send_result(transaction_id, properties, info).await?;

        // Send onBWDone
        self.send_command("onBWDone", &[AmfValue::Number(0.0)]).await?;

        self.state.stream_state = StreamState::Ready;
        self.metrics.connections_total.inc();

        Ok(())
    }

    async fn handle_create_stream(&mut self, transaction_id: f64) -> Result<(), RtmpError> {
        self.state.stream_id = 1; // Assign stream ID 1

        // Send _result with stream ID
        self.send_result(transaction_id, AmfValue::Null, AmfValue::Number(1.0)).await?;

        tracing::debug!("Created stream with ID: {}", self.state.stream_id);
        Ok(())
    }

    async fn handle_publish(&mut self, values: &[AmfValue]) -> Result<(), RtmpError> {
        // publish(txn, null, stream_name, publish_type)
        let stream_name = values.get(3).and_then(|v| v.as_string()).unwrap_or("");
        let publish_type = values.get(4).and_then(|v| v.as_string()).unwrap_or("live");

        tracing::info!("Client publishing: {} (type: {})", stream_name, publish_type);

        // Validate stream key
        let is_authorized = self.config.auth.allowed_stream_keys.iter()
            .any(|key| key == stream_name);

        if !is_authorized {
            self.metrics.auth_failures_total.inc();
            self.send_on_status(
                "error",
                "NetStream.Publish.Unauthorized",
                "Stream key not authorized",
            ).await?;
            return Err(RtmpError::AuthFailed("Invalid stream key".into()));
        }

        self.state.stream_key = Some(stream_name.to_string());
        self.state.stream_state = StreamState::Publishing;
        self.metrics.auth_attempts_total.inc();
        self.metrics.streams_published.inc();

        // Send StreamBegin
        self.send_stream_begin(self.state.stream_id).await?;

        // Send onStatus
        self.send_on_status(
            "status",
            "NetStream.Publish.Start",
            &format!("Publishing {}", stream_name),
        ).await?;

        Ok(())
    }

    async fn forward_media(&mut self, message: AssembledMessage) -> Result<(), RtmpError> {
        let chunk = RtmpChunk::new(
            0,
            message.chunk_stream_id,
            message.timestamp,
            message.data.len() as u32,
            message.message_type_id,
            message.message_stream_id,
            message.data,
        );

        // Send to broadcast channel - if no receivers, that's fine
        let _ = self.media_tx.send(chunk);
        self.metrics.chunks_received.inc();

        Ok(())
    }

    // --- Protocol message helpers ---

    async fn send_window_ack_size(&mut self, size: u32) -> Result<(), RtmpError> {
        let header = create_chunk_header(0, 2, 0, 4, RTMP_MSG_WINDOW_ACK_SIZE, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(&size.to_be_bytes()).await?;
        Ok(())
    }

    async fn send_set_peer_bandwidth(&mut self, size: u32, limit_type: u8) -> Result<(), RtmpError> {
        let header = create_chunk_header(0, 2, 0, 5, RTMP_MSG_SET_PEER_BANDWIDTH, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(&size.to_be_bytes()).await?;
        self.socket.write_all(&[limit_type]).await?;
        Ok(())
    }

    async fn send_set_chunk_size(&mut self, size: u32) -> Result<(), RtmpError> {
        let header = create_chunk_header(0, 2, 0, 4, RTMP_MSG_SET_CHUNK_SIZE, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(&size.to_be_bytes()).await?;
        self.chunk_size_out = size;
        Ok(())
    }

    async fn send_stream_begin(&mut self, stream_id: u32) -> Result<(), RtmpError> {
        let mut data = Vec::with_capacity(6);
        data.extend_from_slice(&(USER_CONTROL_STREAM_BEGIN as u16).to_be_bytes());
        data.extend_from_slice(&stream_id.to_be_bytes());

        let header = create_chunk_header(0, 2, 0, data.len() as u32, RTMP_MSG_USER_CONTROL, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(&data).await?;
        Ok(())
    }

    async fn send_result(
        &mut self,
        transaction_id: f64,
        properties: AmfValue,
        info: AmfValue,
    ) -> Result<(), RtmpError> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.codec.encode(&AmfValue::String("_result".into())));
        data.extend_from_slice(&self.codec.encode(&AmfValue::Number(transaction_id)));
        data.extend_from_slice(&self.codec.encode(&properties));
        data.extend_from_slice(&self.codec.encode(&info));

        self.send_amf0_message(3, &data).await
    }

    async fn send_on_status(
        &mut self,
        level: &str,
        code: &str,
        description: &str,
    ) -> Result<(), RtmpError> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.codec.encode(&AmfValue::String("onStatus".into())));
        data.extend_from_slice(&self.codec.encode(&AmfValue::Number(0.0)));
        data.extend_from_slice(&self.codec.encode(&AmfValue::Null));
        data.extend_from_slice(&self.codec.encode(&AmfValue::Object(vec![
            ("level".into(), AmfValue::String(level.into())),
            ("code".into(), AmfValue::String(code.into())),
            ("description".into(), AmfValue::String(description.into())),
        ])));

        self.send_amf0_message(5, &data).await
    }

    async fn send_command(&mut self, name: &str, args: &[AmfValue]) -> Result<(), RtmpError> {
        let mut data = Vec::new();
        data.extend_from_slice(&self.codec.encode(&AmfValue::String(name.into())));
        data.extend_from_slice(&self.codec.encode(&AmfValue::Number(0.0)));
        for arg in args {
            data.extend_from_slice(&self.codec.encode(arg));
        }

        self.send_amf0_message(3, &data).await
    }

    async fn send_amf0_message(&mut self, csid: u32, data: &[u8]) -> Result<(), RtmpError> {
        // For now, send as single chunk (we'll add chunking in Phase 3)
        let header = create_chunk_header(0, csid, 0, data.len() as u32, RTMP_MSG_AMF0_CMD, 0);
        self.socket.write_all(&header).await?;
        self.socket.write_all(data).await?;
        self.socket.flush().await?;
        Ok(())
    }
}
