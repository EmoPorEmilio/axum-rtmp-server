use crate::error::RtmpError;
use crate::protocol::{perform_client_handshake, RTMP_MSG_AMF0_CMD, RTMP_MSG_USER_CONTROL, USER_CONTROL_STREAM_BEGIN};
use crate::amf::{AmfValue, Amf0Codec, create_rtmp_header};
use crate::metrics::MetricsCollector;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use std::sync::Arc;

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
}

impl RtmpClient {
    pub fn new(
        name: String,
        url: String,
        stream_key: String,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            name,
            url,
            stream_key,
            socket: None,
            state: ClientState::Disconnected,
            retry_count: 0,
            metrics,
        }
    }

    pub async fn connect(&mut self) -> Result<(), RtmpError> {
        self.state = ClientState::Connecting;

        let addr = parse_rtmp_url(&self.url)?;
        let socket = TcpStream::connect(addr).await?;
        self.socket = Some(socket);

        self.state = ClientState::Handshaking;
        perform_client_handshake(self.socket.as_mut().unwrap()).await?;

        let codec = Amf0Codec;

        // Send connect command
        let mut connect_cmd = Vec::new();
        connect_cmd.extend_from_slice(&codec.encode(&AmfValue::String("connect".into())));
        connect_cmd.extend_from_slice(&codec.encode(&AmfValue::Number(1.0)));
        let mut connect_obj = vec![0x03];
        connect_obj.extend_from_slice(&codec.encode(&AmfValue::String("app".into())));
        connect_obj.extend_from_slice(&codec.encode(&AmfValue::String("live".into())));
        connect_obj.extend_from_slice(&[0x00, 0x00, 0x09]); // Object end
        connect_cmd.extend_from_slice(&connect_obj);
        let connect_header = create_rtmp_header(0, 2, 0, connect_cmd.len() as u32, RTMP_MSG_AMF0_CMD, 0);
        self.send_message(2, connect_header, &connect_cmd).await?;

        // Send createStream
        let mut create_stream_cmd = Vec::new();
        create_stream_cmd.extend_from_slice(&codec.encode(&AmfValue::String("createStream".into())));
        create_stream_cmd.extend_from_slice(&codec.encode(&AmfValue::Number(2.0)));
        create_stream_cmd.extend_from_slice(&codec.encode(&AmfValue::Null));
        let create_stream_header = create_rtmp_header(0, 3, 0, create_stream_cmd.len() as u32, RTMP_MSG_AMF0_CMD, 0);
        self.send_message(3, create_stream_header, &create_stream_cmd).await?;

        // Send publish
        let mut publish_cmd = Vec::new();
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::String("publish".into())));
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::Number(3.0)));
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::Null));
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::String(self.stream_key.clone())));
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::String("live".into())));
        let publish_header = create_rtmp_header(0, 4, 0, publish_cmd.len() as u32, RTMP_MSG_AMF0_CMD, 0);
        self.send_message(4, publish_header, &publish_cmd).await?;

        // Send Stream Begin event
        let mut stream_begin = vec![
            0x00,
            USER_CONTROL_STREAM_BEGIN as u8,
            0x00,
            0x00,
            0x00,
            0x01, // Stream ID = 1
        ];
        let stream_begin_header = create_rtmp_header(0, 2, 0, stream_begin.len() as u32, RTMP_MSG_USER_CONTROL, 0);
        self.send_message(2, stream_begin_header, &stream_begin).await?;

        self.state = ClientState::Publishing;
        self.retry_count = 0;

        tracing::info!("Connected to destination: {}", self.name);
        Ok(())
    }

    pub async fn publish(&mut self, data: &[u8], timestamp: u32, msg_type: u8) -> Result<(), RtmpError> {
        if !self.is_connected() {
            return Err(RtmpError::DestinationFailed {
                dest: self.name.clone(),
                reason: format!("Not connected: {:?}", self.state),
            });
        }

        let header = create_rtmp_header(0, 6, timestamp, data.len() as u32, msg_type, 1);
        self.socket.as_mut().unwrap().write_all(&header).await?;
        self.socket.as_mut().unwrap().write_all(data).await?;

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

    async fn send_message(&mut self, _chunk_stream_id: u32, header: Vec<u8>, data: &[u8]) -> Result<(), RtmpError> {
        self.socket.as_mut().unwrap().write_all(&header).await?;
        self.socket.as_mut().unwrap().write_all(data).await?;
        Ok(())
    }
}

fn parse_rtmp_url(url: &str) -> Result<String, RtmpError> {
    let url = url.strip_prefix("rtmp://")
        .ok_or_else(|| RtmpError::Protocol("Invalid RTMP URL".into()))?;

    let parts: Vec<&str> = url.split('/').collect();
    if parts.is_empty() {
        return Err(RtmpError::Protocol("Invalid RTMP URL".into()));
    }

    let port = parts.get(1).unwrap_or(&"1935");
    Ok(format!("{}:{}", parts[0], port))
}
