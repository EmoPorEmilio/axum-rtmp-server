use crate::error::RtmpError;
use bytes::{Buf, BytesMut};
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub const RTMP_VERSION: u8 = 3;
pub const DEFAULT_CHUNK_SIZE: u32 = 128;
pub const MAX_CHUNK_SIZE: u32 = 65536;
pub const INITIAL_BUFFER_SIZE: usize = 4096;
pub const MAX_BUFFER_SIZE: usize = 10_485_760;

// Message Type IDs
pub const RTMP_MSG_WINDOW_ACK_SIZE: u8 = 0x5;
pub const RTMP_MSG_SET_CHUNK_SIZE: u8 = 0x1;
pub const RTMP_MSG_ACK: u8 = 0x3;
pub const RTMP_MSG_USER_CONTROL: u8 = 0x4;
pub const RTMP_MSG_SET_PEER_BANDWIDTH: u8 = 0x6;
pub const RTMP_MSG_AUDIO: u8 = 0x8;
pub const RTMP_MSG_VIDEO: u8 = 0x9;
pub const RTMP_MSG_AMF3_CMD: u8 = 0x11;
pub const RTMP_MSG_AMF3_DATA: u8 = 0x0F;
pub const RTMP_MSG_AMF0_CMD: u8 = 0x14;
pub const RTMP_MSG_AMF0_DATA: u8 = 0x12;
pub const RTMP_MSG_AMF0_METADATA: u8 = 0x12;
pub const RTMP_MSG_AMF3_METADATA: u8 = 0x0F;

// User control message event types
pub const USER_CONTROL_STREAM_BEGIN: u16 = 0;
pub const USER_CONTROL_STREAM_EOF: u16 = 1;
pub const USER_CONTROL_STREAM_DRY: u16 = 2;
pub const USER_CONTROL_SET_BUFFER_LENGTH: u16 = 3;
pub const USER_CONTROL_STREAM_IS_RECORDED: u16 = 4;
pub const USER_CONTROL_PING_REQUEST: u16 = 6;
pub const USER_CONTROL_PING_RESPONSE: u16 = 7;

#[derive(Debug, Clone)]
pub struct RtmpChunk {
    pub chunk_type: u8,
    pub chunk_stream_id: u32,
    pub timestamp: u32,
    pub message_length: u32,
    pub message_type_id: u8,
    pub message_stream_id: u32,
    pub data: Vec<u8>,
}

impl RtmpChunk {
    pub fn new(
        chunk_type: u8,
        chunk_stream_id: u32,
        timestamp: u32,
        message_length: u32,
        message_type_id: u8,
        message_stream_id: u32,
        data: Vec<u8>,
    ) -> Self {
        Self {
            chunk_type,
            chunk_stream_id,
            timestamp,
            message_length,
            message_type_id,
            message_stream_id,
            data,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkHeader {
    pub chunk_type: u8,
    pub chunk_stream_id: u32,
    pub timestamp: u32,
    pub message_length: u32,
    pub message_type_id: u8,
    pub message_stream_id: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ChunkStreamState {
    pub timestamp: u32,
    pub message_length: u32,
    pub message_type_id: u8,
    pub message_stream_id: u32,
    pub bytes_left: u32,
    pub data: Vec<u8>,
}

pub struct PendingMessage {
    data: Vec<u8>,
    bytes_received: u32,
    total_length: u32,
    timestamp: u32,
    message_type_id: u8,
    message_stream_id: u32,
}

pub struct AssembledMessage {
    pub timestamp: u32,
    pub message_type_id: u8,
    pub message_stream_id: u32,
    pub data: Vec<u8>,
}

pub struct MessageAssembler {
    pending: HashMap<u32, PendingMessage>,
}

impl MessageAssembler {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
        }
    }

    pub fn add_chunk(
        &mut self,
        csid: u32,
        chunk: &RtmpChunk,
    ) -> Result<Option<AssembledMessage>, RtmpError> {
        let pending = self.pending.entry(csid).or_insert_with(|| PendingMessage {
            data: Vec::with_capacity(chunk.message_length as usize),
            bytes_received: 0,
            total_length: chunk.message_length,
            timestamp: chunk.timestamp,
            message_type_id: chunk.message_type_id,
            message_stream_id: chunk.message_stream_id,
        });

        pending.data.extend_from_slice(&chunk.data);
        pending.bytes_received += chunk.data.len() as u32;

        if pending.bytes_received >= pending.total_length {
            let msg = AssembledMessage {
                timestamp: pending.timestamp,
                message_type_id: pending.message_type_id,
                message_stream_id: pending.message_stream_id,
                data: std::mem::take(&mut pending.data),
            };
            self.pending.remove(&csid);
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    pub fn cleanup(&mut self, _max_age: std::time::Duration) {
        self.pending.retain(|_, pending| {
            pending.data.len() > 0 || pending.total_length > 0
        });
    }
}
