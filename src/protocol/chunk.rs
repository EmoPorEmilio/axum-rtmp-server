use crate::error::RtmpError;
use bytes::{Buf, Bytes, BytesMut};
use std::collections::HashMap;
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

pub const RTMP_VERSION: u8 = 3;
pub const DEFAULT_CHUNK_SIZE: u32 = 128;
pub const MAX_CHUNK_SIZE: u32 = 65536;
pub const INITIAL_BUFFER_SIZE: usize = 4096;
pub const MAX_BUFFER_SIZE: usize = 10_485_760;
pub const MAX_PENDING_MESSAGES: usize = 256;

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
    pub data: Bytes,
}

impl RtmpChunk {
    pub fn new(
        chunk_type: u8,
        chunk_stream_id: u32,
        timestamp: u32,
        message_length: u32,
        message_type_id: u8,
        message_stream_id: u32,
        data: impl Into<Bytes>,
    ) -> Self {
        Self {
            chunk_type,
            chunk_stream_id,
            timestamp,
            message_length,
            message_type_id,
            message_stream_id,
            data: data.into(),
        }
    }

    /// Create a chunk from a Vec<u8> (for backwards compatibility)
    pub fn from_vec(
        chunk_type: u8,
        chunk_stream_id: u32,
        timestamp: u32,
        message_length: u32,
        message_type_id: u8,
        message_stream_id: u32,
        data: Vec<u8>,
    ) -> Self {
        Self::new(
            chunk_type,
            chunk_stream_id,
            timestamp,
            message_length,
            message_type_id,
            message_stream_id,
            Bytes::from(data),
        )
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
    pub timestamp_delta: u32,
    pub message_length: u32,
    pub message_type_id: u8,
    pub message_stream_id: u32,
    pub has_extended_timestamp: bool,
}

pub struct PendingMessage {
    data: Vec<u8>,
    bytes_received: u32,
    total_length: u32,
    timestamp: u32,
    message_type_id: u8,
    message_stream_id: u32,
    created_at: Instant,
}

pub struct AssembledMessage {
    pub timestamp: u32,
    pub message_type_id: u8,
    pub message_stream_id: u32,
    pub chunk_stream_id: u32,
    pub data: Bytes,
}

pub struct MessageAssembler {
    pending: HashMap<u32, PendingMessage>,
    max_pending_messages: usize,
    max_message_size: usize,
}

impl Default for MessageAssembler {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageAssembler {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
            max_pending_messages: MAX_PENDING_MESSAGES,
            max_message_size: MAX_BUFFER_SIZE,
        }
    }

    pub fn with_limits(max_pending_messages: usize, max_message_size: usize) -> Self {
        Self {
            pending: HashMap::new(),
            max_pending_messages,
            max_message_size,
        }
    }

    pub fn add_chunk(
        &mut self,
        csid: u32,
        chunk: &RtmpChunk,
    ) -> Result<Option<AssembledMessage>, RtmpError> {
        // Enforce message size limit
        if chunk.message_length as usize > self.max_message_size {
            return Err(RtmpError::Protocol(format!(
                "Message too large: {} bytes (max: {})",
                chunk.message_length, self.max_message_size
            )));
        }

        // Enforce pending message count limit
        if !self.pending.contains_key(&csid) && self.pending.len() >= self.max_pending_messages {
            // Try to clean up stale entries first
            self.cleanup(std::time::Duration::from_secs(30));
            if self.pending.len() >= self.max_pending_messages {
                return Err(RtmpError::Protocol(format!(
                    "Too many pending messages: {} (max: {})",
                    self.pending.len(), self.max_pending_messages
                )));
            }
        }

        let pending = self.pending.entry(csid).or_insert_with(|| PendingMessage {
            data: Vec::with_capacity(chunk.message_length as usize),
            bytes_received: 0,
            total_length: chunk.message_length,
            timestamp: chunk.timestamp,
            message_type_id: chunk.message_type_id,
            message_stream_id: chunk.message_stream_id,
            created_at: Instant::now(),
        });

        pending.data.extend_from_slice(&chunk.data);
        pending.bytes_received += chunk.data.len() as u32;

        if pending.bytes_received >= pending.total_length {
            let msg = AssembledMessage {
                timestamp: pending.timestamp,
                message_type_id: pending.message_type_id,
                message_stream_id: pending.message_stream_id,
                chunk_stream_id: csid,
                data: Bytes::from(std::mem::take(&mut pending.data)),
            };
            self.pending.remove(&csid);
            Ok(Some(msg))
        } else {
            Ok(None)
        }
    }

    /// Clean up pending messages older than max_age
    pub fn cleanup(&mut self, max_age: std::time::Duration) {
        let now = Instant::now();
        self.pending.retain(|_, pending| {
            now.duration_since(pending.created_at) < max_age
        });
    }

    /// Get the number of pending messages
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// Clear all pending messages
    pub fn clear(&mut self) {
        self.pending.clear();
    }
}

/// ChunkReader handles reading and parsing RTMP chunks from a byte buffer.
/// It maintains state for each chunk stream to handle abbreviated headers.
pub struct ChunkReader {
    buffer: BytesMut,
    chunk_size: u32,
    chunk_states: HashMap<u32, ChunkStreamState>,
}

impl Default for ChunkReader {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkReader {
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(INITIAL_BUFFER_SIZE),
            chunk_size: DEFAULT_CHUNK_SIZE,
            chunk_states: HashMap::new(),
        }
    }

    /// Set the chunk size for reading (after receiving Set Chunk Size message)
    pub fn set_chunk_size(&mut self, size: u32) {
        self.chunk_size = size.min(MAX_CHUNK_SIZE).max(1);
    }

    /// Get the current chunk size
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Read more data from the socket into the internal buffer
    pub async fn read_from(&mut self, socket: &mut TcpStream) -> Result<usize, RtmpError> {
        // Ensure we have capacity
        if self.buffer.capacity() - self.buffer.len() < 4096 {
            self.buffer.reserve(4096);
        }

        let n = socket.read_buf(&mut self.buffer).await?;
        if n == 0 {
            return Err(RtmpError::ConnectionClosed);
        }
        Ok(n)
    }

    /// Append data directly to the buffer (useful for testing)
    pub fn append(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Get the number of bytes currently buffered
    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    /// Try to parse a complete chunk from the buffer.
    /// Returns None if not enough data is available.
    pub fn try_read_chunk(&mut self) -> Result<Option<RtmpChunk>, RtmpError> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        // Parse basic header to get format type and chunk stream ID
        let (fmt, csid, basic_header_len) = match Self::parse_basic_header(&self.buffer) {
            Some(result) => result,
            None => return Ok(None),
        };

        let data_after_basic = &self.buffer[basic_header_len..];

        // Parse message header based on format type
        let (header, message_header_len) = match self.parse_message_header(fmt, csid, data_after_basic) {
            Some(result) => result,
            None => return Ok(None),
        };

        // Calculate how much chunk data we expect
        let state = self.chunk_states.get(&csid);
        let message_bytes_remaining = if let Some(s) = state {
            // For continuation chunks, check how much is left of the current message
            if s.message_length > 0 {
                s.message_length
            } else {
                header.message_length
            }
        } else {
            header.message_length
        };

        let chunk_data_len = (message_bytes_remaining as usize).min(self.chunk_size as usize);
        let total_header_len = basic_header_len + message_header_len;
        let total_chunk_len = total_header_len + chunk_data_len;

        // Check if we have enough data
        if self.buffer.len() < total_chunk_len {
            return Ok(None);
        }

        // Extract chunk data
        let chunk_data = Bytes::copy_from_slice(
            &self.buffer[total_header_len..total_chunk_len]
        );

        // Update chunk stream state for future abbreviated headers
        let state = self.chunk_states.entry(csid).or_default();
        state.timestamp = header.timestamp;
        state.message_length = header.message_length;
        state.message_type_id = header.message_type_id;
        state.message_stream_id = header.message_stream_id;
        state.has_extended_timestamp = header.timestamp >= 0xFFFFFF;

        // Consume the bytes from the buffer
        self.buffer.advance(total_chunk_len);

        Ok(Some(RtmpChunk {
            chunk_type: fmt,
            chunk_stream_id: csid,
            timestamp: header.timestamp,
            message_length: header.message_length,
            message_type_id: header.message_type_id,
            message_stream_id: header.message_stream_id,
            data: chunk_data,
        }))
    }

    /// Parse the basic header (1-3 bytes) to get format type and chunk stream ID
    /// Returns (fmt, csid, bytes_consumed)
    pub fn parse_basic_header(data: &[u8]) -> Option<(u8, u32, usize)> {
        if data.is_empty() {
            return None;
        }

        let first_byte = data[0];
        let fmt = (first_byte >> 6) & 0x03;
        let csid_indicator = first_byte & 0x3F;

        match csid_indicator {
            0 => {
                // 2-byte form: csid = 64 + second_byte
                if data.len() < 2 {
                    return None;
                }
                Some((fmt, 64 + data[1] as u32, 2))
            }
            1 => {
                // 3-byte form: csid = 64 + second_byte + third_byte * 256
                if data.len() < 3 {
                    return None;
                }
                Some((fmt, 64 + data[1] as u32 + (data[2] as u32) * 256, 3))
            }
            _ => {
                // 1-byte form: csid = csid_indicator (2-63)
                Some((fmt, csid_indicator as u32, 1))
            }
        }
    }

    /// Parse the message header based on format type.
    /// Uses stored state for abbreviated headers (types 1, 2, 3).
    /// Returns (header, bytes_consumed)
    fn parse_message_header(&self, fmt: u8, csid: u32, data: &[u8]) -> Option<(ChunkHeader, usize)> {
        let state = self.chunk_states.get(&csid).cloned().unwrap_or_default();

        match fmt {
            0 => {
                // Type 0: Full header - 11 bytes + optional 4-byte extended timestamp
                // timestamp (3 bytes) + message_length (3 bytes) + message_type_id (1 byte) + message_stream_id (4 bytes LE)
                if data.len() < 11 {
                    return None;
                }

                let timestamp = u32::from_be_bytes([0, data[0], data[1], data[2]]);
                let message_length = u32::from_be_bytes([0, data[3], data[4], data[5]]);
                let message_type_id = data[6];
                let message_stream_id = u32::from_le_bytes([data[7], data[8], data[9], data[10]]);

                // Handle extended timestamp
                let (final_timestamp, header_len) = if timestamp == 0xFFFFFF {
                    if data.len() < 15 {
                        return None;
                    }
                    let ext_ts = u32::from_be_bytes([data[11], data[12], data[13], data[14]]);
                    (ext_ts, 15)
                } else {
                    (timestamp, 11)
                };

                Some((ChunkHeader {
                    chunk_type: 0,
                    chunk_stream_id: csid,
                    timestamp: final_timestamp,
                    message_length,
                    message_type_id,
                    message_stream_id,
                }, header_len))
            }
            1 => {
                // Type 1: 7 bytes - timestamp_delta (3) + message_length (3) + message_type_id (1)
                // Inherits message_stream_id from previous chunk
                if data.len() < 7 {
                    return None;
                }

                let timestamp_delta = u32::from_be_bytes([0, data[0], data[1], data[2]]);
                let message_length = u32::from_be_bytes([0, data[3], data[4], data[5]]);
                let message_type_id = data[6];

                // Handle extended timestamp
                let (final_delta, header_len) = if timestamp_delta == 0xFFFFFF {
                    if data.len() < 11 {
                        return None;
                    }
                    let ext_ts = u32::from_be_bytes([data[7], data[8], data[9], data[10]]);
                    (ext_ts, 11)
                } else {
                    (timestamp_delta, 7)
                };

                Some((ChunkHeader {
                    chunk_type: 1,
                    chunk_stream_id: csid,
                    timestamp: state.timestamp.wrapping_add(final_delta),
                    message_length,
                    message_type_id,
                    message_stream_id: state.message_stream_id,
                }, header_len))
            }
            2 => {
                // Type 2: 3 bytes - timestamp_delta only
                // Inherits message_length, message_type_id, message_stream_id
                if data.len() < 3 {
                    return None;
                }

                let timestamp_delta = u32::from_be_bytes([0, data[0], data[1], data[2]]);

                // Handle extended timestamp
                let (final_delta, header_len) = if timestamp_delta == 0xFFFFFF {
                    if data.len() < 7 {
                        return None;
                    }
                    let ext_ts = u32::from_be_bytes([data[3], data[4], data[5], data[6]]);
                    (ext_ts, 7)
                } else {
                    (timestamp_delta, 3)
                };

                Some((ChunkHeader {
                    chunk_type: 2,
                    chunk_stream_id: csid,
                    timestamp: state.timestamp.wrapping_add(final_delta),
                    message_length: state.message_length,
                    message_type_id: state.message_type_id,
                    message_stream_id: state.message_stream_id,
                }, header_len))
            }
            3 => {
                // Type 3: No message header - 0 bytes
                // Inherits everything from previous chunk
                // Note: Extended timestamp may still be present if previous chunk had one
                let header_len = if state.has_extended_timestamp {
                    if data.len() < 4 {
                        return None;
                    }
                    4 // Extended timestamp is repeated
                } else {
                    0
                };

                Some((ChunkHeader {
                    chunk_type: 3,
                    chunk_stream_id: csid,
                    timestamp: state.timestamp,
                    message_length: state.message_length,
                    message_type_id: state.message_type_id,
                    message_stream_id: state.message_stream_id,
                }, header_len))
            }
            _ => None,
        }
    }

    /// Clear all chunk stream state (useful when resetting connection)
    pub fn clear_state(&mut self) {
        self.chunk_states.clear();
        self.buffer.clear();
    }
}

/// Create a chunk header for outbound messages.
/// Returns the serialized header bytes.
pub fn create_chunk_header(
    fmt: u8,
    csid: u32,
    timestamp: u32,
    message_length: u32,
    message_type_id: u8,
    message_stream_id: u32,
) -> Vec<u8> {
    let mut header = Vec::with_capacity(18);

    // Basic header
    let fmt_bits = (fmt & 0x03) << 6;
    if csid < 64 {
        header.push(fmt_bits | (csid as u8));
    } else if csid < 320 {
        header.push(fmt_bits | 0);
        header.push((csid - 64) as u8);
    } else {
        header.push(fmt_bits | 1);
        let csid_offset = csid - 64;
        header.push((csid_offset & 0xFF) as u8);
        header.push(((csid_offset >> 8) & 0xFF) as u8);
    }

    // Message header based on format type
    let use_extended_timestamp = timestamp >= 0xFFFFFF;
    let ts_bytes = if use_extended_timestamp { 0xFFFFFFu32 } else { timestamp };

    match fmt {
        0 => {
            // Type 0: timestamp (3) + length (3) + type (1) + stream_id (4 LE)
            header.extend_from_slice(&ts_bytes.to_be_bytes()[1..4]);
            header.extend_from_slice(&message_length.to_be_bytes()[1..4]);
            header.push(message_type_id);
            header.extend_from_slice(&message_stream_id.to_le_bytes());
        }
        1 => {
            // Type 1: timestamp_delta (3) + length (3) + type (1)
            header.extend_from_slice(&ts_bytes.to_be_bytes()[1..4]);
            header.extend_from_slice(&message_length.to_be_bytes()[1..4]);
            header.push(message_type_id);
        }
        2 => {
            // Type 2: timestamp_delta (3)
            header.extend_from_slice(&ts_bytes.to_be_bytes()[1..4]);
        }
        3 => {
            // Type 3: no message header
        }
        _ => {}
    }

    // Extended timestamp if needed
    if use_extended_timestamp && fmt < 3 {
        header.extend_from_slice(&timestamp.to_be_bytes());
    }

    header
}

/// ChunkWriter handles splitting outbound messages into properly sized chunks.
/// Tracks state per chunk stream for using abbreviated headers (types 1/2/3).
pub struct ChunkWriter {
    chunk_size: u32,
    /// State per chunk stream for abbreviated headers
    chunk_states: HashMap<u32, ChunkWriterState>,
}

#[derive(Default, Clone)]
struct ChunkWriterState {
    last_timestamp: u32,
    last_message_length: u32,
    last_message_type_id: u8,
    last_message_stream_id: u32,
}

impl Default for ChunkWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkWriter {
    pub fn new() -> Self {
        Self {
            chunk_size: 4096, // Default outbound chunk size
            chunk_states: HashMap::new(),
        }
    }

    /// Set the chunk size for outbound messages
    pub fn set_chunk_size(&mut self, size: u32) {
        self.chunk_size = size.min(MAX_CHUNK_SIZE).max(1);
    }

    /// Get the current chunk size
    pub fn chunk_size(&self) -> u32 {
        self.chunk_size
    }

    /// Serialize a message into one or more RTMP chunks.
    /// Handles splitting large messages and uses appropriate header formats.
    pub fn write_message(
        &mut self,
        csid: u32,
        timestamp: u32,
        message_type_id: u8,
        message_stream_id: u32,
        data: &[u8],
    ) -> Vec<u8> {
        let message_length = data.len() as u32;
        let state = self.chunk_states.get(&csid).cloned().unwrap_or_default();

        // Determine which header format to use based on what changed
        let fmt = self.determine_format(
            &state,
            timestamp,
            message_length,
            message_type_id,
            message_stream_id,
        );

        let mut output = Vec::with_capacity(data.len() + 64);
        let mut offset = 0;

        // First chunk uses the determined format
        let first_chunk_size = (message_length as usize).min(self.chunk_size as usize);
        let first_header = self.create_header(
            fmt,
            csid,
            timestamp,
            message_length,
            message_type_id,
            message_stream_id,
            &state,
        );
        output.extend_from_slice(&first_header);
        output.extend_from_slice(&data[offset..offset + first_chunk_size]);
        offset += first_chunk_size;

        // Continuation chunks use Type 3 headers
        while offset < data.len() {
            let chunk_size = (data.len() - offset).min(self.chunk_size as usize);
            let continuation_header = self.create_basic_header(3, csid);
            output.extend_from_slice(&continuation_header);

            // Extended timestamp is repeated in Type 3 if original had one
            if timestamp >= 0xFFFFFF {
                output.extend_from_slice(&timestamp.to_be_bytes());
            }

            output.extend_from_slice(&data[offset..offset + chunk_size]);
            offset += chunk_size;
        }

        // Update state for this chunk stream
        self.chunk_states.insert(csid, ChunkWriterState {
            last_timestamp: timestamp,
            last_message_length: message_length,
            last_message_type_id: message_type_id,
            last_message_stream_id: message_stream_id,
        });

        output
    }

    /// Determine the best header format based on what changed from the previous message
    fn determine_format(
        &self,
        state: &ChunkWriterState,
        timestamp: u32,
        message_length: u32,
        message_type_id: u8,
        message_stream_id: u32,
    ) -> u8 {
        // No previous state - must use Type 0
        if state.last_message_stream_id == 0 && state.last_timestamp == 0 {
            return 0;
        }

        // Stream ID changed - must use Type 0
        if message_stream_id != state.last_message_stream_id {
            return 0;
        }

        // Everything same except timestamp delta - could use Type 2
        // But for simplicity and reliability, we'll use Type 0 for first message
        // and Type 1 for subsequent messages (this is what most servers do)

        // If message length or type changed, use Type 1
        if message_length != state.last_message_length || message_type_id != state.last_message_type_id {
            return 1;
        }

        // Same length and type, could use Type 2 (just timestamp delta)
        // For reliability, we'll default to Type 1 for media streams
        1
    }

    /// Create the basic header (1-3 bytes) for a chunk
    fn create_basic_header(&self, fmt: u8, csid: u32) -> Vec<u8> {
        let fmt_bits = (fmt & 0x03) << 6;

        if csid < 64 {
            vec![fmt_bits | (csid as u8)]
        } else if csid < 320 {
            vec![fmt_bits | 0, (csid - 64) as u8]
        } else {
            let csid_offset = csid - 64;
            vec![
                fmt_bits | 1,
                (csid_offset & 0xFF) as u8,
                ((csid_offset >> 8) & 0xFF) as u8,
            ]
        }
    }

    /// Create a complete chunk header
    fn create_header(
        &self,
        fmt: u8,
        csid: u32,
        timestamp: u32,
        message_length: u32,
        message_type_id: u8,
        message_stream_id: u32,
        state: &ChunkWriterState,
    ) -> Vec<u8> {
        let mut header = self.create_basic_header(fmt, csid);
        let use_extended_timestamp = timestamp >= 0xFFFFFF;
        let ts_field = if use_extended_timestamp { 0xFFFFFF } else { timestamp };

        match fmt {
            0 => {
                // Type 0: Full header
                header.extend_from_slice(&ts_field.to_be_bytes()[1..4]);
                header.extend_from_slice(&message_length.to_be_bytes()[1..4]);
                header.push(message_type_id);
                header.extend_from_slice(&message_stream_id.to_le_bytes());
            }
            1 => {
                // Type 1: Timestamp delta, length, type
                let delta = timestamp.wrapping_sub(state.last_timestamp);
                let delta_field = if delta >= 0xFFFFFF { 0xFFFFFF } else { delta };
                header.extend_from_slice(&delta_field.to_be_bytes()[1..4]);
                header.extend_from_slice(&message_length.to_be_bytes()[1..4]);
                header.push(message_type_id);

                if delta >= 0xFFFFFF {
                    header.extend_from_slice(&delta.to_be_bytes());
                }
            }
            2 => {
                // Type 2: Timestamp delta only
                let delta = timestamp.wrapping_sub(state.last_timestamp);
                let delta_field = if delta >= 0xFFFFFF { 0xFFFFFF } else { delta };
                header.extend_from_slice(&delta_field.to_be_bytes()[1..4]);

                if delta >= 0xFFFFFF {
                    header.extend_from_slice(&delta.to_be_bytes());
                }
            }
            3 => {
                // Type 3: No header beyond basic
            }
            _ => {}
        }

        // Extended timestamp for Type 0
        if fmt == 0 && use_extended_timestamp {
            header.extend_from_slice(&timestamp.to_be_bytes());
        }

        header
    }

    /// Clear state for all chunk streams
    pub fn clear_state(&mut self) {
        self.chunk_states.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_header_one_byte() {
        // fmt=0, csid=3
        let data = [0x03];
        let (fmt, csid, len) = ChunkReader::parse_basic_header(&data).unwrap();
        assert_eq!(fmt, 0);
        assert_eq!(csid, 3);
        assert_eq!(len, 1);
    }

    #[test]
    fn test_parse_basic_header_one_byte_with_fmt() {
        // fmt=1, csid=4 (0x44 = 01 000100)
        let data = [0x44];
        let (fmt, csid, len) = ChunkReader::parse_basic_header(&data).unwrap();
        assert_eq!(fmt, 1);
        assert_eq!(csid, 4);
        assert_eq!(len, 1);
    }

    #[test]
    fn test_parse_basic_header_two_byte() {
        // fmt=0, csid=0 in first byte means 2-byte form
        // csid = 64 + second_byte
        let data = [0x00, 0x40]; // csid = 64 + 64 = 128
        let (fmt, csid, len) = ChunkReader::parse_basic_header(&data).unwrap();
        assert_eq!(fmt, 0);
        assert_eq!(csid, 128);
        assert_eq!(len, 2);
    }

    #[test]
    fn test_parse_basic_header_three_byte() {
        // fmt=0, csid=1 in first byte means 3-byte form
        // csid = 64 + data[1] + data[2] * 256
        let data = [0x01, 0x00, 0x01]; // csid = 64 + 0 + 256 = 320
        let (fmt, csid, len) = ChunkReader::parse_basic_header(&data).unwrap();
        assert_eq!(fmt, 0);
        assert_eq!(csid, 320);
        assert_eq!(len, 3);
    }

    #[test]
    fn test_parse_basic_header_insufficient_data() {
        // 2-byte form but only 1 byte provided
        let data = [0x00];
        assert!(ChunkReader::parse_basic_header(&data).is_none());
    }

    #[test]
    fn test_create_chunk_header_type0() {
        let header = create_chunk_header(0, 3, 1000, 500, RTMP_MSG_VIDEO, 1);

        // Basic header: 1 byte (fmt=0, csid=3)
        assert_eq!(header[0], 0x03);

        // Timestamp: 3 bytes (1000 = 0x0003E8)
        assert_eq!(&header[1..4], &[0x00, 0x03, 0xE8]);

        // Message length: 3 bytes (500 = 0x0001F4)
        assert_eq!(&header[4..7], &[0x00, 0x01, 0xF4]);

        // Message type: 1 byte (video = 0x09)
        assert_eq!(header[7], RTMP_MSG_VIDEO);

        // Stream ID: 4 bytes LE (1)
        assert_eq!(&header[8..12], &[0x01, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_create_chunk_header_extended_timestamp() {
        let header = create_chunk_header(0, 3, 0x01000000, 100, RTMP_MSG_AUDIO, 1);

        // Timestamp field should be 0xFFFFFF
        assert_eq!(&header[1..4], &[0xFF, 0xFF, 0xFF]);

        // Extended timestamp should be at the end
        assert_eq!(&header[12..16], &[0x01, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_message_assembler_limits() {
        let mut assembler = MessageAssembler::with_limits(2, 1000);

        // Add two pending messages
        let chunk1 = RtmpChunk::new(0, 1, 0, 100, RTMP_MSG_VIDEO, 1, vec![0u8; 50]);
        let chunk2 = RtmpChunk::new(0, 2, 0, 100, RTMP_MSG_VIDEO, 1, vec![0u8; 50]);

        assert!(assembler.add_chunk(1, &chunk1).is_ok());
        assert!(assembler.add_chunk(2, &chunk2).is_ok());

        // Third should fail (exceeds limit)
        let chunk3 = RtmpChunk::new(0, 3, 0, 100, RTMP_MSG_VIDEO, 1, vec![0u8; 50]);
        assert!(assembler.add_chunk(3, &chunk3).is_err());
    }

    #[test]
    fn test_message_assembler_rejects_oversized() {
        let mut assembler = MessageAssembler::with_limits(256, 1000);

        // Create chunk claiming to be very large
        let chunk = RtmpChunk::new(0, 1, 0, 2000, RTMP_MSG_VIDEO, 1, vec![0u8; 50]);

        let result = assembler.add_chunk(1, &chunk);
        assert!(result.is_err());
    }

    #[test]
    fn test_message_assembler_complete_message() {
        let mut assembler = MessageAssembler::new();

        // Create a complete message in one chunk
        let chunk = RtmpChunk::new(0, 1, 1000, 10, RTMP_MSG_VIDEO, 1, vec![1u8; 10]);

        let result = assembler.add_chunk(1, &chunk).unwrap();
        assert!(result.is_some());

        let msg = result.unwrap();
        assert_eq!(msg.timestamp, 1000);
        assert_eq!(msg.message_type_id, RTMP_MSG_VIDEO);
        assert_eq!(msg.data.len(), 10);
    }

    #[test]
    fn test_message_assembler_multi_chunk() {
        let mut assembler = MessageAssembler::new();

        // First chunk (incomplete)
        let chunk1 = RtmpChunk::new(0, 1, 1000, 20, RTMP_MSG_VIDEO, 1, vec![1u8; 10]);
        let result1 = assembler.add_chunk(1, &chunk1).unwrap();
        assert!(result1.is_none());

        // Second chunk (completes message)
        let chunk2 = RtmpChunk::new(3, 1, 1000, 20, RTMP_MSG_VIDEO, 1, vec![2u8; 10]);
        let result2 = assembler.add_chunk(1, &chunk2).unwrap();
        assert!(result2.is_some());

        let msg = result2.unwrap();
        assert_eq!(msg.data.len(), 20);
        assert_eq!(&msg.data[0..10], &[1u8; 10]);
        assert_eq!(&msg.data[10..20], &[2u8; 10]);
    }

    #[test]
    fn test_chunk_writer_small_message() {
        let mut writer = ChunkWriter::new();
        writer.set_chunk_size(128);

        let data = vec![0xABu8; 50]; // Small message, fits in one chunk
        let output = writer.write_message(6, 1000, RTMP_MSG_VIDEO, 1, &data);

        // Should have Type 0 header + 50 bytes data
        // Basic header (1 byte, csid=6) + timestamp (3) + length (3) + type (1) + stream_id (4) = 12 bytes
        assert_eq!(output.len(), 12 + 50);

        // Check basic header: fmt=0, csid=6
        assert_eq!(output[0], 0x06);

        // Check data
        assert_eq!(&output[12..], &data[..]);
    }

    #[test]
    fn test_chunk_writer_splits_large_message() {
        let mut writer = ChunkWriter::new();
        writer.set_chunk_size(128);

        let data = vec![0xCDu8; 300]; // Large message, needs 3 chunks (128 + 128 + 44)
        let output = writer.write_message(6, 1000, RTMP_MSG_VIDEO, 1, &data);

        // First chunk: 12 byte header + 128 bytes data
        // Second chunk: 1 byte Type 3 header + 128 bytes data
        // Third chunk: 1 byte Type 3 header + 44 bytes data
        // Total: 12 + 128 + 1 + 128 + 1 + 44 = 314 bytes
        assert_eq!(output.len(), 314);

        // Verify first chunk header (Type 0)
        assert_eq!(output[0] & 0xC0, 0x00); // fmt = 0
        assert_eq!(output[0] & 0x3F, 6);    // csid = 6

        // Verify continuation chunk headers (Type 3)
        assert_eq!(output[12 + 128] & 0xC0, 0xC0); // fmt = 3
        assert_eq!(output[12 + 128 + 1 + 128] & 0xC0, 0xC0); // fmt = 3
    }

    #[test]
    fn test_chunk_writer_preserves_data() {
        let mut writer = ChunkWriter::new();
        writer.set_chunk_size(64);

        let data: Vec<u8> = (0..200).collect();
        let output = writer.write_message(4, 0, RTMP_MSG_AUDIO, 1, &data);

        // Extract data from chunks
        let mut extracted = Vec::new();

        // First chunk: 12 byte Type 0 header + 64 bytes
        extracted.extend_from_slice(&output[12..12 + 64]);

        // Second chunk: 1 byte Type 3 header + 64 bytes
        extracted.extend_from_slice(&output[12 + 64 + 1..12 + 64 + 1 + 64]);

        // Third chunk: 1 byte Type 3 header + 64 bytes
        extracted.extend_from_slice(&output[12 + 64 + 1 + 64 + 1..12 + 64 + 1 + 64 + 1 + 64]);

        // Fourth chunk: 1 byte Type 3 header + 8 bytes
        extracted.extend_from_slice(&output[12 + 64 + 1 + 64 + 1 + 64 + 1..]);

        assert_eq!(extracted, data);
    }

    #[test]
    fn test_chunk_writer_uses_type1_for_subsequent() {
        let mut writer = ChunkWriter::new();
        writer.set_chunk_size(128);

        // First message - uses Type 0
        let data1 = vec![0xAAu8; 50];
        let output1 = writer.write_message(6, 1000, RTMP_MSG_VIDEO, 1, &data1);
        assert_eq!(output1[0] & 0xC0, 0x00); // Type 0

        // Second message on same stream - uses Type 1
        let data2 = vec![0xBBu8; 60];
        let output2 = writer.write_message(6, 1033, RTMP_MSG_VIDEO, 1, &data2);
        assert_eq!(output2[0] & 0xC0, 0x40); // Type 1 (0x40 = 01xxxxxx)

        // Type 1 header: basic (1) + timestamp_delta (3) + length (3) + type (1) = 8 bytes
        assert_eq!(output2.len(), 8 + 60);
    }

    #[test]
    fn test_chunk_writer_extended_timestamp() {
        let mut writer = ChunkWriter::new();
        writer.set_chunk_size(128);

        let data = vec![0xEEu8; 50];
        let timestamp = 0x01000000; // Requires extended timestamp
        let output = writer.write_message(6, timestamp, RTMP_MSG_VIDEO, 1, &data);

        // Type 0 with extended timestamp: 12 + 4 (ext ts) + 50 = 66 bytes
        assert_eq!(output.len(), 66);

        // Timestamp field should be 0xFFFFFF
        assert_eq!(&output[1..4], &[0xFF, 0xFF, 0xFF]);

        // Extended timestamp at end of header
        assert_eq!(&output[12..16], &[0x01, 0x00, 0x00, 0x00]);
    }
}
