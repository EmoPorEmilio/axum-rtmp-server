# RTMP Multi-Destination Server - Implementation Plan

## Project Overview

Build an RTMP server that receives a single stream from OBS and replicates it to multiple destinations (Twitch, YouTube, TikTok) with exponential backoff retry, authentication, Prometheus metrics, and hot-reload configuration.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [File Structure](#file-structure)
3. [Configuration](#configuration)
4. [Phase 1: Critical Bug Fixes](#phase-1-critical-bug-fixes-1-2-days)
5. [Phase 2: Protocol Correctness](#phase-2-protocol-correctness-3-4-days)
6. [Phase 3: Architecture Refactor](#phase-3-architecture-refactor-5-7-days)
7. [Phase 4: Multi-Destination Support](#phase-4-multi-destination-support-3-4-days)
8. [Phase 5: Testing & Polish](#phase-5-testing--polish-2-3-days)
9. [Testing Strategy](#testing-strategy)
10. [Prometheus Metrics](#prometheus-metrics)
11. [Timeline](#timeline)

---

## Architecture Overview

### System Diagram

```
OBS (Publisher)                      Destinations
    |                                    |
    | 1. connect                         |
    ├──────────────────────────────────>|
    | 2. handshake                       |
    ├<──────────────────────────────────┤
    |                                    |
    | 3. createStream                    |
    ├──────────────────────────────────>|
    |                                    |
    | 4. publish (with stream_key)      |
    ├──────────────────────────────────>|
    |                                    |
    | 5. video/audio chunks              |   DestinationPool
    ├──────────────────────────────────>┤         |
    |                                    |    ┌────┴────┐
    |                                    |    ↓         ↓
    |                                    |  Twitch    YouTube
    |                                    |    ↑         ↑
    |                                    |    └────┬────┘
    |                                    |         |
    | Chunk Replication (broadcast) ────┼─────────┘
```

### Key Differences from Standard RTMP Server

| Standard RTMP Server | Multi-Destination Relay |
|---------------------|------------------------|
| Receives + broadcasts to many play clients | Receives + pushes to few destinations |
| Subscribers pull from broadcast channel | Server actively pushes to destinations |
| Simple pub/sub model | Replication + retry logic |
| Playback clients can disconnect freely | Destination failures need reconnection |

### Core Components

1. **RTMP Server**: Accepts incoming RTMP connections from OBS
2. **Protocol Handler**: Manages handshake, chunk parsing, AMF commands
3. **Stream Registry**: Tracks source stream metadata
4. **Replicator**: Broadcasts chunks to all destinations
5. **Destination Pool**: Manages outgoing RTMP connections to platforms
6. **Retry Policy**: Exponential backoff for failed destinations
7. **Metrics Collector**: Exposes Prometheus metrics
8. **Config Manager**: Loads and hot-reloads configuration

---

## File Structure

```
axum-rtmp-server/
├── Cargo.toml
├── Cargo.lock
├── config.toml                 # Public configuration (committed)
├── secrets.toml                # Sensitive data (gitignored)
├── .gitignore
├── IMPLEMENTATION_PLAN.md      # This document
├── README.md
├── src/
│   ├── main.rs                 # Entry point (~100 lines)
│   ├── lib.rs                  # Module exports
│   ├── error.rs                # Error types
│   ├── config.rs               # Configuration loading and hot-reload
│   │
│   ├── server/
│   │   ├── mod.rs
│   │   ├── listener.rs         # TCP listener, spawn connections
│   │   └── metrics.rs          # HTTP /metrics endpoint
│   │
│   ├── protocol/
│   │   ├── mod.rs
│   │   ├── handshake.rs         # Handshake logic (server & client)
│   │   ├── chunk.rs             # Chunk parsing and reassembly
│   │   ├── message.rs           # MessageType enum, dispatch
│   │   └── ack.rs               # Window ACK tracking
│   │
│   ├── connection/
│   │   ├── mod.rs
│   │   ├── handler.rs           # Source connection handler
│   │   └── state.rs             # ConnectionState, StreamState
│   │
│   ├── amf/
│   │   ├── mod.rs
│   │   ├── codec.rs             # AMF0/AMF3 encoding/decoding
│   │   └── commands.rs          # Command response builders
│   │
│   ├── stream/
│   │   ├── mod.rs
│   │   ├── registry.rs          # Source stream registry
│   │   └── replicator.rs        # Chunk replication to destinations
│   │
│   ├── destination/
│   │   ├── mod.rs
│   │   ├── client.rs            # Outgoing RTMP client
│   │   ├── pool.rs              # Manage all destinations
│   │   └── retry.rs             # Exponential backoff logic
│   │
│   ├── metrics/
│   │   ├── mod.rs
│   │   └── collector.rs         # Prometheus metrics
│   │
│   └── hotreload/
│       ├── mod.rs
│       └── watcher.rs           # Config file watcher
│
├── tests/
│   ├── protocol_test.rs        # Protocol unit tests
│   ├── destination_test.rs     # Destination tests
│   ├── auth_test.rs             # Authentication tests
│   ├── config_test.rs           # Config loading tests
│   └── e2e_test.rs              # End-to-end integration tests
│
└── examples/
    └── config-example.toml      # Example configuration
```

---

## Configuration

### `config.toml` (Committed to git)

```toml
[rtmp]
addr = "0.0.0.0:1935"

[server]
max_connections = 100
timeout_seconds = 30

[http]
addr = "0.0.0.0:3000"

[auth]
allowed_stream_keys = ["obs_source_key", "test_key"]

[destinations]
# Destination names reference keys in secrets.toml
twitch = { enabled = true }
youtube = { enabled = true }
tiktok = { enabled = false }

[retry]
initial_interval_ms = 1000
max_interval_ms = 30000
max_attempts = 7
multiplier = 2.0

[hotreload]
enabled = true
check_interval_ms = 1000
```

### `secrets.toml` (Gitignored)

```toml
[destinations.twitch]
rtmp_url = "rtmp://live.twitch.tv/app"
stream_key = "live_12345678_abcde"

[destinations.youtube]
rtmp_url = "rtmp://a.rtmp.youtube.com/live2"
stream_key = "your-youtube-stream-key"

[destinations.tiktok]
rtmp_url = "rtmp://push.tiktok.com/live"
stream_key = "tiktok-stream-key"
```

### `.gitignore`

```
/target
secrets.toml
*.log
.DS_Store
```

### Configuration Structures

```rust
// config.rs
use serde::Deserialize;
use std::net::SocketAddr;
use std::time::Duration;
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub rtmp: RtmpConfig,
    pub server: ServerConfig,
    pub http: HttpConfig,
    pub auth: AuthConfig,
    pub destinations: DestinationRefs,
    pub retry: RetryConfig,
    pub hotreload: HotReloadConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RtmpConfig {
    pub addr: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub max_connections: usize,
    pub timeout_seconds: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpConfig {
    pub addr: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    pub allowed_stream_keys: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DestinationRefs {
    pub twitch: Option<DestinationRef>,
    pub youtube: Option<DestinationRef>,
    pub tiktok: Option<DestinationRef>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DestinationRef {
    pub enabled: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RetryConfig {
    pub initial_interval_ms: u64,
    pub max_interval_ms: u64,
    pub max_attempts: u32,
    pub multiplier: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HotReloadConfig {
    pub enabled: bool,
    pub check_interval_ms: u64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Secrets {
    pub destinations: HashMap<String, DestinationSecret>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DestinationSecret {
    pub rtmp_url: String,
    pub stream_key: String,
}
```

---

## Phase 1: Critical Bug Fixes (1-2 days)

### Task List

**1.1 Compilation Fixes**
- [ ] Add `use rand::Rng;` at top of main.rs (line 334 uses rand::random)
- [ ] Remove unused `rml_amf0` dependency from Cargo.toml
- [ ] Remove duplicate match arm (lines 669-671): `RTMP_MSG_AMF0_CMD | RTMP_MSG_AMF3_CMD`

**1.2 Logic Errors**
- [ ] Remove duplicate `self.state.is_connected = true` (line 819, keep line 829)
- [ ] Remove duplicate Stream Begin event (lines 914-928 - event sent twice)

**1.3 Memory Management**
- [ ] Add `MAX_BUFFER_SIZE: usize = 10_485_760` (10MB) constant
- [ ] In `read_chunk()`: check `self.buffer.len() < MAX_BUFFER_SIZE` before reading
- [ ] In `handle_connection()`: add 30s idle timeout with `tokio::time::timeout`
- [ ] Clean up `chunk_states` entries: keep only last 64 CSIDs (remove older entries periodically)

**1.4 Basic Unit Tests**
- [ ] Add test module to main.rs
- [ ] Test: `test_parse_connect_params()` - validates AMF connect parsing
- [ ] Test: `test_chunk_header_type_0()` - validates full header parsing
- [ ] Test: `test_chunk_header_type_1_2_3()` - validates abbreviated headers
- [ ] Test: `test_create_rtmp_header()` - validates header generation

### Code Changes Required

```rust
// Add import at top
use rand::Rng;

// Add constants
const MAX_BUFFER_SIZE: usize = 10_485_760;

// In read_chunk(), add buffer check
if self.buffer.len() >= MAX_BUFFER_SIZE {
    return Err(RtmpError::Protocol("Buffer overflow".into()));
}

// In handle_connection(), add timeout
match tokio::time::timeout(Duration::from_secs(30), self.read_chunk()).await {
    Ok(Ok(chunk)) => { /* ... */ }
    Ok(Err(e)) => return Err(e),
    Err(_) => return Err(RtmpError::Timeout("Idle connection".into())),
}

// Add cleanup for chunk_states (call periodically)
if self.chunk_states.len() > 64 {
    // Remove oldest entries
}
```

### Acceptance Criteria

- [ ] `cargo build` succeeds without warnings
- [ ] `cargo test` passes all new tests
- [ ] `cargo clippy` reports no issues
- [ ] Server starts and handles basic RTMP handshake

---

## Phase 2: Protocol Correctness (3-4 days)

### Task List

**2.1 Chunk Reassembly**

Current issue: Multi-chunk messages are processed individually instead of being reassembled.

Solution: Implement `MessageAssembler` to accumulate chunks until complete message is received.

```rust
// protocol/chunk.rs
pub struct MessageAssembler {
    pending: HashMap<u32, PendingMessage>,  // csid -> accumulated data
}

struct PendingMessage {
    data: Vec<u8>,
    bytes_received: u32,
    total_length: u32,
    timestamp: u32,
    message_type_id: u8,
    message_stream_id: u32,
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

    pub fn cleanup(&mut self, max_age: Duration) {
        // Remove stale pending messages
    }
}

pub struct AssembledMessage {
    pub timestamp: u32,
    pub message_type_id: u8,
    pub message_stream_id: u32,
    pub data: Vec<u8>,
}
```

Integration points:
- [ ] Modify `read_chunk()` to return chunks
- [ ] Add `MessageAssembler` to `RtmpConnection`
- [ ] Call `add_chunk()` after reading each chunk
- [ ] Process complete messages from `AssembledMessage`
- [ ] Test: Multi-chunk message spanning 3+ chunks

**2.2 ACK Mechanism**

Current issue: Server doesn't track bytes sent or send ACK messages.

Solution: Implement `AckTracker` and send ACK when window is filled.

```rust
// protocol/ack.rs
pub struct AckTracker {
    window_size: u32,
    bytes_sent: u32,
    bytes_received: u32,
}

impl AckTracker {
    pub fn new(window_size: u32) -> Self {
        Self {
            window_size,
            bytes_sent: 0,
            bytes_received: 0,
        }
    }

    pub fn record_sent(&mut self, bytes: u32) -> bool {
        self.bytes_sent += bytes;
        self.bytes_sent >= self.window_size
    }

    pub fn record_received(&mut self, bytes: u32) -> bool {
        self.bytes_received += bytes;
        self.bytes_received >= self.window_size
    }

    pub fn get_ack_value(&self) -> u32 {
        self.bytes_sent
    }

    pub fn reset(&mut self) {
        self.bytes_sent = 0;
        self.bytes_received = 0;
    }
}
```

Integration points:
- [ ] Add `AckTracker` to `RtmpConnection`
- [ ] After each write, check `record_sent()` and send ACK if true
- [ ] When receiving ACK, update `bytes_received`
- [ ] Handle extended timestamp (0xFFFFFF)
- [ ] Test: Verify ACK sent after window_size bytes

**2.3 Reverse Handshake (for Destinations)**

Client needs to initiate handshake as an RTMP client (not server).

```rust
// protocol/handshake.rs
pub async fn perform_server_handshake(socket: &mut TcpStream) -> Result<(), RtmpError> {
    // Read C0+C1
    let mut c0c1 = [0u8; 1537];
    socket.read_exact(&mut c0c1).await?;

    // Verify version
    if c0c1[0] != RTMP_VERSION {
        return Err(RtmpError::Protocol(format!("Unsupported version: {}", c0c1[0])));
    }

    // Send S0+S1+S2
    let mut response = Vec::with_capacity(3073);
    response.push(RTMP_VERSION);

    // S1
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    response.extend_from_slice(&time.to_be_bytes());
    response.extend_from_slice(&[0u8; 4]);

    let random: Vec<u8> = (0..1528).map(|_| rand::random()).collect();
    response.extend_from_slice(&random);

    // S2 (echo C1)
    response.extend_from_slice(&c0c1[1..1537]);
    socket.write_all(&response).await?;

    // Read C2
    let mut c2 = [0u8; 1536];
    socket.read_exact(&mut c2).await?;

    Ok(())
}

pub async fn perform_client_handshake(socket: &mut TcpStream) -> Result<(), RtmpError> {
    // Send C0+C1
    let mut c0c1 = Vec::with_capacity(1537);
    c0c1.push(RTMP_VERSION);

    // C1
    let time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as u32;
    c0c1.extend_from_slice(&time.to_be_bytes());
    c0c1.extend_from_slice(&[0u8; 4]);

    let random: Vec<u8> = (0..1528).map(|_| rand::random()).collect();
    c0c1.extend_from_slice(&random);
    socket.write_all(&c0c1).await?;

    // Read S0+S1+S2
    let mut s0s1s2 = [0u8; 3073];
    socket.read_exact(&mut s0s1s2).await?;

    // Verify version
    if s0s1s2[0] != RTMP_VERSION {
        return Err(RtmpError::Protocol(format!("Unsupported version: {}", s0s1s2[0])));
    }

    // Send C2 (echo S1)
    let c2 = &s0s1s2[1..1537];
    socket.write_all(c2).await?;

    Ok(())
}
```

- [ ] Extract existing handshake to `perform_server_handshake()`
- [ ] Implement `perform_client_handshake()` for destinations
- [ ] Test with mock RTMP server

**2.4 AMF Parser Refinement**

Keep handrolled parser for simplicity (rml_amf0 has licensing complexity), but clean up duplicates.

- [ ] Unify `parse_connect_params` and `parse_app_name_from_connect` into single function
- [ ] Extract AMF marker constants
- [ ] Add comprehensive AMF parsing tests
- [ ] Handle edge cases: empty strings, null values, nested objects

### Tests for Phase 2

```rust
#[test]
fn test_multi_chunk_reassembly() {
    let mut assembler = MessageAssembler::new();

    // Create 3 chunks for a 300KB message (default chunk 128KB)
    let chunks = create_test_chunks(3, 300_000);

    let results = chunks.iter().enumerate().map(|(i, chunk)| {
        assembler.add_chunk(i as u32, chunk).unwrap()
    }).collect::<Vec<_>>();

    assert_eq!(results[0], None);  // First chunk incomplete
    assert_eq!(results[1], None);  // Second chunk incomplete
    assert!(results[2].is_some()); // Third chunk completes

    let complete = results[2].unwrap().unwrap();
    assert_eq!(complete.data.len(), 300_000);
}

#[test]
fn test_ack_tracking() {
    let mut tracker = AckTracker::new(1000);

    assert!(!tracker.record_sent(500));  // 500/1000
    assert!(tracker.record_sent(500));  // 1000/1000 -> send ACK

    tracker.reset();
    assert!(!tracker.record_sent(999));  // 999/1000
    assert!(tracker.record_sent(10));   // 1009/1000 -> send ACK
}

#[test]
fn test_reverse_handshake() {
    // Use mock TCP stream to test client handshake
}
```

### Acceptance Criteria

- [ ] OBS can publish stream successfully
- [ ] Multi-chunk messages handled correctly
- [ ] ACK messages sent at correct intervals
- [ ] Client handshake works for destination connections
- [ ] All unit tests pass

---

## Phase 3: Architecture Refactor (5-7 days)

### Task Breakdown

**3.1 Error Module (`src/error.rs`)**

```rust
use thiserror::Error;

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

    #[error("Destination {dest} failed: {reason}")]
    DestinationFailed { dest: String, reason: String },

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Authentication failed: {0}")]
    AuthFailed(String),

    #[error("Channel error: {0}")]
    Channel(String),
}
```

- [ ] Create `error.rs` with comprehensive error types
- [ ] Implement `From` traits for common error conversions

**3.2 Config Module (`src/config.rs`)**

```rust
use serde::Deserialize;
use std::fs;
use std::path::Path;
use std::net::SocketAddr;
use std::time::Duration;
use std::collections::HashMap;

// Config structs (shown earlier in Configuration section)

impl Config {
    pub fn load<P: AsRef<Path>>(config_path: P) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(config_path)?;
        let config: Config = toml::from_str(&content)?;
        Ok(config)
    }

    pub fn load_with_secrets<P: AsRef<Path>>(
        config_path: P,
        secrets_path: P,
    ) -> Result<FullConfig, Box<dyn std::error::Error>> {
        let config = Self::load(config_path)?;
        let secrets_content = fs::read_to_string(secrets_path)?;
        let secrets: Secrets = toml::from_str(&secrets_content)?;

        Ok(FullConfig { config, secrets })
    }
}

impl RetryConfig {
    pub fn get_delay(&self, attempt: u32) -> Duration {
        if attempt >= self.max_attempts {
            return Duration::MAX; // Give up
        }

        let delay_ms = self.initial_interval_ms as f64
            * self.multiplier.powi(attempt as i32);
        let delay_ms = delay_ms.min(self.max_interval_ms as f64);
        Duration::from_millis(delay_ms as u64)
    }
}
```

- [ ] Create `config.rs` with all config structs
- [ ] Implement `load()` for TOML parsing
- [ ] Implement `load_with_secrets()` for merging config + secrets
- [ ] Add retry delay calculation

**3.3 Protocol Modules**

`src/protocol/mod.rs`:
```rust
pub mod handshake;
pub mod chunk;
pub mod message;
pub mod ack;

pub use handshake::{perform_server_handshake, perform_client_handshake};
pub use chunk::{RtmpChunk, ChunkHeader, MessageAssembler, AssembledMessage};
pub use message::{MessageType, dispatch_message};
pub use ack::{AckTracker};
```

`src/protocol/handshake.rs` (already designed in Phase 2)

`src/protocol/chunk.rs`:
```rust
use bytes::BytesMut;

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

#[derive(Debug, Clone)]
pub struct ChunkHeader {
    pub chunk_type: u8,
    pub chunk_stream_id: u32,
    pub timestamp: u32,
    pub message_length: u32,
    pub message_type_id: u8,
    pub message_stream_id: u32,
}

pub fn parse_header(buffer: &[u8]) -> Result<(ChunkHeader, usize), RtmpError> {
    // Extract header parsing logic from main.rs
}

// MessageAssembler (already designed in Phase 2)
// AssembledMessage (already designed in Phase 2)
```

`src/protocol/message.rs`:
```rust
pub enum MessageType {
    WindowAckSize(u32),
    SetPeerBandwidth(u32, u8),
    SetChunkSize(u32),
    UserControl(UserControlEvent),
    Amf0Command(AmfCommand),
    Amf0Data(Vec<u8>),
    Audio(Vec<u8>),
    Video(Vec<u8>),
}

pub enum UserControlEvent {
    StreamBegin(u32),
    StreamEof(u32),
    StreamDry(u32),
    SetBufferLength(u32, u32),
    StreamIsRecorded(u32),
    PingRequest(u32),
    PingResponse(u32),
}

pub enum AmfCommand {
    Connect { transaction_id: f64, params: Vec<AmfValue> },
    CreateStream { transaction_id: f64 },
    Publish { transaction_id: f64, stream_name: String },
    Play { transaction_id: f64, stream_name: String },
    ReleaseStream { transaction_id: f64, stream_name: String },
    FCPublish { transaction_id: f64, stream_name: String },
}

pub fn dispatch_message(
    chunk: &AssembledMessage,
) -> Result<MessageType, RtmpError> {
    match chunk.message_type_id {
        RTMP_MSG_WINDOW_ACK_SIZE => { /* ... */ }
        RTMP_MSG_AMF0_CMD => { /* ... */ }
        // ... etc
    }
}
```

`src/protocol/ack.rs` (already designed in Phase 2)

**3.4 AMF Module**

`src/amf/mod.rs`:
```rust
pub mod codec;
pub mod commands;

pub use codec::{AmfValue, Amf0Codec};
pub use commands::*;
```

`src/amf/codec.rs`:
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum AmfValue {
    Number(f64),
    Boolean(bool),
    String(String),
    Object(Vec<(String, AmfValue)>),
    EcmaArray(Vec<(String, AmfValue)>),
    StrictArray(Vec<AmfValue>),
    Date(f64, Option<f16>),
    Null,
    Undefined,
}

pub struct Amf0Codec;

impl Amf0Codec {
    pub fn encode(&self, value: &AmfValue) -> Vec<u8> {
        match value {
            AmfValue::Number(n) => {
                let mut buf = vec![0x00];
                buf.extend_from_slice(&n.to_be_bytes());
                buf
            }
            AmfValue::String(s) => {
                let mut buf = vec![0x02];
                buf.extend_from_slice(&(s.len() as u16).to_be_bytes());
                buf.extend_from_slice(s.as_bytes());
                buf
            }
            AmfValue::Null => vec![0x05],
            // ... other types
        }
    }

    pub fn decode(&self, data: &[u8]) -> Result<(AmfValue, usize), RtmpError> {
        if data.is_empty() {
            return Err(RtmpError::InvalidAmf("Empty data".into()));
        }

        let marker = data[0];
        let mut pos = 1;

        let value = match marker {
            0x00 => {
                let bytes: [u8; 8] = data[pos..pos+8].try_into().unwrap();
                AmfValue::Number(f64::from_be_bytes(bytes))
            }
            0x01 => AmfValue::Boolean(data[pos] != 0),
            0x02 => {
                let len = u16::from_be_bytes([data[pos], data[pos+1]]) as usize;
                pos += 2;
                let s = String::from_utf8_lossy(&data[pos..pos+len]).to_string();
                pos += len;
                AmfValue::String(s)
            }
            0x03 => {
                // Object
                let mut props = Vec::new();
                loop {
                    if data.len() < pos + 3 {
                        break;
                    }
                    if data[pos] == 0 && data[pos+1] == 0 && data[pos+2] == 0x09 {
                        pos += 3;
                        break;
                    }
                    let name_len = u16::from_be_bytes([data[pos], data[pos+1]]) as usize;
                    pos += 2;
                    let name = String::from_utf8_lossy(&data[pos..pos+name_len]).to_string();
                    pos += name_len;
                    let (value, consumed) = self.decode(&data[pos..])?;
                    pos += consumed;
                    props.push((name, value));
                }
                AmfValue::Object(props)
            }
            0x05 => AmfValue::Null,
            _ => return Err(RtmpError::InvalidAmf(format!("Unknown marker: 0x{:02X}", marker))),
        };

        Ok((value, pos))
    }
}
```

`src/amf/commands.rs`:
```rust
use super::codec::AmfValue;

pub fn create_connect_response(transaction_id: f64) -> Vec<u8> {
    let codec = Amf0Codec;

    let mut response = Vec::new();

    // Command name
    response.extend_from_slice(&codec.encode(&AmfValue::String("_result".into())));

    // Transaction ID
    response.extend_from_slice(&codec.encode(&AmfValue::Number(transaction_id)));

    // Result object
    let mut props = vec![
        ("level".into(), AmfValue::String("status".into())),
        ("code".into(), AmfValue::String("NetConnection.Connect.Success".into())),
        ("description".into(), AmfValue::String("Connection succeeded.".into())),
    ];

    // Object end marker
    response.push(0x03);
    for (name, value) in props {
        response.extend_from_slice(&codec.encode(&AmfValue::String(name)));
        response.extend_from_slice(&codec.encode(&value));
    }
    response.extend_from_slice(&[0x00, 0x00, 0x09]);

    response
}

pub fn create_publish_response() -> Vec<u8> {
    let codec = Amf0Codec;
    let mut response = Vec::new();

    response.extend_from_slice(&codec.encode(&AmfValue::String("onStatus".into())));
    response.extend_from_slice(&codec.encode(&AmfValue::Number(0.0)));
    response.extend_from_slice(&codec.encode(&AmfValue::Null));

    let mut info = vec![
        ("level".into(), AmfValue::String("status".into())),
        ("code".into(), AmfValue::String("NetStream.Publish.Start".into())),
        ("description".into(), AmfValue::String("Stream is now published.".into())),
    ];

    response.push(0x03);
    for (name, value) in info {
        response.extend_from_slice(&codec.encode(&AmfValue::String(name)));
        response.extend_from_slice(&codec.encode(&value));
    }
    response.extend_from_slice(&[0x00, 0x00, 0x09]);

    response
}

pub fn create_reject_response(reason: &str) -> Vec<u8> {
    let codec = Amf0Codec;
    let mut response = Vec::new();

    response.extend_from_slice(&codec.encode(&AmfValue::String("onStatus".into())));
    response.extend_from_slice(&codec.encode(&AmfValue::Number(0.0)));
    response.extend_from_slice(&codec.encode(&AmfValue::Null));

    let mut info = vec![
        ("level".into(), AmfValue::String("error".into())),
        ("code".into(), AmfValue::String("NetStream.Publish.Rejected".into())),
        ("description".into(), AmfValue::String(reason.into())),
    ];

    response.push(0x03);
    for (name, value) in info {
        response.extend_from_slice(&codec.encode(&AmfValue::String(name)));
        response.extend_from_slice(&codec.encode(&value));
    }
    response.extend_from_slice(&[0x00, 0x00, 0x09]);

    response
}
```

**3.5 Connection Module**

`src/connection/state.rs`:
```rust
#[derive(Debug, Clone, PartialEq)]
pub enum StreamState {
    Initial,
    Connected,
    Publishing,
    Error(String),
}

#[derive(Debug, Clone)]
pub struct ConnectionState {
    pub transaction_id: f64,
    pub stream_id: u32,
    pub is_connected: bool,
    pub app_name: Option<String>,
    pub stream_key: Option<String>,
    pub stream_state: StreamState,
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
```

`src/connection/handler.rs`:
```rust
use crate::protocol::*;
use crate::amf::*;
use crate::config::Config;
use crate::error::RtmpError;
use bytes::BytesMut;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct Handler {
    socket: TcpStream,
    buffer: BytesMut,
    chunk_size: u32,
    ack_tracker: AckTracker,
    message_assembler: MessageAssembler,
    chunk_states: HashMap<u32, ChunkStreamState>,
    state: ConnectionState,
    config: Arc<Config>,
    replicator: Arc<Replicator>,
    metrics: Arc<MetricsCollector>,
}

#[derive(Debug, Clone, Default)]
struct ChunkStreamState {
    timestamp: u32,
    message_length: u32,
    message_type_id: u8,
    message_stream_id: u32,
    bytes_left: u32,
}

impl Handler {
    pub fn new(
        socket: TcpStream,
        config: Arc<Config>,
        replicator: Arc<Replicator>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            socket,
            buffer: BytesMut::with_capacity(4096),
            chunk_size: 128,
            ack_tracker: AckTracker::new(config.server.window_size),
            message_assembler: MessageAssembler::new(),
            chunk_states: HashMap::new(),
            state: ConnectionState::default(),
            config,
            replicator,
            metrics,
        }
    }

    pub async fn run(&mut self) -> Result<(), RtmpError> {
        perform_server_handshake(&mut self.socket).await?;
        tracing::info!("Handshake completed");

        loop {
            match tokio::time::timeout(Duration::from_secs(30), self.read_chunk()).await {
                Ok(Ok(chunk)) => {
                    if let Some(msg) = self.message_assembler.add_chunk(chunk.chunk_stream_id, &chunk)? {
                        self.process_message(msg).await?;
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => return Err(RtmpError::Timeout("Idle connection".into())),
            }
        }
    }

    async fn read_chunk(&mut self) -> Result<RtmpChunk, RtmpError> {
        // Extract chunk reading logic from main.rs
    }

    async fn process_message(&mut self, msg: AssembledMessage) -> Result<(), RtmpError> {
        self.metrics.bytes_received.inc_by(msg.data.len() as u64);

        match msg.message_type_id {
            RTMP_MSG_AMF0_CMD => self.handle_amf_command(&msg.data).await?,
            RTMP_MSG_VIDEO => {
                if self.state.stream_state == StreamState::Publishing {
                    self.replicator.publish(RtmpChunk {
                        chunk_type: 0,
                        chunk_stream_id: 6, // Video stream
                        timestamp: msg.timestamp,
                        message_length: msg.data.len() as u32,
                        message_type_id: msg.message_type_id,
                        message_stream_id: msg.message_stream_id,
                        data: msg.data.clone(),
                    });
                }
            }
            RTMP_MSG_AUDIO => {
                if self.state.stream_state == StreamState::Publishing {
                    self.replicator.publish(RtmpChunk {
                        chunk_type: 0,
                        chunk_stream_id: 4, // Audio stream
                        timestamp: msg.timestamp,
                        message_length: msg.data.len() as u32,
                        message_type_id: msg.message_type_id,
                        message_stream_id: msg.message_stream_id,
                        data: msg.data.clone(),
                    });
                }
            }
            _ => {}
        }

        Ok(())
    }

    async fn handle_amf_command(&mut self, data: &[u8]) -> Result<(), RtmpError> {
        // Extract AMF command handling from main.rs
    }
}
```

**3.6 Server Module**

`src/server/listener.rs`:
```rust
use crate::config::Config;
use crate::connection::Handler;
use crate::stream::Replicator;
use crate::metrics::MetricsCollector;
use crate::error::RtmpError;
use std::sync::Arc;
use tokio::net::TcpListener;

pub struct RtmpServer {
    listener: TcpListener,
    config: Arc<Config>,
    replicator: Arc<Replicator>,
    metrics: Arc<MetricsCollector>,
}

impl RtmpServer {
    pub fn new(
        listener: TcpListener,
        config: Arc<Config>,
        replicator: Arc<Replicator>,
        metrics: Arc<MetricsCollector>,
    ) -> Self {
        Self {
            listener,
            config,
            replicator,
            metrics,
        }
    }

    pub async fn run(&self) -> Result<(), RtmpError> {
        loop {
            let (socket, addr) = self.listener.accept().await?;
            tracing::info!("New connection from {}", addr);

            let config = self.config.clone();
            let replicator = self.replicator.clone();
            let metrics = self.metrics.clone();

            tokio::spawn(async move {
                let mut handler = Handler::new(socket, config, replicator, metrics);
                if let Err(e) = handler.run().await {
                    tracing::error!("Connection error: {}", e);
                }
            });
        }
    }
}
```

`src/server/metrics.rs`:
```rust
use crate::metrics::MetricsCollector;
use crate::error::RtmpError;
use axum::{
    extract::State,
    routing::get,
    Router,
};
use std::sync::Arc;

pub struct MetricsServer {
    listener: tokio::net::TcpListener,
    metrics: Arc<MetricsCollector>,
}

impl MetricsServer {
    pub fn new(listener: tokio::net::TcpListener, metrics: Arc<MetricsCollector>) -> Self {
        Self { listener, metrics }
    }

    pub async fn run(&self) -> Result<(), RtmpError> {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .route("/", get(|| async { "RTMP Multi-Destination Server" }))
            .with_state(self.metrics.clone());

        axum::serve(self.listener, app).await?;
        Ok(())
    }
}

async fn metrics_handler(State(metrics): State<Arc<MetricsCollector>>) -> String {
    metrics.export()
}
```

**3.7 Stream Module**

`src/stream/registry.rs`:
```rust
use dashmap::DashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct StreamMetadata {
    pub app_name: String,
    pub stream_key: String,
    pub chunk_size: u32,
    pub video_codec: Option<String>,
    pub audio_codec: Option<String>,
}

pub struct StreamRegistry {
    streams: DashMap<String, StreamMetadata>,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self {
            streams: DashMap::new(),
        }
    }

    pub fn register(&self, key: String, metadata: StreamMetadata) {
        self.streams.insert(key, metadata);
    }

    pub fn get(&self, key: &str) -> Option<StreamMetadata> {
        self.streams.get(key).map(|v| v.clone())
    }

    pub fn unregister(&self, key: &str) {
        self.streams.remove(key);
    }
}
```

`src/stream/replicator.rs`:
```rust
use crate::protocol::RtmpChunk;
use crate::destination::DestinationPool;
use crate::error::RtmpError;
use std::sync::Arc;
use tokio::sync::broadcast;

pub struct Replicator {
    destinations: Arc<DestinationPool>,
    tx: broadcast::Sender<RtmpChunk>,
}

impl Replicator {
    pub fn new(destinations: Arc<DestinationPool>) -> Self {
        let (tx, _) = broadcast::channel(1000);
        Self { destinations, tx }
    }

    pub fn publish(&self, chunk: RtmpChunk) {
        let _ = self.tx.send(chunk);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RtmpChunk> {
        self.tx.subscribe()
    }

    pub async fn run(&self) -> Result<(), RtmpError> {
        let mut rx = self.subscribe();
        loop {
            match rx.recv().await {
                Ok(chunk) => {
                    self.destinations.replicate(&chunk).await;
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("Replicator lagged {} chunks", n);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    break;
                }
            }
        }
    }
}
```

**3.8 Destination Module**

`src/destination/retry.rs`:
```rust
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct RetryPolicy {
    initial_interval: Duration,
    max_interval: Duration,
    max_attempts: u32,
    multiplier: f64,
}

impl RetryPolicy {
    pub fn new(
        initial_interval: Duration,
        max_interval: Duration,
        max_attempts: u32,
        multiplier: f64,
    ) -> Self {
        Self {
            initial_interval,
            max_interval,
            max_attempts,
            multiplier,
        }
    }

    pub fn get_delay(&self, attempt: u32) -> Duration {
        if attempt >= self.max_attempts {
            return Duration::MAX;
        }

        let delay_ms = self.initial_interval.as_millis() as f64
            * self.multiplier.powi(attempt as i32);
        let delay_ms = delay_ms.min(self.max_interval.as_millis() as f64);
        Duration::from_millis(delay_ms as u64)
    }

    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }
}
```

`src/destination/client.rs`:
```rust
use crate::protocol::{perform_client_handshake, RtmpChunk};
use crate::amf::{AmfValue, Amf0Codec};
use crate::error::RtmpError;
use crate::metrics::MetricsCollector;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Clone, PartialEq)]
pub enum ClientState {
    Disconnected,
    Connecting,
    Handshaking,
    Publishing,
    Failed(String),
    MaxRetriesExceeded,
}

pub struct RtmpClient {
    name: String,
    url: String,
    stream_key: String,
    socket: Option<TcpStream>,
    state: ClientState,
    retry_count: u32,
    metrics: Arc<MetricsCollector>,
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

        // Send connect command
        let codec = Amf0Codec;
        let mut connect_cmd = Vec::new();
        connect_cmd.extend_from_slice(&codec.encode(&AmfValue::String("connect".into())));
        connect_cmd.extend_from_slice(&codec.encode(&AmfValue::Number(1.0)));
        connect_cmd.extend_from_slice(&codec.encode(&AmfValue::Object(vec![
            ("app".into(), AmfValue::String("live".into())),
        ])));
        self.send_message(0, connect_cmd).await?;

        // Send createStream
        let mut create_stream_cmd = Vec::new();
        create_stream_cmd.extend_from_slice(&codec.encode(&AmfValue::String("createStream".into())));
        create_stream_cmd.extend_from_slice(&codec.encode(&AmfValue::Number(2.0)));
        create_stream_cmd.extend_from_slice(&codec.encode(&AmfValue::Null));
        self.send_message(3, create_stream_cmd).await?;

        // Send publish
        let mut publish_cmd = Vec::new();
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::String("publish".into())));
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::Number(3.0)));
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::Null));
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::String(self.stream_key.clone())));
        publish_cmd.extend_from_slice(&codec.encode(&AmfValue::String("live".into())));
        self.send_message(4, publish_cmd).await?;

        self.state = ClientState::Publishing;
        self.retry_count = 0;

        tracing::info!("Connected to destination: {}", self.name);
        Ok(())
    }

    pub async fn publish(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
        if !self.is_connected() {
            return Err(RtmpError::DestinationFailed {
                dest: self.name.clone(),
                reason: format!("Not connected: {:?}", self.state),
            });
        }

        let header = create_rtmp_header(
            chunk.chunk_type,
            chunk.chunk_stream_id as u8,
            chunk.timestamp,
            chunk.message_length,
            chunk.message_type_id,
            chunk.message_stream_id,
        );

        let socket = self.socket.as_mut().unwrap();
        socket.write_all(&header).await?;
        socket.write_all(&chunk.data).await?;

        Ok(())
    }

    pub fn is_connected(&self) -> bool {
        self.state == ClientState::Publishing
    }

    pub fn mark_failed(&mut self, reason: String) {
        self.state = ClientState::Failed(reason);
        self.socket = None;
    }

    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }

    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }

    async fn send_message(&mut self, chunk_stream_id: u32, data: Vec<u8>) -> Result<(), RtmpError> {
        let header = create_rtmp_header(0, chunk_stream_id as u8, 0, data.len() as u32, 0x14, 0);
        let socket = self.socket.as_mut().unwrap();
        socket.write_all(&header).await?;
        socket.write_all(&data).await?;
        Ok(())
    }
}

fn parse_rtmp_url(url: &str) -> Result<String, RtmpError> {
    // Parse rtmp://host:port/app -> host:port
    let url = url.strip_prefix("rtmp://")
        .ok_or_else(|| RtmpError::Protocol("Invalid RTMP URL".into()))?;

    let parts: Vec<&str> = url.split('/').collect();
    if parts.is_empty() {
        return Err(RtmpError::Protocol("Invalid RTMP URL".into()));
    }

    Ok(parts[0].to_string())
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

    header.push((chunk_type << 6) | chunk_stream_id);

    match chunk_type {
        0 => {
            header.extend_from_slice(&timestamp.to_be_bytes()[1..]);
            header.extend_from_slice(&message_length.to_be_bytes()[1..]);
            header.push(message_type_id);
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
```

`src/destination/pool.rs`:
```rust
use crate::destination::{RtmpClient, ClientState, RetryPolicy};
use crate::protocol::RtmpChunk;
use crate::metrics::MetricsCollector;
use crate::error::RtmpError;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct DestinationPool {
    clients: Vec<Arc<Mutex<RtmpClient>>>,
    retry_policy: RetryPolicy,
    metrics: Arc<MetricsCollector>,
}

impl DestinationPool {
    pub fn new(clients: Vec<Arc<Mutex<RtmpClient>>>, retry_policy: RetryPolicy, metrics: Arc<MetricsCollector>) -> Self {
        Self {
            clients,
            retry_policy,
            metrics,
        }
    }

    pub async fn start_all(&self) -> Result<(), RtmpError> {
        for client in &self.clients {
            let mut c = client.lock().await;
            if c.is_connected() {
                continue;
            }

            if let Err(e) = c.connect().await {
                tracing::warn!("Failed to connect to {}: {}", c.name, e);
                c.mark_failed(e.to_string());
            }
        }
        Ok(())
    }

    pub async fn replicate(&self, chunk: &RtmpChunk) {
        for client in &self.clients {
            let client = client.clone();
            let chunk = chunk.clone();

            tokio::spawn(async move {
                let mut c = client.lock().await;
                if let Err(e) = c.publish(&chunk).await {
                    tracing::warn!("Failed to publish to {}: {}", c.name, e);
                    c.mark_failed(e.to_string());
                }
            });
        }
    }

    pub async fn retry_failed(&self) -> Result<(), RtmpError> {
        for client in &self.clients {
            let mut c = client.lock().await;

            if !c.state.is_failed() {
                continue;
            }

            if !self.retry_policy.should_retry(c.retry_count()) {
                c.mark_failed("Max retries exceeded".into());
                continue;
            }

            let delay = self.retry_policy.get_delay(c.retry_count());
            tokio::time::sleep(delay).await;

            c.increment_retry();
            if let Err(e) = c.connect().await {
                tracing::error!("Retry failed for {}: {}", c.name, e);
            }
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.clients.len()
    }
}
```

**3.9 Metrics Module**

`src/metrics/collector.rs`:
```rust
use prometheus::{IntGauge, IntCounter, IntCounterVec, IntGaugeVec, Registry, TextEncoder};
use std::sync::Arc;

pub struct MetricsCollector {
    connections_active: IntGauge,
    connections_total: IntCounter,
    connections_errors_total: IntCounter,
    streams_published: IntCounter,
    bytes_received: IntCounter,
    bytes_sent: IntCounter,
    chunks_received: IntCounter,
    auth_attempts_total: IntCounter,
    auth_failures_total: IntCounter,
    destinations_connected: IntGaugeVec,
    chunks_replicated_total: IntCounterVec,
    destination_errors_total: IntCounterVec,
    registry: Registry,
}

impl MetricsCollector {
    pub fn new() -> Self {
        let registry = Registry::new();

        let connections_active = IntGauge::new("rtmp_connections_active", "Current active RTMP connections")
            .unwrap();
        let connections_total = IntCounter::new("rtmp_connections_total", "Total RTMP connections")
            .unwrap();
        let connections_errors_total = IntCounter::new("rtmp_connections_errors_total", "Total connection errors")
            .unwrap();
        let streams_published = IntCounter::new("rtmp_streams_published", "Total streams published")
            .unwrap();
        let bytes_received = IntCounter::new("rtmp_bytes_received", "Total bytes received")
            .unwrap();
        let bytes_sent = IntCounter::new("rtmp_bytes_sent", "Total bytes sent")
            .unwrap();
        let chunks_received = IntCounter::new("rtmp_chunks_received", "Total chunks received")
            .unwrap();
        let auth_attempts_total = IntCounter::new("rtmp_auth_attempts_total", "Total auth attempts")
            .unwrap();
        let auth_failures_total = IntCounter::new("rtmp_auth_failures_total", "Total auth failures")
            .unwrap();
        let destinations_connected = IntGaugeVec::new(
            "rtmp_destinations_connected",
            "Current connected destinations",
            &["destination"]
        ).unwrap();
        let chunks_replicated_total = IntCounterVec::new(
            "rtmp_chunks_replicated_total",
            "Chunks replicated to destinations",
            &["destination"]
        ).unwrap();
        let destination_errors_total = IntCounterVec::new(
            "rtmp_destination_errors_total",
            "Destination errors",
            &["destination"]
        ).unwrap();

        registry.register(Box::new(connections_active.clone())).unwrap();
        registry.register(Box::new(connections_total.clone())).unwrap();
        registry.register(Box::new(connections_errors_total.clone())).unwrap();
        registry.register(Box::new(streams_published.clone())).unwrap();
        registry.register(Box::new(bytes_received.clone())).unwrap();
        registry.register(Box::new(bytes_sent.clone())).unwrap();
        registry.register(Box::new(chunks_received.clone())).unwrap();
        registry.register(Box::new(auth_attempts_total.clone())).unwrap();
        registry.register(Box::new(auth_failures_total.clone())).unwrap();
        registry.register(Box::new(destinations_connected.clone())).unwrap();
        registry.register(Box::new(chunks_replicated_total.clone())).unwrap();
        registry.register(Box::new(destination_errors_total.clone())).unwrap();

        Self {
            connections_active,
            connections_total,
            connections_errors_total,
            streams_published,
            bytes_received,
            bytes_sent,
            chunks_received,
            auth_attempts_total,
            auth_failures_total,
            destinations_connected,
            chunks_replicated_total,
            destination_errors_total,
            registry,
        }
    }

    pub fn export(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        match encoder.encode_to_string(&metric_families) {
            Ok(s) => s,
            Err(e) => format!("# Error encoding metrics: {}", e),
        }
    }

    pub fn connections_active(&self) -> &IntGauge { &self.connections_active }
    pub fn connections_total(&self) -> &IntCounter { &self.connections_total }
    pub fn streams_published(&self) -> &IntCounter { &self.streams_published }
    pub fn bytes_received(&self) -> &IntCounter { &self.bytes_received }
    pub fn bytes_sent(&self) -> &IntCounter { &self.bytes_sent }
    pub fn chunks_received(&self) -> &IntCounter { &self.chunks_received }
    pub fn auth_attempts_total(&self) -> &IntCounter { &self.auth_attempts_total }
    pub fn auth_failures_total(&self) -> &IntCounter { &self.auth_failures_total }
    pub fn destinations_connected(&self) -> &IntGaugeVec { &self.destinations_connected }
    pub fn chunks_replicated_total(&self) -> &IntCounterVec { &self.chunks_replicated_total }
    pub fn destination_errors_total(&self) -> &IntCounterVec { &self.destination_errors_total }
}
```

**3.10 Hot Reload Module**

`src/hotreload/watcher.rs`:
```rust
use crate::config::{Config, Secrets};
use notify::{Watcher, RecursiveMode, watcher};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct ConfigChange {
    pub config: Config,
    pub secrets: Secrets,
}

pub struct ConfigWatcher {
    config_path: String,
    secrets_path: String,
    tx: broadcast::Sender<ConfigChange>,
}

impl ConfigWatcher {
    pub fn new(config_path: String, secrets_path: String) -> Self {
        let (tx, _) = broadcast::channel(10);

        Self {
            config_path,
            secrets_path,
            tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ConfigChange> {
        self.tx.subscribe()
    }

    pub async fn spawn(self) -> Result<(), Box<dyn std::error::Error>> {
        let mut watcher = watcher(move |res| {
            if let Ok(event) = res {
                tracing::debug!("Config file changed: {:?}", event);
            }
        }, Duration::from_millis(1000))?;

        watcher.watch(&self.config_path, RecursiveMode::NonRecursive)?;
        watcher.watch(&self.secrets_path, RecursiveMode::NonRecursive)?;

        let tx = self.tx;
        let config_path = self.config_path;
        let secrets_path = self.secrets_path;

        tokio::spawn(async move {
            let mut last_modified_config = std::fs::metadata(&config_path).ok().and_then(|m| m.modified().ok());
            let mut last_modified_secrets = std::fs::metadata(&secrets_path).ok().and_then(|m| m.modified().ok());

            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;

                let config_modified = std::fs::metadata(&config_path)
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .filter(|t| last_modified_config.map(|l| t > &l).unwrap_or(true));

                let secrets_modified = std::fs::metadata(&secrets_path)
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .filter(|t| last_modified_secrets.map(|l| t > &l).unwrap_or(true));

                if config_modified.is_some() || secrets_modified.is_some() {
                    if let Ok(config) = Config::load(&config_path) {
                        if let Ok(secrets) = toml::from_str::<Secrets>(
                            &std::fs::read_to_string(&secrets_path)?
                        ) {
                            tx.send(ConfigChange { config, secrets }).ok();
                            tracing::info!("Configuration reloaded");
                        }
                    }

                    if let Some(m) = std::fs::metadata(&config_path).ok().and_then(|m| m.modified().ok()) {
                        last_modified_config = Some(m);
                    }
                    if let Some(m) = std::fs::metadata(&secrets_path).ok().and_then(|m| m.modified().ok()) {
                        last_modified_secrets = Some(m);
                    }
                }
            }
        });

        Ok(())
    }
}
```

**3.11 Main Refactor**

`src/main.rs`:
```rust
use axum_rtmp_server::{
    config::Config,
    server::{RtmpServer, MetricsServer},
    destination::{DestinationPool, RtmpClient, RetryPolicy},
    stream::Replicator,
    metrics::MetricsCollector,
    hotreload::ConfigWatcher,
};
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .init();

    let full_config = Config::load_with_secrets("config.toml", "secrets.toml")?;
    let config = Arc::new(full_config.config);
    let secrets = full_config.secrets;

    let metrics = Arc::new(MetricsCollector::new());

    // Create destination clients
    let mut clients = Vec::new();

    if let Some(twitch) = &config.destinations.twitch {
        if twitch.enabled {
            if let Some(secret) = secrets.destinations.get("twitch") {
                clients.push(Arc::new(tokio::sync::Mutex::new(
                    RtmpClient::new(
                        "twitch".into(),
                        secret.rtmp_url.clone(),
                        secret.stream_key.clone(),
                        metrics.clone(),
                    )
                )));
            }
        }
    }

    if let Some(youtube) = &config.destinations.youtube {
        if youtube.enabled {
            if let Some(secret) = secrets.destinations.get("youtube") {
                clients.push(Arc::new(tokio::sync::Mutex::new(
                    RtmpClient::new(
                        "youtube".into(),
                        secret.rtmp_url.clone(),
                        secret.stream_key.clone(),
                        metrics.clone(),
                    )
                )));
            }
        }
    }

    if let Some(tiktok) = &config.destinations.tiktok {
        if tiktok.enabled {
            if let Some(secret) = secrets.destinations.get("tiktok") {
                clients.push(Arc::new(tokio::sync::Mutex::new(
                    RtmpClient::new(
                        "tiktok".into(),
                        secret.rtmp_url.clone(),
                        secret.stream_key.clone(),
                        metrics.clone(),
                    )
                )));
            }
        }
    }

    let retry_policy = RetryPolicy::new(
        std::time::Duration::from_millis(config.retry.initial_interval_ms),
        std::time::Duration::from_millis(config.retry.max_interval_ms),
        config.retry.max_attempts,
        config.retry.multiplier,
    );

    let destination_pool = Arc::new(DestinationPool::new(clients, retry_policy, metrics.clone()));
    let replicator = Arc::new(Replicator::new(destination_pool.clone()));

    // Start destination pool
    let pool = destination_pool.clone();
    tokio::spawn(async move {
        pool.start_all().await.ok();
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
        loop {
            interval.tick().await;
            pool.retry_failed().await.ok();
        }
    });

    // Start replicator
    let rep = replicator.clone();
    tokio::spawn(async move {
        rep.run().await.ok();
    });

    // Hot reload watcher
    if config.hotreload.enabled {
        let watcher = ConfigWatcher::new("config.toml".into(), "secrets.toml".into());
        watcher.spawn()?;
    }

    let rtmp_addr: std::net::SocketAddr = config.rtmp.addr.parse()?;
    let rtmp_listener = TcpListener::bind(rtmp_addr).await?;
    tracing::info!("RTMP server listening on {}", rtmp_addr);

    let http_addr: std::net::SocketAddr = config.http.addr.parse()?;
    let http_listener = TcpListener::bind(http_addr).await?;
    tracing::info!("HTTP metrics server listening on {}", http_addr);

    let rtmp_server = RtmpServer::new(rtmp_listener, config.clone(), replicator, metrics.clone());
    let metrics_server = MetricsServer::new(http_listener, metrics);

    tokio::select! {
        _ = rtmp_server.run() => {}
        _ = metrics_server.run() => {}
    }

    Ok(())
}
```

`src/lib.rs`:
```rust
pub mod config;
pub mod error;
pub mod server;
pub mod protocol;
pub mod connection;
pub mod amf;
pub mod stream;
pub mod destination;
pub mod metrics;
pub mod hotreload;
```

### Acceptance Criteria for Phase 3

- [ ] All modules compile independently
- [ ] Clean separation of concerns
- [ ] Config loads and merges correctly
- [ ] Metrics exposed at `/metrics`
- [ ] Server starts and accepts connections

---

## Phase 4: Multi-Destination Support (3-4 days)

### Task List

**4.1 Destination Pool Integration**

- [ ] Ensure `DestinationPool::start_all()` connects to all enabled destinations
- [ ] Ensure `DestinationPool::replicate()` sends chunks to all destinations
- [ ] Ensure `DestinationPool::retry_failed()` reconnects to failed destinations
- [ ] Test with mock RTMP server

**4.2 Source Connection Integration**

- [ ] Wire up `Replicator` in `connection::Handler`
- [ ] Send video/audio chunks to replicator when `StreamState::Publishing`
- [ ] Update metrics for chunks replicated

**4.3 Stream Key Authentication**

- [ ] In `handle_publish()`, validate stream key against `config.auth.allowed_stream_keys`
- [ ] On invalid key: send reject response, increment auth failure metric
- [ ] On valid key: proceed with publish

```rust
// In connection/handler.rs
async fn handle_publish(&mut self, data: &[u8]) -> Result<(), RtmpError> {
    if let Some(stream_key) = self.parse_stream_name(data) {
        self.metrics.auth_attempts_total.inc();

        if !self.config.auth.allowed_stream_keys.contains(&stream_key) {
            tracing::warn!("Invalid stream key: {}", stream_key);
            self.metrics.auth_failures_total.inc();

            let response = create_reject_response("Invalid stream key");
            self.send_amf_message(response).await?;
            return Err(RtmpError::AuthFailed("Invalid stream key".into()));
        }

        self.state.stream_key = Some(stream_key);
        self.state.stream_state = StreamState::Publishing;

        // Send success response
        let response = create_publish_response();
        self.send_amf_message(response).await?;
    }
}
```

**4.4 Destination State Tracking**

- [ ] Update metrics gauge `destinations_connected` when destination state changes
- [ ] Update counter `destination_errors_total` on publish failures
- [ ] Update counter `chunks_replicated_total` on successful publish

```rust
// In destination/client.rs
pub async fn connect(&mut self) -> Result<(), RtmpError> {
    // ... connection logic ...

    self.state = ClientState::Publishing;
    self.metrics.destinations_connected
        .with_label_values(&[&self.name])
        .set(1);

    tracing::info!("Connected to destination: {}", self.name);
    Ok(())
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

pub async fn publish(&mut self, chunk: &RtmpChunk) -> Result<(), RtmpError> {
    // ... publish logic ...

    self.metrics.chunks_replicated_total
        .with_label_values(&[&self.name])
        .inc();

    Ok(())
}
```

**4.5 Hot Reload Support**

- [ ] Add `notify` crate to Cargo.toml
- [ ] Implement `ConfigWatcher` (designed in Phase 3)
- [ ] Update destination pool on config change:
  - Add newly enabled destinations
  - Remove disabled destinations
  - Update retry policy

```rust
// In main.rs, add hot reload task
let config_watcher = ConfigWatcher::new("config.toml".into(), "secrets.toml".into());
let mut rx = config_watcher.subscribe();

tokio::spawn(async move {
    while let Ok(change) = rx.recv().await {
        // Update destination pool with new config
        tracing::info!("Configuration changed, updating destination pool");
        // Implementation: recreate destination clients for changed config
    }
});
```

**4.6 Error Handling Improvements**

- [ ] Add detailed error logging for destination failures
- [ ] Implement graceful degradation: if one destination fails, others continue
- [ ] Add circuit breaker pattern for consistently failing destinations

### Tests for Phase 4

```rust
// tests/destination_test.rs
#[tokio::test]
async fn test_destination_pool_connect() {
    let mock_server = start_mock_rtmp_server().await;
    let mut client = RtmpClient::new(
        "test".into(),
        format!("rtmp://{}/app", mock_server.addr()),
        "key".into(),
        MetricsCollector::new(),
    );

    client.connect().await.unwrap();
    assert!(client.is_connected());
}

#[tokio::test]
async fn test_replicate_to_multiple_destinations() {
    let pool = create_test_destination_pool(3).await;
    let chunk = create_test_chunk();

    pool.replicate(&chunk).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify all destinations received chunk
}

#[tokio::test]
async fn test_retry_policy_exponential() {
    let policy = RetryPolicy::new(
        Duration::from_millis(100),
        Duration::from_millis(1000),
        7,
        2.0,
    );

    assert_eq!(policy.get_delay(0), Duration::from_millis(100));
    assert_eq!(policy.get_delay(1), Duration::from_millis(200));
    assert_eq!(policy.get_delay(2), Duration::from_millis(400));
    assert_eq!(policy.get_delay(3), Duration::from_millis(800));
    assert_eq!(policy.get_delay(4), Duration::from_millis(1000)); // capped
    assert_eq!(policy.get_delay(5), Duration::from_millis(1000));
    assert_eq!(policy.get_delay(6), Duration::from_millis(1000));
    assert_eq!(policy.get_delay(7), Duration::MAX); // give up
}

#[tokio::test]
async fn test_stream_key_validation() {
    let config = create_test_config();
    assert!(validate_stream_key(&config, "valid_key"));
    assert!(!validate_stream_key(&config, "invalid_key"));
}
```

### Manual Testing

```bash
# Terminal 1: Start server
cargo run

# Terminal 2: Publish from OBS
# OBS Settings:
# Server: rtmp://localhost:1935/live
# Stream Key: obs_source_key

# Terminal 3: Check metrics
watch -n 1 'curl -s http://localhost:3000/metrics | grep rtmp_destination'

# Terminal 4: Check logs
# Should see:
# "Connecting to twitch..."
# "Connected to twitch"
# "Connecting to youtube..."
# "Connected to youtube"

# Test failure: Stop Twitch endpoint temporarily
# Verify retry in logs
# Verify YouTube still receives stream

# Test auth: Publish with wrong stream key
# Verify rejection
```

### Acceptance Criteria for Phase 4

- [ ] OBS publishes successfully
- [ ] Twitch + YouTube receive stream (verify in their dashboards)
- [ ] Auth rejects invalid stream keys
- [ ] Retry with exponential backoff on destination failure
- [ ] Hot reload works (change enabled destinations in config.toml)
- [ ] Metrics show destination status

---

## Phase 5: Testing & Polish (2-3 days)

### Task List

**5.1 Comprehensive Unit Tests**

Target: 90%+ coverage for core modules

Protocol:
- [ ] All chunk header types (0, 1, 2, 3)
- [ ] Extended timestamp handling
- [ ] Message reassembly (1 chunk, 3 chunks, 10 chunks)
- [ ] ACK tracking
- [ ] Server and client handshake

AMF:
- [ ] All AMF0 types: Number, String, Boolean, Object, Null, Array
- [ ] Nested objects
- [ ] All command response builders

Destination:
- [ ] Connection flow (handshake, connect, publish)
- [ ] Retry policy calculations
- [ ] State transitions

Config:
- [ ] Config loading from TOML
- [ ] Secrets loading and merging
- [ ] Invalid config handling

**5.2 Integration Tests**

- [ ] Full handshake + publish flow
- [ ] Multi-destination replication
- [ ] Auth failure paths
- [ ] Connection timeout handling
- [ ] Hot reload

**5.3 Load Testing**

```rust
// tests/load_test.rs
#[tokio::test]
async fn test_concurrent_publishing() {
    // Test multiple publishers (if allowed by config)
}

#[tokio::test]
async fn test_large_message_replication() {
    // Test 1MB+ message replication
}

#[tokio::test]
async fn test_sustained_replication() {
    // Test replication for 5+ minutes
}
```

**5.4 Documentation**

README.md:
```markdown
# RTMP Multi-Destination Server

## Quick Start

1. Create `config.toml`:
```toml
[rtmp]
addr = "0.0.0.0:1935"

[auth]
allowed_stream_keys = ["my_stream_key"]
```

2. Create `secrets.toml`:
```toml
[destinations.twitch]
rtmp_url = "rtmp://live.twitch.tv/app"
stream_key = "your_twitch_key"
```

3. Run:
```bash
cargo run
```

4. Publish from OBS:
- Server: `rtmp://localhost:1935/live`
- Stream Key: `my_stream_key`

## Configuration

### Options

- `rtmp.addr`: RTMP server address (default: `0.0.0.0:1935`)
- `http.addr`: HTTP metrics address (default: `0.0.0.0:3000`)
- `auth.allowed_stream_keys`: List of valid stream keys
- `destinations`: Configure destination platforms

## Metrics

Access metrics at `http://localhost:3000/metrics`

Key metrics:
- `rtmp_connections_active` - Active connections
- `rtmp_destinations_connected` - Connected destinations (label: destination)
- `rtmp_chunks_replicated_total` - Chunks replicated (label: destination)
- `rtmp_destination_errors_total` - Destination errors (label: destination)

## Hot Reload

Enable hot reload in `config.toml`:
```toml
[hotreload]
enabled = true
check_interval_ms = 1000
```

Modify `config.toml` or `secrets.toml` to reload without restart.
```

- [ ] Add inline documentation for public APIs
- [ ] Add example configuration in `examples/config-example.toml`

**5.5 Performance**

- [ ] Profile with `cargo flamegraph`
- [ ] Optimize hot paths (reduce allocations, use `Bytes` instead of `Vec<u8>`)
- [ ] Benchmark throughput (MB/s replication)

```bash
# Profiling
cargo install flamegraph
cargo flamegraph --bin axum-rtmp-server

# Benchmarking
cargo install cargo-criterion
cargo criterion
```

---

## Testing Strategy

### Unit Tests

```bash
# Run all unit tests
cargo test --lib

# Run specific module tests
cargo test --lib protocol::
cargo test --lib amf::

# Run with coverage
cargo install tarpaulin
cargo tarpaulin --out Html --output-dir coverage/
```

### Integration Tests

```bash
# Run all integration tests
cargo test --test

# Run specific test
cargo test test_multi_chunk_reassembly

# Run with output
cargo test -- --nocapture
```

### Manual Testing Checklist

- [ ] OBS connects successfully with correct credentials
- [ ] OBS publishes video + audio
- [ ] Twitch receives stream (check Twitch dashboard)
- [ ] YouTube receives stream (check YouTube Studio)
- [ ] Auth rejects invalid stream keys (OBS shows error)
- [ ] Retry on destination failure (log shows retry attempts)
- [ ] Hot reload works (change config, verify without restart)
- [ ] Metrics endpoint returns correct data
- [ ] Server handles graceful shutdown

### Real-World Testing

```bash
# 1. Test with actual Twitch credentials
# Add stream key to secrets.toml
# Start server
# Publish from OBS
# Verify stream appears on Twitch

# 2. Test destination failure
# Block YouTube destination (firewall or disable)
# Observe retry logs with exponential backoff
# Verify Twitch still receives stream
# Unblock YouTube, verify reconnection

# 3. Test sustained operation
# Publish for 30+ minutes
# Monitor memory usage
# Monitor metrics (no memory leaks)

# 4. Test hot reload
# Add new destination in config.toml
# Save file
# Verify destination connects without restart
# Disable destination in config.toml
# Verify destination disconnects
```

---

## Prometheus Metrics

### Available Metrics

```
# Connection Metrics
rtmp_connections_active - Gauge
rtmp_connections_total - Counter
rtmp_connections_errors_total - Counter

# Stream Metrics
rtmp_streams_published - Counter
rtmp_bytes_received - Counter
rtmp_chunks_received - Counter

# Destination Metrics
rtmp_destinations_connected - GaugeVec (label: destination)
rtmp_chunks_replicated_total - CounterVec (label: destination)
rtmp_destination_errors_total - CounterVec (label: destination)

# Auth Metrics
rtmp_auth_attempts_total - Counter
rtmp_auth_failures_total - Counter

# Future: Sent metrics (not tracked yet)
# rtmp_bytes_sent - Counter
```

### Example Output

```
# HELP rtmp_connections_active Current active RTMP connections
# TYPE rtmp_connections_active gauge
rtmp_connections_active 1

# HELP rtmp_destinations_connected Current connected destinations
# TYPE rtmp_destinations_connected gauge
rtmp_destinations_connected{destination="twitch"} 1
rtmp_destinations_connected{destination="youtube"} 1
rtmp_destinations_connected{destination="tiktok"} 0

# HELP rtmp_chunks_replicated_total Chunks replicated to destinations
# TYPE rtmp_chunks_replicated_total counter
rtmp_chunks_replicated_total{destination="twitch"} 15234
rtmp_chunks_replicated_total{destination="youtube"} 15234

# HELP rtmp_destination_errors_total Destination errors
# TYPE rtmp_destination_errors_total counter
rtmp_destination_errors_total{destination="tiktok"} 0

# HELP rtmp_auth_attempts_total Total auth attempts
# TYPE rtmp_auth_attempts_total counter
rtmp_auth_attempts_total 5
```

### Query Examples (PromLens)

```promql
# Active connections
rtmp_connections_active

# Chunks per second to Twitch
rate(rtmp_chunks_replicated_total{destination="twitch"}[1m])

# Error rate by destination
rate(rtmp_destination_errors_total[5m])

# Destination connection status
rtmp_destinations_connected
```

### Visualization Setup

**Option 1: Built-in Prometheus UI**
```bash
# Install Prometheus
brew install prometheus  # macOS
# or download from prometheus.io

# Create prometheus.yml
cat > prometheus.yml <<EOF
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: 'rtmp-server'
    static_configs:
      - targets: ['localhost:3000']
EOF

# Run Prometheus
prometheus --config.file=prometheus.yml

# Access UI at http://localhost:9090
```

**Option 2: PromLens (Standalone)**
```bash
# Install PromLens
git clone https://github.com/prometheus/promlens.git
cd promlens
npm install
npm start

# Access at http://localhost:3000
# Set Prometheus URL to http://localhost:9090
```

**Option 3: Grafana**
```bash
# Run Grafana with Docker
docker run -d -p 3001:3000 grafana/grafana

# Login (admin/admin)
# Add Prometheus data source (http://localhost:9090)
# Import dashboard
```

---

## Timeline

| Phase | Tasks | Duration | Dependencies |
|-------|-------|----------|--------------|
| 1 | Critical bug fixes | 1-2 days | None |
| 2 | Protocol correctness | 3-4 days | Phase 1 |
| 3 | Architecture refactor | 5-7 days | Phase 2 |
| 4 | Multi-destination support | 3-4 days | Phase 3 |
| 5 | Testing & polish | 2-3 days | Phase 4 |
| **Total** | | **14-20 days** | |

---

## Future Enhancements (Post-MVP)

### Playback Support
- Implement RTMP playback (`play` command)
- Add HTTP-FLV endpoint for browser playback
- Consider WebRTC for lower latency

### Advanced Features
- Stream transcoding (different bitrates per destination)
- Stream recording (save to MP4)
- SRT protocol support
- Authentication via external API
- Web UI for management

### Monitoring
- Health check endpoint
- Integration with PagerDuty/Sentry for alerts
- Detailed logs per destination

---

## Notes

- All decisions made during planning phase are reflected in this document
- Exponential backoff with max 7 attempts
- Hot reload enabled by default
- Prometheus metrics at `/metrics`
- Stream key authentication from config
- Secrets stored in separate gitignored file
- Playback deferred (will be WebRTC when implemented)

---

## Dependencies

### New crates to add:
```toml
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
toml = "0.8"
prometheus = "0.13"
notify = "6.1"  # For hot reload
```

### Existing crates to keep:
```toml
axum = "0.7"
bytes = "1.5"
dashmap = "5.5"
tokio = { version = "1.35", features = ["full"] }
tracing = "0.1"
tracing-subscriber = "0.3"
rand = "0.8"
thiserror = "1.0"
futures = "0.3"
```

### Remove:
```toml
rml_amf0 = "0.3"  # Not used
```

---

## Appendix

### Protocol Constants

```
RTMP_VERSION = 3
DEFAULT_CHUNK_SIZE = 128 bytes
MAX_CHUNK_SIZE = 65536 bytes

Message Type IDs:
0x05 = Window Acknowledgement Size
0x01 = Set Chunk Size
0x03 = Acknowledgement
0x04 = User Control Message
0x06 = Set Peer Bandwidth
0x08 = Audio Data
0x09 = Video Data
0x12 = Data Message (AMF0)
0x14 = Command Message (AMF0)
0x0F = Data Message (AMF3)
0x11 = Command Message (AMF3)

User Control Events:
0 = Stream Begin
1 = Stream EOF
2 = Stream Dry
3 = Set Buffer Length
4 = Stream Is Recorded
6 = Ping Request
7 = Ping Response
```

### AMF0 Type Markers

```
0x00 = Number
0x01 = Boolean
0x02 = String
0x03 = Object
0x05 = Null
0x06 = Undefined
0x07 = Reference (deprecated)
0x08 = ECMA Array
0x0A = Strict Array
0x0B = Date
0x0C = Long String
0x00 0x00 0x09 = Object End Marker
```
