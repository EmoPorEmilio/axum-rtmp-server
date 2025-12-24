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

    #[error("Handshake error: {0}")]
    HandshakeError(String),

    #[error("Connection closed")]
    ConnectionClosed,
}
