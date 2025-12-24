#[derive(Debug, Clone, PartialEq)]
pub enum StreamState {
    Initial,
    Connected,
    Ready,
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
