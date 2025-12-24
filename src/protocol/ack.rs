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

    pub fn record_received(&mut self, bytes: u32) {
        self.bytes_received += bytes;
        if self.bytes_received >= self.window_size {
            self.bytes_received = 0;
        }
    }

    pub fn get_ack_value(&self) -> u32 {
        self.bytes_sent
    }

    pub fn reset(&mut self) {
        self.bytes_sent = 0;
        self.bytes_received = 0;
    }
}
