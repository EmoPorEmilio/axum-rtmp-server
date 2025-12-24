use std::sync::Arc;
use tokio::sync::broadcast;
use dashmap::DashMap;

pub struct StreamRegistry {
    streams: dashmap::DashMap<String, SourceStreamMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceStreamMetadata {
    pub app_name: String,
    pub stream_key: String,
    pub chunk_size: u32,
}

impl StreamRegistry {
    pub fn new() -> Self {
        Self {
            streams: dashmap::DashMap::new(),
        }
    }

    pub fn register(&self, key: String, metadata: SourceStreamMetadata) {
        self.streams.insert(key, metadata);
    }

    pub fn get(&self, key: &str) -> Option<SourceStreamMetadata> {
        self.streams.get(key).map(|v| v.clone())
    }

    pub fn unregister(&self, key: &str) {
        self.streams.remove(key);
    }
}
