use serde::Deserialize;
use std::fs;
use std::path::Path;
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
            return Duration::MAX;
        }

        let delay_ms = self.initial_interval_ms as f64
            * self.multiplier.powi(attempt as i32);
        let delay_ms = delay_ms.min(self.max_interval_ms as f64);
        Duration::from_millis(delay_ms as u64)
    }

    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }
}

pub struct FullConfig {
    pub config: Config,
    pub secrets: Secrets,
}
