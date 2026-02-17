use std::fs;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Clone, Deserialize)]
pub struct PluginConfig {
    #[serde(default = "default_clickhouse_url")]
    pub clickhouse_url: String,
    #[serde(default = "default_clickhouse_database")]
    pub clickhouse_database: String,
    #[serde(default = "default_clickhouse_table")]
    pub clickhouse_table: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_create_schema")]
    pub create_schema: bool,
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            clickhouse_url: default_clickhouse_url(),
            clickhouse_database: default_clickhouse_database(),
            clickhouse_table: default_clickhouse_table(),
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
            max_retries: default_max_retries(),
            channel_capacity: default_channel_capacity(),
            create_schema: default_create_schema(),
        }
    }
}

impl PluginConfig {
    pub fn load(path: &str) -> Result<Self, ConfigError> {
        let raw = fs::read_to_string(path)?;
        let parsed: RawConfig = serde_json::from_str(&raw)?;
        let mut config = Self::default();

        if let Some(v) = parsed.clickhouse_url {
            config.clickhouse_url = v;
        }
        if let Some(v) = parsed.clickhouse_database {
            config.clickhouse_database = v;
        }
        if let Some(v) = parsed.clickhouse_table {
            config.clickhouse_table = v;
        }
        if let Some(v) = parsed.batch_size {
            config.batch_size = v;
        }
        if let Some(v) = parsed.batch_timeout_ms {
            config.batch_timeout_ms = v;
        }
        if let Some(v) = parsed.max_retries {
            config.max_retries = v;
        }
        if let Some(v) = parsed.channel_capacity {
            config.channel_capacity = v;
        }
        if let Some(v) = parsed.create_schema {
            config.create_schema = v;
        }

        if let Some(clickhouse) = parsed.clickhouse {
            if let Some(endpoint) = clickhouse.endpoint {
                config.clickhouse_url = normalize_clickhouse_url(&endpoint);
            }
            if let Some(database) = clickhouse.database {
                config.clickhouse_database = database;
            }
        }

        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.batch_size == 0 {
            return Err(ConfigError::Invalid(
                "batch_size must be greater than 0".to_string(),
            ));
        }
        if self.batch_timeout_ms == 0 {
            return Err(ConfigError::Invalid(
                "batch_timeout_ms must be greater than 0".to_string(),
            ));
        }
        if self.channel_capacity == 0 {
            return Err(ConfigError::Invalid(
                "channel_capacity must be greater than 0".to_string(),
            ));
        }
        if self.clickhouse_database.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "clickhouse_database cannot be empty".to_string(),
            ));
        }
        if self.clickhouse_table.trim().is_empty() {
            return Err(ConfigError::Invalid(
                "clickhouse_table cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config file JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    clickhouse_url: Option<String>,
    clickhouse_database: Option<String>,
    clickhouse_table: Option<String>,
    batch_size: Option<usize>,
    batch_timeout_ms: Option<u64>,
    max_retries: Option<u32>,
    channel_capacity: Option<usize>,
    create_schema: Option<bool>,
    clickhouse: Option<LegacyClickhouse>,
}

#[derive(Debug, Deserialize)]
struct LegacyClickhouse {
    endpoint: Option<String>,
    database: Option<String>,
}

fn default_clickhouse_url() -> String {
    "http://127.0.0.1:8123".to_string()
}

fn default_clickhouse_database() -> String {
    "solana".to_string()
}

fn default_clickhouse_table() -> String {
    "accounts".to_string()
}

fn default_batch_size() -> usize {
    1_000
}

fn default_batch_timeout_ms() -> u64 {
    5_000
}

fn default_max_retries() -> u32 {
    3
}

fn default_channel_capacity() -> usize {
    100_000
}

fn default_create_schema() -> bool {
    true
}

fn normalize_clickhouse_url(endpoint: &str) -> String {
    if endpoint.starts_with("tcp://") {
        endpoint.replacen("tcp://", "http://", 1)
    } else {
        endpoint.to_string()
    }
}
