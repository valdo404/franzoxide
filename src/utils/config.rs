use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

/// Main configuration for the Rust-Connect service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Optional TCP address for the gRPC server (e.g., "127.0.0.1:50051")
    pub tcp_address: Option<String>,
    
    /// Optional Unix socket path for the gRPC server (e.g., "/tmp/rust-connect.sock")
    pub unix_socket_path: Option<String>,
    
    /// Kafka broker configuration
    pub kafka: KafkaConfig,
    
    /// List of connector configurations
    pub connectors: Vec<ConnectorConfig>,
}

/// Kafka broker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// List of Kafka bootstrap servers
    pub bootstrap_servers: Vec<String>,
    
    /// Optional group ID for the Kafka consumer
    pub group_id: Option<String>,
    
    /// Additional Kafka client configuration
    #[serde(default)]
    pub properties: std::collections::HashMap<String, String>,
}

/// Connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    /// Connector name
    pub name: String,
    
    /// Connector class
    pub connector_class: String,
    
    /// Connector type (source or sink)
    pub connector_type: ConnectorType,
    
    /// Maximum number of tasks
    pub tasks_max: i32,
    
    /// Topics to consume from (for sink connectors) or produce to (for source connectors)
    pub topics: Vec<String>,
    
    /// Additional connector-specific configuration
    #[serde(default)]
    pub config: std::collections::HashMap<String, String>,
}

/// Connector type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConnectorType {
    /// Source connector (reads from external system, writes to Kafka)
    Source,
    
    /// Sink connector (reads from Kafka, writes to external system)
    Sink,
}

/// Load configuration from a JSON file
pub fn load_config<P: AsRef<Path>>(path: P) -> Result<Config> {
    let file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open config file: {}", path.as_ref().display()))?;
    
    let reader = BufReader::new(file);
    let config = serde_json::from_reader(reader)
        .with_context(|| format!("Failed to parse config file: {}", path.as_ref().display()))?;
    
    Ok(config)
}

/// Create a default configuration
#[allow(dead_code)]
pub fn default_config() -> Config {
    Config {
        tcp_address: Some("127.0.0.1:50051".to_string()),
        unix_socket_path: Some("/tmp/rust-connect.sock".to_string()),
        kafka: KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            group_id: Some("rust-connect".to_string()),
            properties: std::collections::HashMap::new(),
        },
        connectors: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_load_config() {
        // Create a temporary config file
        let mut file = NamedTempFile::new().unwrap();
        let config_json = r#"
        {
            "tcp_address": "127.0.0.1:50051",
            "unix_socket_path": "/tmp/rust-connect.sock",
            "kafka": {
                "bootstrap_servers": ["localhost:9092"],
                "group_id": "rust-connect",
                "properties": {
                    "auto.offset.reset": "earliest"
                }
            },
            "connectors": [
                {
                    "name": "s3-sink",
                    "connector_class": "io.rustconnect.S3SinkConnector",
                    "connector_type": "sink",
                    "tasks_max": 2,
                    "topics": ["test-topic"],
                    "config": {
                        "s3.bucket.name": "test-bucket",
                        "s3.region": "us-east-1",
                        "format.class": "json"
                    }
                }
            ]
        }
        "#;
        file.write_all(config_json.as_bytes()).unwrap();
        
        // Load the config
        let config = load_config(file.path()).unwrap();
        
        // Verify the config
        assert_eq!(config.tcp_address, Some("127.0.0.1:50051".to_string()));
        assert_eq!(config.unix_socket_path, Some("/tmp/rust-connect.sock".to_string()));
        assert_eq!(config.kafka.bootstrap_servers, vec!["localhost:9092".to_string()]);
        assert_eq!(config.kafka.group_id, Some("rust-connect".to_string()));
        assert_eq!(config.kafka.properties.get("auto.offset.reset").unwrap(), "earliest");
        
        assert_eq!(config.connectors.len(), 1);
        let connector = &config.connectors[0];
        assert_eq!(connector.name, "s3-sink");
        assert_eq!(connector.connector_class, "io.rustconnect.S3SinkConnector");
        assert_eq!(connector.connector_type, ConnectorType::Sink);
        assert_eq!(connector.tasks_max, 2);
        assert_eq!(connector.topics, vec!["test-topic".to_string()]);
        assert_eq!(connector.config.get("s3.bucket.name").unwrap(), "test-bucket");
        assert_eq!(connector.config.get("s3.region").unwrap(), "us-east-1");
        assert_eq!(connector.config.get("format.class").unwrap(), "json");
    }
}
