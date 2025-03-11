use async_trait::async_trait;
use aws_sdk_s3::{Client as S3Client};
use aws_sdk_s3::primitives::ByteStream;
use log::{debug, error, info};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use chrono::{Utc, Datelike, Timelike};
use tokio::sync::Mutex as TokioMutex;

use crate::connector::common::{Connector, ConnectorState, SinkConnector, TaskConfig};
use crate::kafka_connect::KafkaRecord;
use crate::utils::error::{ConnectorError, ConnectorResult};

/// S3 Sink Connector implementation
pub struct S3SinkConnector {
    /// Name of the connector
    name: String,
    
    /// Configuration for the connector
    config: HashMap<String, String>,
    
    /// S3 client
    s3_client: Option<S3Client>,
    
    /// Current state of the connector
    state: ConnectorState,
    
    /// Buffer for records before flushing to S3
    buffer: Arc<TokioMutex<Vec<KafkaRecord>>>,
    
    /// Bucket name
    bucket: String,
    
    /// Prefix for S3 objects
    prefix: String,
    
    /// Format for S3 objects
    format: Format,
    
    /// Partitioner for S3 objects
    partitioner: Partitioner,
    
    /// Number of records to buffer before flushing
    flush_size: usize,
}

/// Format for S3 objects
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// JSON format
    Json,
    
    /// Avro format
    Avro,
    
    /// Parquet format
    Parquet,
    
    /// Raw bytes format
    Bytes,
}

impl Format {
    /// Parse format from string
    pub fn from_str(s: &str) -> ConnectorResult<Self> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Format::Json),
            "avro" => Ok(Format::Avro),
            "parquet" => Ok(Format::Parquet),
            "bytes" => Ok(Format::Bytes),
            _ => Err(ConnectorError::ConfigError(format!("Invalid format: {}", s))),
        }
    }
    
    /// Get file extension for format
    pub fn extension(&self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Avro => "avro",
            Format::Parquet => "parquet",
            Format::Bytes => "bin",
        }
    }
}

/// Partitioner for S3 objects
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Partitioner {
    /// Default partitioner (topic/partition/timestamp)
    Default,
    
    /// Field-based partitioner
    Field,
    
    /// Time-based partitioner
    Time,
}

impl Partitioner {
    /// Parse partitioner from string
    pub fn from_str(s: &str) -> ConnectorResult<Self> {
        match s.to_lowercase().as_str() {
            "default" => Ok(Partitioner::Default),
            "field" => Ok(Partitioner::Field),
            "time" => Ok(Partitioner::Time),
            _ => Err(ConnectorError::ConfigError(format!("Invalid partitioner: {}", s))),
        }
    }
}

impl S3SinkConnector {
    /// Create a new S3 sink connector
    pub fn new(name: String, task_config: TaskConfig) -> Self {
        Self {
            name,
            config: task_config.config,
            s3_client: None,
            state: ConnectorState::Uninitialized,
            buffer: Arc::new(TokioMutex::new(Vec::new())),
            bucket: String::new(),
            prefix: String::new(),
            format: Format::Json,
            partitioner: Partitioner::Default,
            flush_size: 1000,
        }
    }
    
    /// Generate S3 key for a record
    fn generate_key(&self, record: &KafkaRecord) -> String {
        match self.partitioner {
            Partitioner::Default => {
                // Default partitioning: topics/partition/timestamp
                format!(
                    "{}/{}/{}_{}.{}",
                    self.prefix,
                    record.topic,
                    record.partition,
                    record.timestamp,
                    self.format.extension()
                )
            }
            Partitioner::Field => {
                // Field-based partitioning (not implemented yet)
                // For now, fall back to default partitioning
                format!(
                    "{}/{}/{}_{}.{}",
                    self.prefix,
                    record.topic,
                    record.partition,
                    record.timestamp,
                    self.format.extension()
                )
            }
            Partitioner::Time => {
                // Time-based partitioning
                let dt = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(record.timestamp)
                    .unwrap_or_else(|| Utc::now());
                
                format!(
                    "{}/{}/year={}/month={:02}/day={:02}/hour={:02}/{}.{}",
                    self.prefix,
                    record.topic,
                    dt.year(),
                    dt.month(),
                    dt.day(),
                    dt.hour(),
                    record.offset,
                    self.format.extension()
                )
            }
        }
    }
    
    /// Format records as JSON
    fn format_as_json(&self, records: &[KafkaRecord]) -> ConnectorResult<Vec<u8>> {
        let mut buffer = Vec::new();
        
        for record in records {
            let mut json_record = serde_json::Map::new();
            
            // Add metadata
            json_record.insert("topic".to_string(), serde_json::Value::String(record.topic.clone()));
            json_record.insert("partition".to_string(), serde_json::Value::Number(serde_json::Number::from(record.partition)));
            json_record.insert("offset".to_string(), serde_json::Value::Number(serde_json::Number::from(record.offset)));
            json_record.insert("timestamp".to_string(), serde_json::Value::Number(serde_json::Number::from(record.timestamp)));
            
            // Add key and value
            if !record.key.is_empty() {
                match serde_json::from_slice::<serde_json::Value>(&record.key) {
                    Ok(key) => {
                        json_record.insert("key".to_string(), key);
                    }
                    Err(_) => {
                        // If key is not valid JSON, store it as base64
                        let base64_key = base64::encode(&record.key);
                        json_record.insert("key".to_string(), serde_json::Value::String(base64_key));
                        json_record.insert("key_format".to_string(), serde_json::Value::String("base64".to_string()));
                    }
                }
            }
            
            if !record.value.is_empty() {
                match serde_json::from_slice::<serde_json::Value>(&record.value) {
                    Ok(value) => {
                        json_record.insert("value".to_string(), value);
                    }
                    Err(_) => {
                        // If value is not valid JSON, store it as base64
                        let base64_value = base64::encode(&record.value);
                        json_record.insert("value".to_string(), serde_json::Value::String(base64_value));
                        json_record.insert("value_format".to_string(), serde_json::Value::String("base64".to_string()));
                    }
                }
            }
            
            // Add headers
            let mut headers = serde_json::Map::new();
            for (key, value) in &record.headers {
                headers.insert(key.clone(), serde_json::Value::String(value.clone()));
            }
            json_record.insert("headers".to_string(), serde_json::Value::Object(headers));
            
            // Write the record to the buffer
            serde_json::to_writer(&mut buffer, &json_record)?;
            buffer.write_all(b"\n")?;
        }
        
        Ok(buffer)
    }
    
    /// Upload data to S3
    async fn upload_to_s3(&self, key: &str, data: Vec<u8>) -> ConnectorResult<()> {
        let client = self.s3_client.as_ref()
            .ok_or_else(|| ConnectorError::General("S3 client not initialized".to_string()))?;
        
        debug!("Uploading {} bytes to s3://{}/{}", data.len(), self.bucket, key);
        
        let body = ByteStream::from(data);
        
        match client.put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(body)
            .send()
            .await {
                Ok(_) => {
                    info!("Successfully uploaded to s3://{}/{}", self.bucket, key);
                    Ok(())
                }
                Err(err) => {
                    error!("Failed to upload to S3: {}", err);
                    Err(ConnectorError::S3Error(err.to_string()))
                }
            }
    }
}

#[async_trait]
impl Connector for S3SinkConnector {
    fn name(&self) -> &str {
        &self.name
    }
    
    async fn initialize(&mut self, config: HashMap<String, String>) -> ConnectorResult<()> {
        info!("Initializing S3 sink connector: {}", self.name);
        
        // Update configuration
        self.config.extend(config);
        
        // Get required configuration
        self.bucket = self.config.get("s3.bucket.name")
            .ok_or_else(|| ConnectorError::ConfigError("Missing s3.bucket.name".to_string()))?
            .clone();
        
        // Get optional configuration with defaults
        self.prefix = self.config.get("s3.prefix")
            .cloned()
            .unwrap_or_else(|| "".to_string());
        
        let format_str = self.config.get("format.class")
            .or_else(|| self.config.get("format"))
            .cloned()
            .unwrap_or_else(|| "json".to_string());
        self.format = Format::from_str(&format_str)?;
        
        let partitioner_str = self.config.get("partitioner.class")
            .or_else(|| self.config.get("partitioner"))
            .cloned()
            .unwrap_or_else(|| "default".to_string());
        self.partitioner = Partitioner::from_str(&partitioner_str)?;
        
        let flush_size_str = self.config.get("flush.size")
            .cloned()
            .unwrap_or_else(|| "1000".to_string());
        self.flush_size = flush_size_str.parse()
            .map_err(|_| ConnectorError::ConfigError(format!("Invalid flush.size: {}", flush_size_str)))?;
        
        // Initialize AWS SDK
        let region = self.config.get("s3.region")
            .cloned()
            .unwrap_or_else(|| "us-east-1".to_string());
        
        let config = aws_sdk_s3::config::Builder::new()
            .region(aws_sdk_s3::config::Region::new(region))
            .build();
        
        self.s3_client = Some(aws_sdk_s3::Client::from_conf(config));
        
        self.state = ConnectorState::Stopped;
        
        Ok(())
    }
    
    async fn start(&mut self) -> ConnectorResult<()> {
        info!("Starting S3 sink connector: {}", self.name);
        self.state = ConnectorState::Running;
        Ok(())
    }
    
    async fn stop(&mut self) -> ConnectorResult<()> {
        info!("Stopping S3 sink connector: {}", self.name);
        self.state = ConnectorState::Stopped;
        Ok(())
    }
    
    async fn state(&self) -> ConnectorState {
        self.state
    }
}

#[async_trait]
impl SinkConnector for S3SinkConnector {
    async fn put(&mut self, records: Vec<KafkaRecord>) -> ConnectorResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        
        debug!("Received {} records", records.len());
        
        // Add records to buffer
        {
            let mut buffer = self.buffer.lock().await;
            buffer.extend(records);
            
            // If buffer size exceeds flush size, flush
            if buffer.len() >= self.flush_size {
                let records_to_flush = buffer.clone();
                buffer.clear();
                
                // Drop the lock before flushing
                drop(buffer);
                
                // Flush records
                self.flush_records(records_to_flush).await?;
            }
        }
        
        Ok(())
    }
    
    async fn flush(&mut self) -> ConnectorResult<()> {
        let records_to_flush = {
            let mut buffer = self.buffer.lock().await;
            let records = buffer.clone();
            buffer.clear();
            records
        };
        
        if !records_to_flush.is_empty() {
            self.flush_records(records_to_flush).await?;
        }
        
        Ok(())
    }
}

impl S3SinkConnector {
    /// Flush records to S3
    async fn flush_records(&self, records: Vec<KafkaRecord>) -> ConnectorResult<()> {
        if records.is_empty() {
            return Ok(());
        }
        
        info!("Flushing {} records to S3", records.len());
        
        // Group records by topic and partition
        let mut grouped_records: HashMap<(String, i32), Vec<KafkaRecord>> = HashMap::new();
        
        for record in records {
            let key = (record.topic.clone(), record.partition);
            grouped_records.entry(key).or_default().push(record);
        }
        
        // Process each group
        for ((_topic, _partition), records) in grouped_records {
            // Use the first record for key generation
            let key = self.generate_key(&records[0]);
            
            // Format records based on the configured format
            let data = match self.format {
                Format::Json => self.format_as_json(&records)?,
                Format::Avro => {
                    // Not implemented yet
                    return Err(ConnectorError::General("Avro format not implemented yet".to_string()));
                }
                Format::Parquet => {
                    // Not implemented yet
                    return Err(ConnectorError::General("Parquet format not implemented yet".to_string()));
                }
                Format::Bytes => {
                    // Just concatenate the raw values
                    let mut buffer = Vec::new();
                    for record in &records {
                        buffer.extend_from_slice(&record.value);
                    }
                    buffer
                }
            };
            
            // Upload to S3
            self.upload_to_s3(&key, data).await?;
        }
        
        Ok(())
    }
}

/// Factory for creating S3 sink connector tasks
pub struct S3SinkConnectorFactory;

impl S3SinkConnectorFactory {
    /// Create a new S3 sink connector factory
    pub fn new() -> Self {
        Self
    }
    
    /// Create a new S3 sink connector task
    pub async fn create_task(&self, name: String, task_config: TaskConfig) -> ConnectorResult<S3SinkConnector> {
        let mut connector = S3SinkConnector::new(name, task_config.clone());
        connector.initialize(task_config.config).await?;
        Ok(connector)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    #[tokio::test]
    async fn test_s3_sink_connector_initialization() {
        let name = "test-connector".to_string();
        let mut config = HashMap::new();
        config.insert("s3.bucket.name".to_string(), "test-bucket".to_string());
        config.insert("s3.region".to_string(), "us-east-1".to_string());
        config.insert("format.class".to_string(), "json".to_string());
        config.insert("partitioner.class".to_string(), "default".to_string());
        config.insert("flush.size".to_string(), "100".to_string());
        
        let task_config = TaskConfig {
            task_id: 0,
            config: config.clone(),
        };
        
        let mut connector = S3SinkConnector::new(name, task_config);
        
        // Initialize the connector
        // Note: This test will fail if AWS credentials are not available
        // In a real test, we would mock the S3 client
        let result = connector.initialize(config).await;
        
        // We expect this to succeed if AWS credentials are available
        // Otherwise, it will fail with an AWS error
        if result.is_err() {
            let err = result.unwrap_err();
            println!("Error initializing connector: {}", err);
            // We'll consider this a success if the error is not a configuration error
            match err {
                ConnectorError::ConfigError(_) => panic!("Configuration error: {}", err),
                _ => {} // Other errors are expected if AWS credentials are not available
            }
        }
        
        // Check that the connector is in the stopped state
        assert_eq!(connector.state, ConnectorState::Stopped);
    }
    
    #[test]
    fn test_format_from_str() {
        assert_eq!(Format::from_str("json").unwrap(), Format::Json);
        assert_eq!(Format::from_str("avro").unwrap(), Format::Avro);
        assert_eq!(Format::from_str("parquet").unwrap(), Format::Parquet);
        assert_eq!(Format::from_str("bytes").unwrap(), Format::Bytes);
        
        // Case insensitive
        assert_eq!(Format::from_str("JSON").unwrap(), Format::Json);
        
        // Invalid format
        assert!(Format::from_str("invalid").is_err());
    }
    
    #[test]
    fn test_partitioner_from_str() {
        assert_eq!(Partitioner::from_str("default").unwrap(), Partitioner::Default);
        assert_eq!(Partitioner::from_str("field").unwrap(), Partitioner::Field);
        assert_eq!(Partitioner::from_str("time").unwrap(), Partitioner::Time);
        
        // Case insensitive
        assert_eq!(Partitioner::from_str("DEFAULT").unwrap(), Partitioner::Default);
        
        // Invalid partitioner
        assert!(Partitioner::from_str("invalid").is_err());
    }
    
    #[test]
    fn test_generate_key() {
        let name = "test-connector".to_string();
        let mut config = HashMap::new();
        config.insert("s3.bucket.name".to_string(), "test-bucket".to_string());
        config.insert("s3.prefix".to_string(), "prefix".to_string());
        
        let task_config = TaskConfig {
            task_id: 0,
            config: config.clone(),
        };
        
        let connector = S3SinkConnector {
            name,
            config,
            s3_client: None,
            state: ConnectorState::Uninitialized,
            buffer: Arc::new(TokioMutex::new(Vec::new())),
            bucket: "test-bucket".to_string(),
            prefix: "prefix".to_string(),
            format: Format::Json,
            partitioner: Partitioner::Default,
            flush_size: 1000,
        };
        
        let record = KafkaRecord {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            timestamp: 1234567890,
            key: Vec::new(),
            value: Vec::new(),
            headers: HashMap::new(),
        };
        
        // Test default partitioner
        let key = connector.generate_key(&record);
        assert_eq!(key, "prefix/test-topic/0_1234567890.json");
        
        // Test time partitioner
        let connector = S3SinkConnector {
            partitioner: Partitioner::Time,
            ..connector
        };
        
        let key = connector.generate_key(&record);
        // The exact format will depend on the timestamp, but we can check that it contains the expected components
        assert!(key.starts_with("prefix/test-topic/year="));
        assert!(key.contains("/month="));
        assert!(key.contains("/day="));
        assert!(key.contains("/hour="));
        assert!(key.ends_with("100.json"));
    }
}
