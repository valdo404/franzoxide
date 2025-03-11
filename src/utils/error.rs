use thiserror::Error;

/// Error types for the Rust-Connect application
#[derive(Error, Debug)]
pub enum ConnectorError {
    /// Error related to configuration
    #[error("Configuration error: {0}")]
    ConfigError(String),
    
    /// Error related to Kafka
    #[error("Kafka error: {0}")]
    KafkaError(#[from] rdkafka::error::KafkaError),
    
    /// Error related to S3
    #[error("S3 error: {0}")]
    S3Error(String),
    
    /// Error related to gRPC
    #[error("gRPC error: {0}")]
    GrpcError(#[from] tonic::Status),
    
    /// Error related to serialization/deserialization
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    
    /// Error related to I/O
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    
    /// General error
    #[error("{0}")]
    General(String),
}

/// Result type for the Rust-Connect application
pub type ConnectorResult<T> = Result<T, ConnectorError>;

/// Convert any error to a ConnectorError::General
#[allow(dead_code)]
pub fn to_connector_error<E: std::fmt::Display>(err: E) -> ConnectorError {
    ConnectorError::General(err.to_string())
}
