use async_trait::async_trait;
use std::collections::HashMap;

use crate::kafka_connect::KafkaRecord;
use crate::utils::error::ConnectorResult;

/// Common trait for all connectors
#[async_trait]
pub trait Connector {
    /// Get the name of the connector
    #[allow(dead_code)]
    fn name(&self) -> &str;

    /// Initialize the connector with the given configuration
    async fn initialize(&mut self, config: HashMap<String, String>) -> ConnectorResult<()>;

    /// Start the connector
    async fn start(&mut self) -> ConnectorResult<()>;

    /// Stop the connector
    async fn stop(&mut self) -> ConnectorResult<()>;

    /// Get the current state of the connector
    async fn state(&self) -> ConnectorState;
}

/// State of a connector
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ConnectorState {
    /// The connector is not initialized
    Uninitialized,

    /// The connector is running
    Running,

    /// The connector is paused
    Paused,

    /// The connector has failed
    Failed,

    /// The connector is stopped
    Stopped,
}

/// Trait for source connectors
#[async_trait]
pub trait SourceConnector: Connector {
    /// Poll for records from the source system
    #[allow(dead_code)]
    async fn poll(&mut self) -> ConnectorResult<Vec<KafkaRecord>>;

    /// Commit offsets for the given records
    #[allow(dead_code)]
    async fn commit(&mut self, offsets: Vec<(String, i32, i64)>) -> ConnectorResult<()>;
}

/// Trait for sink connectors
#[async_trait]
pub trait SinkConnector: Connector {
    /// Put records to the sink system
    async fn put(&mut self, records: Vec<KafkaRecord>) -> ConnectorResult<()>;

    /// Flush any buffered records to the sink system
    #[allow(dead_code)]
    async fn flush(&mut self) -> ConnectorResult<()>;
}

/// Task configuration for a connector
#[derive(Debug, Clone)]
pub struct TaskConfig {
    /// Task ID
    pub task_id: i32,

    /// Configuration for the task
    pub config: HashMap<String, String>,
}

/// Factory trait for creating connector tasks
#[async_trait]
#[allow(dead_code)]
pub trait ConnectorTaskFactory {
    /// Create a source connector task
    async fn create_source_task(
        &self,
        config: TaskConfig,
    ) -> ConnectorResult<Box<dyn SourceConnector>>;

    /// Create a sink connector task
    async fn create_sink_task(&self, config: TaskConfig)
        -> ConnectorResult<Box<dyn SinkConnector>>;
}
