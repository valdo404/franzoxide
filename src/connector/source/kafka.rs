use async_trait::async_trait;
use log::{debug, error, info, warn};
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{
    BaseConsumer, CommitMode, Consumer, ConsumerContext, DefaultConsumerContext, Rebalance,
    StreamConsumer,
};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time;

use crate::connector::common::{Connector, ConnectorState, SourceConnector, TaskConfig};
use crate::kafka_connect::KafkaRecord;
use crate::utils::error::{ConnectorError, ConnectorResult};

/// Custom context for Kafka consumer
struct KafkaSourceContext;

impl ClientContext for KafkaSourceContext {}

impl ConsumerContext for KafkaSourceContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Pre rebalance: {:?}", rebalance);
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance) {
        info!("Post rebalance: {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => debug!("Offsets committed successfully"),
            Err(e) => warn!("Error committing offsets: {}", e),
        }
    }
}

type LoggingConsumer = StreamConsumer<KafkaSourceContext>;

/// Kafka Source Connector implementation
pub struct KafkaSourceConnector {
    /// Name of the connector
    name: String,

    /// Configuration for the connector
    config: HashMap<String, String>,

    /// Kafka consumer
    consumer: Option<LoggingConsumer>,

    /// Current state of the connector
    state: ConnectorState,

    /// Topics to consume from
    topics: Vec<String>,

    /// Channel for sending records to the sink
    record_tx: Option<mpsc::Sender<Vec<KafkaRecord>>>,

    /// Poll timeout in milliseconds
    poll_timeout_ms: u64,

    /// Batch size for polling records
    batch_size: usize,

    /// Offset commits
    offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
}

impl KafkaSourceConnector {
    /// Create a new Kafka source connector
    pub fn new(name: String, task_config: TaskConfig) -> Self {
        Self {
            name,
            config: task_config.config,
            consumer: None,
            state: ConnectorState::Uninitialized,
            topics: Vec::new(),
            record_tx: None,
            poll_timeout_ms: 100,
            batch_size: 100,
            offsets: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Set the channel for sending records to the sink
    pub fn set_record_sender(&mut self, tx: mpsc::Sender<Vec<KafkaRecord>>) {
        self.record_tx = Some(tx);
    }

    /// Convert a Kafka message to a KafkaRecord
    fn message_to_record(&self, msg: &BorrowedMessage) -> KafkaRecord {
        let topic = msg.topic().to_string();
        let partition = msg.partition();
        let offset = msg.offset();
        let timestamp = msg.timestamp().to_millis().unwrap_or(0);

        // Extract key
        let key = msg.key().map(|k| k.to_vec()).unwrap_or_default();

        // Extract value
        let value = msg.payload().map(|p| p.to_vec()).unwrap_or_default();

        // Extract headers
        let mut headers = HashMap::new();
        if let Some(hdrs) = msg.headers() {
            for i in 0..hdrs.count() {
                let header = hdrs.get(i);
                let name = header.key;
                if let Some(value) = header.value {
                    if let Ok(value_str) = std::str::from_utf8(value) {
                        headers.insert(name.to_string(), value_str.to_string());
                    }
                }
            }
        }

        KafkaRecord {
            topic,
            partition,
            offset,
            timestamp,
            key,
            value,
            headers,
        }
    }

    /// Poll for records from Kafka
    async fn poll_records(&self) -> ConnectorResult<Vec<KafkaRecord>> {
        let consumer = self
            .consumer
            .as_ref()
            .ok_or_else(|| ConnectorError::General("Kafka consumer not initialized".to_string()))?;

        let mut records = Vec::with_capacity(self.batch_size);

        for _ in 0..self.batch_size {
            match time::timeout(Duration::from_millis(self.poll_timeout_ms), consumer.recv()).await
            {
                Ok(Ok(msg)) => {
                    debug!(
                        "Received message: {}:{}:{}",
                        msg.topic(),
                        msg.partition(),
                        msg.offset()
                    );

                    // Store the offset
                    {
                        let mut offsets = self.offsets.lock().unwrap();
                        offsets.insert(
                            (msg.topic().to_string(), msg.partition()),
                            msg.offset() + 1, // Store next offset to commit
                        );
                    }

                    // Convert to KafkaRecord
                    let record = self.message_to_record(&msg);
                    records.push(record);
                }
                Ok(Err(e)) => {
                    error!("Error while receiving from Kafka: {}", e);
                    break;
                }
                Err(_) => {
                    // Timeout, break the loop
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Start the polling loop
    #[allow(dead_code)]
    async fn start_polling_loop(&self) -> ConnectorResult<()> {
        let record_tx = match &self.record_tx {
            Some(tx) => tx.clone(),
            None => return Err(ConnectorError::General("Record sender not set".to_string())),
        };

        let offsets = self.offsets.clone();
        // For the consumer, we need to use a different approach
        // Instead of trying to clone it, we'll use a copy of the original consumer's configuration
        let consumer = match &self.consumer {
            Some(_) => {
                // Since we can't easily clone the consumer, we'll create a new one with the same topics
                let mut client_config = ClientConfig::new();
                for (key, value) in &self.config {
                    client_config.set(key, value);
                }

                let consumer: StreamConsumer<DefaultConsumerContext> =
                    client_config.create().map_err(ConnectorError::KafkaError)?;

                let topic_strs: Vec<&str> = self.topics.iter().map(|s| s.as_str()).collect();
                consumer
                    .subscribe(&topic_strs)
                    .map_err(ConnectorError::KafkaError)?;

                consumer
            }
            None => {
                return Err(ConnectorError::General(
                    "Kafka consumer not initialized".to_string(),
                ))
            }
        };

        // Spawn a task to poll for records
        tokio::spawn(async move {
            loop {
                match Self::poll_records_static(&consumer, offsets.clone()).await {
                    Ok(records) => {
                        if !records.is_empty() {
                            if let Err(e) = record_tx.send(records).await {
                                error!("Error sending records to sink: {}", e);
                                break;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error polling records: {}", e);
                        break;
                    }
                }

                // Commit offsets
                if let Err(e) = Self::commit_offsets_static(&consumer, offsets.clone()).await {
                    error!("Error committing offsets: {}", e);
                }

                // Sleep for a short time to avoid busy waiting
                time::sleep(Duration::from_millis(100)).await;
            }
        });

        Ok(())
    }

    /// Static method to poll for records (used in the polling loop)
    #[allow(dead_code)]
    async fn poll_records_static(
        consumer: &StreamConsumer<DefaultConsumerContext>,
        offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    ) -> ConnectorResult<Vec<KafkaRecord>> {
        let mut records = Vec::new();

        for _ in 0..100 {
            match time::timeout(Duration::from_millis(100), consumer.recv()).await {
                Ok(Ok(msg)) => {
                    debug!(
                        "Received message: {}:{}:{}",
                        msg.topic(),
                        msg.partition(),
                        msg.offset()
                    );

                    // Store the offset
                    {
                        let mut offsets = offsets.lock().unwrap();
                        offsets.insert(
                            (msg.topic().to_string(), msg.partition()),
                            msg.offset() + 1, // Store next offset to commit
                        );
                    }

                    // Convert to KafkaRecord
                    let record = Self::message_to_record_static(&msg);
                    records.push(record);
                }
                Ok(Err(e)) => {
                    error!("Error while receiving from Kafka: {}", e);
                    break;
                }
                Err(_) => {
                    // Timeout, break the loop
                    break;
                }
            }
        }

        Ok(records)
    }

    /// Static method to convert a Kafka message to a KafkaRecord (used in the polling loop)
    #[allow(dead_code)]
    fn message_to_record_static(msg: &BorrowedMessage) -> KafkaRecord {
        let topic = msg.topic().to_string();
        let partition = msg.partition();
        let offset = msg.offset();
        let timestamp = msg.timestamp().to_millis().unwrap_or(0);

        // Extract key
        let key = msg.key().map(|k| k.to_vec()).unwrap_or_default();

        // Extract value
        let value = msg.payload().map(|p| p.to_vec()).unwrap_or_default();

        // Extract headers
        let mut headers = HashMap::new();
        if let Some(hdrs) = msg.headers() {
            for i in 0..hdrs.count() {
                let header = hdrs.get(i);
                let name = header.key;
                if let Some(value) = header.value {
                    if let Ok(value_str) = std::str::from_utf8(value) {
                        headers.insert(name.to_string(), value_str.to_string());
                    }
                }
            }
        }

        KafkaRecord {
            topic,
            partition,
            offset,
            timestamp,
            key,
            value,
            headers,
        }
    }

    /// Static method to commit offsets (used in the polling loop)
    #[allow(dead_code)]
    async fn commit_offsets_static(
        consumer: &StreamConsumer<DefaultConsumerContext>,
        offsets: Arc<Mutex<HashMap<(String, i32), i64>>>,
    ) -> ConnectorResult<()> {
        let offsets_to_commit = {
            let offsets = offsets.lock().unwrap();
            offsets.clone()
        };

        if offsets_to_commit.is_empty() {
            return Ok(());
        }

        let mut tpl = TopicPartitionList::new();
        for ((topic, partition), offset) in offsets_to_commit {
            tpl.add_partition_offset(&topic, partition, Offset::Offset(offset))
                .map_err(ConnectorError::KafkaError)?;
        }

        consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(ConnectorError::KafkaError)?;

        Ok(())
    }
}

#[async_trait]
impl Connector for KafkaSourceConnector {
    fn name(&self) -> &str {
        &self.name
    }

    async fn initialize(&mut self, config: HashMap<String, String>) -> ConnectorResult<()> {
        // Store configuration
        self.config = config.clone();

        // Extract topics from config
        if let Some(topics) = config.get("topics") {
            self.topics = topics
                .split(',')
                .map(|s| s.trim().to_string())
                .collect::<Vec<_>>();
        } else {
            return Err(ConnectorError::ConfigError(
                "Missing required config: topics".to_string(),
            ));
        }

        // Extract poll timeout
        if let Some(timeout) = config.get("poll.timeout.ms") {
            self.poll_timeout_ms = timeout.parse().map_err(|_| {
                ConnectorError::ConfigError(format!("Invalid poll.timeout.ms value: {}", timeout))
            })?;
        }

        // Extract batch size
        if let Some(batch_size) = config.get("batch.size") {
            self.batch_size = batch_size.parse().map_err(|_| {
                ConnectorError::ConfigError(format!("Invalid batch.size value: {}", batch_size))
            })?;
        }

        // Create Kafka consumer
        let mut client_config = ClientConfig::new();

        // Set client.id if not provided
        if !config.contains_key("client.id") {
            client_config.set("client.id", format!("franzoxide-{}", self.name));
        }

        // Set group.id if not provided
        if !config.contains_key("group.id") {
            client_config.set("group.id", format!("franzoxide-{}", self.name));
        }

        // Set auto.offset.reset if not provided
        if !config.contains_key("auto.offset.reset") {
            client_config.set("auto.offset.reset", "earliest");
        }

        // Set enable.auto.commit to false
        client_config.set("enable.auto.commit", "false");

        // Copy all other config
        for (key, value) in &config {
            if key != "topics" && key != "poll.timeout.ms" && key != "batch.size" {
                client_config.set(key, value);
            }
        }

        // Set log level
        client_config.set_log_level(RDKafkaLogLevel::Debug);

        // Create consumer with custom context
        let consumer: LoggingConsumer = client_config
            .create_with_context(KafkaSourceContext)
            .map_err(ConnectorError::KafkaError)?;

        // Subscribe to topics
        let topic_strs: Vec<&str> = self.topics.iter().map(|s| s.as_str()).collect();
        consumer
            .subscribe(&topic_strs)
            .map_err(ConnectorError::KafkaError)?;

        // Store consumer
        self.consumer = Some(consumer);

        // Update state
        self.state = ConnectorState::Uninitialized;

        Ok(())
    }

    async fn start(&mut self) -> ConnectorResult<()> {
        if self.state != ConnectorState::Uninitialized {
            return Err(ConnectorError::General(
                "Connector not initialized".to_string(),
            ));
        }

        self.state = ConnectorState::Running;
        Ok(())
    }

    async fn stop(&mut self) -> ConnectorResult<()> {
        if self.state != ConnectorState::Running {
            return Err(ConnectorError::General("Connector not running".to_string()));
        }

        self.state = ConnectorState::Stopped;
        Ok(())
    }

    async fn state(&self) -> ConnectorState {
        self.state
    }
}

#[async_trait]
impl SourceConnector for KafkaSourceConnector {
    async fn poll(&mut self) -> ConnectorResult<Vec<KafkaRecord>> {
        self.poll_records().await
    }

    async fn commit(&mut self, offsets: Vec<(String, i32, i64)>) -> ConnectorResult<()> {
        let consumer = match &self.consumer {
            Some(c) => c,
            None => {
                return Err(ConnectorError::General(
                    "Kafka consumer not initialized".to_string(),
                ))
            }
        };

        let mut tpl = TopicPartitionList::new();
        for (topic, partition, offset) in offsets {
            tpl.add_partition_offset(&topic, partition, Offset::Offset(offset))
                .map_err(ConnectorError::KafkaError)?;
        }

        consumer
            .commit(&tpl, CommitMode::Async)
            .map_err(ConnectorError::KafkaError)?;

        Ok(())
    }
}

/// Factory for creating Kafka source connector tasks
pub struct KafkaSourceConnectorFactory;

impl Default for KafkaSourceConnectorFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl KafkaSourceConnectorFactory {
    /// Create a new Kafka source connector factory
    pub fn new() -> Self {
        Self
    }

    /// Create a new Kafka source connector task
    pub async fn create_task(
        &self,
        name: String,
        task_config: TaskConfig,
    ) -> ConnectorResult<KafkaSourceConnector> {
        let mut connector = KafkaSourceConnector::new(name, task_config.clone());
        connector.initialize(task_config.config).await?;
        Ok(connector)
    }
}
