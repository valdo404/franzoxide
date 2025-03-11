use log::{debug, error, info};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};

use crate::connector::common::{ConnectorState, SinkConnector, SourceConnector, TaskConfig};
use crate::connector::sink::s3::S3SinkConnectorFactory;
use crate::connector::source::kafka::KafkaSourceConnectorFactory;
use crate::kafka_connect::KafkaRecord;
use crate::utils::config::{Config, ConnectorConfig, ConnectorType};
use crate::utils::error::{ConnectorError, ConnectorResult};

/// Manager for connectors
pub struct ConnectorManager {
    /// Global configuration
    config: Config,

    /// Source connectors
    source_connectors: HashMap<String, Arc<Mutex<Box<dyn SourceConnector + Send>>>>,

    /// Sink connectors
    pub sink_connectors: HashMap<String, Arc<Mutex<Box<dyn SinkConnector + Send>>>>,

    /// Record channels
    record_channels: HashMap<String, mpsc::Sender<Vec<KafkaRecord>>>,
}

impl ConnectorManager {
    /// Create a new connector manager
    pub fn new(config: Config) -> Self {
        Self {
            config,
            source_connectors: HashMap::new(),
            sink_connectors: HashMap::new(),
            record_channels: HashMap::new(),
        }
    }

    /// Initialize all connectors
    pub async fn initialize(&mut self) -> ConnectorResult<()> {
        info!("Initializing connectors");

        // Collect connector configs first to avoid borrow issues
        let connector_configs: Vec<_> = self.config.connectors.clone();

        // Initialize each connector
        for connector_config in connector_configs {
            self.initialize_connector(&connector_config).await?;
        }

        Ok(())
    }

    /// Initialize a single connector
    async fn initialize_connector(
        &mut self,
        connector_config: &ConnectorConfig,
    ) -> ConnectorResult<()> {
        info!("Initializing connector: {}", connector_config.name);

        match connector_config.connector_type {
            ConnectorType::Source => {
                self.initialize_source_connector(connector_config).await?;
            }
            ConnectorType::Sink => {
                self.initialize_sink_connector(connector_config).await?;
            }
        }

        Ok(())
    }

    /// Initialize a source connector
    async fn initialize_source_connector(
        &mut self,
        connector_config: &ConnectorConfig,
    ) -> ConnectorResult<()> {
        let name = connector_config.name.clone();
        let connector_class = connector_config.connector_class.clone();

        // Create task configs
        let tasks = (0..connector_config.tasks_max)
            .map(|i| TaskConfig {
                task_id: i,
                config: connector_config.config.clone(),
            })
            .collect::<Vec<_>>();

        // Create source connector based on connector class
        for task_config in tasks {
            let task_name = format!("{}-{}", name, task_config.task_id);

            let source_connector: Box<dyn SourceConnector + Send> = match connector_class.as_str() {
                "io.rustconnect.KafkaSourceConnector" => {
                    // Create Kafka source connector
                    let factory = KafkaSourceConnectorFactory::new();
                    let mut connector = factory.create_task(task_name.clone(), task_config).await?;

                    // Create channel for records
                    let (tx, rx) = mpsc::channel(1000);
                    self.record_channels.insert(task_name.clone(), tx.clone());

                    // Set record sender
                    connector.set_record_sender(tx);

                    // Create sink for this source
                    self.create_sink_for_source(task_name.clone(), rx).await?;

                    Box::new(connector)
                }
                _ => {
                    return Err(ConnectorError::ConfigError(format!(
                        "Unsupported source connector class: {}",
                        connector_class
                    )));
                }
            };

            // Store the connector
            self.source_connectors
                .insert(task_name, Arc::new(Mutex::new(source_connector)));
        }

        Ok(())
    }

    /// Initialize a sink connector
    async fn initialize_sink_connector(
        &mut self,
        connector_config: &ConnectorConfig,
    ) -> ConnectorResult<()> {
        let name = connector_config.name.clone();
        let connector_class = connector_config.connector_class.clone();

        // Create task configs
        let tasks = (0..connector_config.tasks_max)
            .map(|i| TaskConfig {
                task_id: i,
                config: connector_config.config.clone(),
            })
            .collect::<Vec<_>>();

        // Create sink connector based on connector class
        for task_config in tasks {
            let task_name = format!("{}-{}", name, task_config.task_id);

            let sink_connector: Box<dyn SinkConnector + Send> = match connector_class.as_str() {
                "io.rustconnect.S3SinkConnector" => {
                    // Create S3 sink connector
                    let factory = S3SinkConnectorFactory::new();
                    let connector = factory.create_task(task_name.clone(), task_config).await?;
                    Box::new(connector)
                }
                _ => {
                    return Err(ConnectorError::ConfigError(format!(
                        "Unsupported sink connector class: {}",
                        connector_class
                    )));
                }
            };

            // Store the connector
            self.sink_connectors
                .insert(task_name, Arc::new(Mutex::new(sink_connector)));
        }

        Ok(())
    }

    /// Create a sink for a source connector
    async fn create_sink_for_source(
        &mut self,
        source_name: String,
        mut rx: mpsc::Receiver<Vec<KafkaRecord>>,
    ) -> ConnectorResult<()> {
        // Find a sink connector to use
        if self.sink_connectors.is_empty() {
            return Err(ConnectorError::ConfigError(
                "No sink connectors available".to_string(),
            ));
        }

        // Use the first sink connector
        let sink_name = self.sink_connectors.keys().next().unwrap().clone();
        let sink_connector = self.sink_connectors.get(&sink_name).unwrap().clone();

        info!("Connecting source {} to sink {}", source_name, sink_name);

        // Start a task to forward records from source to sink
        tokio::spawn(async move {
            while let Some(records) = rx.recv().await {
                debug!(
                    "Forwarding {} records from {} to {}",
                    records.len(),
                    source_name,
                    sink_name
                );

                let mut sink = sink_connector.lock().await;
                if let Err(e) = sink.put(records).await {
                    error!("Error putting records to sink {}: {}", sink_name, e);
                }
            }
        });

        Ok(())
    }

    /// Start all connectors
    pub async fn start(&mut self) -> ConnectorResult<()> {
        info!("Starting connectors");

        // Start sink connectors first
        for (name, connector) in &self.sink_connectors {
            info!("Starting sink connector: {}", name);
            let mut connector = connector.lock().await;
            connector.start().await?;
        }

        // Then start source connectors
        for (name, connector) in &self.source_connectors {
            info!("Starting source connector: {}", name);
            let mut connector = connector.lock().await;
            connector.start().await?;
        }

        Ok(())
    }

    /// Stop all connectors
    pub async fn stop(&mut self) -> ConnectorResult<()> {
        info!("Stopping connectors");

        // Stop source connectors first
        for (name, connector) in &self.source_connectors {
            info!("Stopping source connector: {}", name);
            let mut connector = connector.lock().await;
            connector.stop().await?;
        }

        // Then stop sink connectors
        for (name, connector) in &self.sink_connectors {
            info!("Stopping sink connector: {}", name);
            let mut connector = connector.lock().await;
            connector.stop().await?;
        }

        Ok(())
    }

    /// Get the status of all connectors
    pub async fn status(&self) -> HashMap<String, ConnectorState> {
        let mut status = HashMap::new();

        // Get status of source connectors
        for (name, connector) in &self.source_connectors {
            let connector = connector.lock().await;
            status.insert(name.clone(), connector.state().await);
        }

        // Get status of sink connectors
        for (name, connector) in &self.sink_connectors {
            let connector = connector.lock().await;
            status.insert(name.clone(), connector.state().await);
        }

        status
    }
}
