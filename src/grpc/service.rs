// Pin import removed as it's unused
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::connector::manager::ConnectorManager;
use crate::kafka_connect::connector_service_server::ConnectorService;
use crate::kafka_connect::*;
use crate::utils::config::Config;

/// Implementation of the ConnectorService gRPC service
#[derive(Clone)]
pub struct ConnectorServiceImpl {
    config: Config,
    // Connector manager
    manager: Arc<Mutex<ConnectorManager>>,
}

impl ConnectorServiceImpl {
    /// Create a new instance of the connector service
    pub fn new(config: Config, manager: Arc<Mutex<ConnectorManager>>) -> Self {
        Self { config, manager }
    }
}

#[tonic::async_trait]
impl ConnectorService for ConnectorServiceImpl {
    /// Bidirectional streaming RPC for source connectors
    type SourceStreamStream = ReceiverStream<Result<SourceResponse, Status>>;

    #[allow(unused_variables)]
    async fn source_stream(
        &self,
        request: Request<tonic::Streaming<SourceRequest>>,
    ) -> Result<Response<Self::SourceStreamStream>, Status> {
        log::info!("New source connector connection established");

        let mut in_stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let manager = self.manager.clone();

        // Spawn a task to handle the incoming stream
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(req) => {
                        // Handle the request based on its type
                        match req.request {
                            Some(source_request::Request::Heartbeat(heartbeat)) => {
                                log::debug!(
                                    "Received heartbeat from source connector: {:?}",
                                    heartbeat
                                );

                                // Send a heartbeat response
                                let resp = SourceResponse {
                                    response: Some(source_response::Response::Heartbeat(
                                        Heartbeat {
                                            timestamp: chrono::Utc::now().timestamp_millis(),
                                        },
                                    )),
                                };

                                if let Err(e) = tx.send(Ok(resp)).await {
                                    log::error!("Failed to send response: {}", e);
                                    break;
                                }
                            }
                            Some(source_request::Request::Ack(ack)) => {
                                log::debug!("Received ack from source connector: {:?}", ack);
                                unimplemented!(
                                    "Source connector acknowledgment processing not implemented"
                                )
                            }
                            Some(source_request::Request::Commit(commit)) => {
                                log::debug!("Received commit from source connector: {:?}", commit);
                                unimplemented!("Source connector commit processing not implemented")
                            }
                            None => {
                                log::warn!("Received empty request from source connector");
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error receiving request from source connector: {}", e);
                        break;
                    }
                }
            }

            log::info!("Source connector connection closed");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Bidirectional streaming RPC for sink connectors
    type SinkStreamStream = ReceiverStream<Result<SinkResponse, Status>>;

    async fn sink_stream(
        &self,
        request: Request<tonic::Streaming<SinkRequest>>,
    ) -> Result<Response<Self::SinkStreamStream>, Status> {
        log::info!("New sink connector connection established");

        let mut in_stream = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(100);
        let manager = self.manager.clone();

        // Spawn a task to handle the incoming stream
        tokio::spawn(async move {
            while let Some(result) = in_stream.next().await {
                match result {
                    Ok(req) => {
                        // Handle the request based on its type
                        match req.request {
                            Some(sink_request::Request::Heartbeat(heartbeat)) => {
                                log::debug!(
                                    "Received heartbeat from sink connector: {:?}",
                                    heartbeat
                                );

                                // Send a heartbeat response
                                let resp = SinkResponse {
                                    response: Some(sink_response::Response::Heartbeat(Heartbeat {
                                        timestamp: chrono::Utc::now().timestamp_millis(),
                                    })),
                                };

                                if let Err(e) = tx.send(Ok(resp)).await {
                                    log::error!("Failed to send response: {}", e);
                                    break;
                                }
                            }
                            Some(sink_request::Request::RecordBatch(batch)) => {
                                log::debug!(
                                    "Received record batch from sink connector with {} records",
                                    batch.records.len()
                                );

                                // Process the record batch
                                // Forward the records to the sink connector
                                let manager_lock = manager.lock().await;
                                let sink_name = "s3-sink-0"; // Use the first task of the s3-sink connector

                                // Get the sink connector
                                let sink_connector =
                                    match manager_lock.sink_connectors.get(sink_name) {
                                        Some(connector) => connector.clone(),
                                        None => {
                                            log::error!("Sink connector not found: {}", sink_name);

                                            // Send an error acknowledgment
                                            let resp = SinkResponse {
                                                response: Some(sink_response::Response::Ack(
                                                    RecordAck {
                                                        record_ids: vec![],
                                                        success: false,
                                                        error_message: format!(
                                                            "Sink connector not found: {}",
                                                            sink_name
                                                        ),
                                                    },
                                                )),
                                            };

                                            if let Err(e) = tx.send(Ok(resp)).await {
                                                log::error!("Failed to send error response: {}", e);
                                                break;
                                            }

                                            continue;
                                        }
                                    };

                                // Drop the manager lock before locking the sink connector
                                drop(manager_lock);

                                // Lock the sink connector and put records
                                let mut sink = sink_connector.lock().await;
                                if let Err(e) = sink.put(batch.records.clone()).await {
                                    log::error!("Failed to put records to sink connector: {}", e);

                                    // Send an error acknowledgment
                                    let resp = SinkResponse {
                                        response: Some(sink_response::Response::Ack(RecordAck {
                                            record_ids: vec![],
                                            success: false,
                                            error_message: format!(
                                                "Failed to put records to sink connector: {}",
                                                e
                                            ),
                                        })),
                                    };

                                    if let Err(e) = tx.send(Ok(resp)).await {
                                        log::error!("Failed to send error response: {}", e);
                                        break;
                                    }

                                    continue;
                                }

                                // Send an acknowledgment
                                let record_ids = batch
                                    .records
                                    .iter()
                                    .map(|record| RecordId {
                                        topic: record.topic.clone(),
                                        partition: record.partition,
                                        offset: record.offset,
                                    })
                                    .collect::<Vec<_>>();

                                let resp = SinkResponse {
                                    response: Some(sink_response::Response::Ack(RecordAck {
                                        record_ids,
                                        success: true,
                                        error_message: String::new(),
                                    })),
                                };

                                if let Err(e) = tx.send(Ok(resp)).await {
                                    log::error!("Failed to send response: {}", e);
                                    break;
                                }
                            }
                            Some(sink_request::Request::Flush(flush)) => {
                                log::debug!(
                                    "Received flush request from sink connector: {:?}",
                                    flush
                                );

                                // Process the flush request
                                // Trigger the sink connector to flush data
                                let manager_lock = manager.lock().await;
                                let sink_name = "s3-sink-0"; // Use the first task of the s3-sink connector

                                // Get the sink connector
                                let sink_connector = match manager_lock
                                    .sink_connectors
                                    .get(sink_name)
                                {
                                    Some(connector) => connector.clone(),
                                    None => {
                                        log::error!("Sink connector not found: {}", sink_name);

                                        // Send an error response
                                        let resp = SinkResponse {
                                            response: Some(sink_response::Response::FlushResponse(
                                                FlushResponse {
                                                    request_id: flush.request_id,
                                                    success: false,
                                                    error_message: format!(
                                                        "Sink connector not found: {}",
                                                        sink_name
                                                    ),
                                                },
                                            )),
                                        };

                                        if let Err(e) = tx.send(Ok(resp)).await {
                                            log::error!("Failed to send error response: {}", e);
                                            break;
                                        }

                                        continue;
                                    }
                                };

                                // Drop the manager lock before locking the sink connector
                                drop(manager_lock);

                                // Lock the sink connector and flush
                                let mut sink = sink_connector.lock().await;
                                if let Err(e) = sink.flush().await {
                                    log::error!("Failed to flush sink connector: {}", e);

                                    // Send an error response
                                    let resp = SinkResponse {
                                        response: Some(sink_response::Response::FlushResponse(
                                            FlushResponse {
                                                request_id: flush.request_id,
                                                success: false,
                                                error_message: format!(
                                                    "Failed to flush sink connector: {}",
                                                    e
                                                ),
                                            },
                                        )),
                                    };

                                    if let Err(e) = tx.send(Ok(resp)).await {
                                        log::error!("Failed to send error response: {}", e);
                                        break;
                                    }

                                    continue;
                                }

                                // Send a flush response
                                let resp = SinkResponse {
                                    response: Some(sink_response::Response::FlushResponse(
                                        FlushResponse {
                                            request_id: flush.request_id,
                                            success: true,
                                            error_message: String::new(),
                                        },
                                    )),
                                };

                                if let Err(e) = tx.send(Ok(resp)).await {
                                    log::error!("Failed to send response: {}", e);
                                    break;
                                }
                            }
                            None => {
                                log::warn!("Received empty request from sink connector");
                            }
                        }
                    }
                    Err(e) => {
                        log::error!("Error receiving request from sink connector: {}", e);
                        break;
                    }
                }
            }

            log::info!("Sink connector connection closed");
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// Get connector configuration
    async fn get_config(
        &self,
        request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let req = request.into_inner();
        log::info!("Get config request for connector: {}", req.connector_name);

        // Find the connector configuration
        let connector_config = self
            .config
            .connectors
            .iter()
            .find(|c| c.name == req.connector_name)
            .ok_or_else(|| {
                Status::not_found(format!("Connector not found: {}", req.connector_name))
            })?;

        // Convert to the gRPC response format
        let config = ConnectorConfig {
            connector_class: connector_config.connector_class.clone(),
            name: connector_config.name.clone(),
            config: connector_config.config.clone(),
            tasks_max: connector_config.tasks_max,
        };

        Ok(Response::new(ConfigResponse {
            config: Some(config),
        }))
    }

    /// Update connector configuration
    async fn update_config(
        &self,
        request: Request<ConfigUpdateRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let req = request.into_inner();
        let config = req
            .config
            .ok_or_else(|| Status::invalid_argument("Missing connector configuration"))?;

        log::info!("Update config request for connector: {}", config.name);

        // Update the connector configuration
        unimplemented!("Connector configuration update not implemented");
    }

    /// Get connector status
    #[allow(unused_variables)]
    async fn get_status(
        &self,
        request: Request<StatusRequest>,
    ) -> Result<Response<StatusResponse>, Status> {
        let req = request.into_inner();
        log::info!("Get status request for connector: {}", req.connector_name);

        // Find the connector configuration
        let connector_config = self
            .config
            .connectors
            .iter()
            .find(|c| c.name == req.connector_name)
            .ok_or_else(|| {
                Status::not_found(format!("Connector not found: {}", req.connector_name))
            })?;

        // Get the actual status of the connector
        unimplemented!("Connector status retrieval not implemented");

        #[allow(unreachable_code)]
        let status = StatusResponse {
            state: status_response::State::Running as i32,
            worker_id: "worker-1".to_string(),
            tasks: (0..connector_config.tasks_max)
                .map(|i| TaskStatus {
                    task_id: i,
                    state: status_response::State::Running as i32,
                    worker_id: format!("worker-1-task-{}", i),
                    error_message: String::new(),
                })
                .collect(),
            error_message: String::new(),
        };

        Ok(Response::new(status))
    }
}
