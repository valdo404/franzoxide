use anyhow::Result;
use log::{error, info};
use std::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
// Removed unused tokio::io imports
use futures_util::stream::Stream;
use tokio::sync::Mutex;
use tonic::transport::Server;

// Include the generated proto code
pub mod kafka_connect {
    tonic::include_proto!("kafka.connect");
}

// Import modules
mod connector;
mod grpc;
mod utils;

use connector::manager::ConnectorManager;
use grpc::service::ConnectorServiceImpl;

/// Helper struct to convert UnixListener to a Stream
pub struct UnixIncoming {
    listener: tokio::net::UnixListener,
}

impl UnixIncoming {
    pub fn new(listener: tokio::net::UnixListener) -> Self {
        Self { listener }
    }
}

impl Stream for UnixIncoming {
    type Item = Result<tokio::net::UnixStream, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let pin = self.get_mut();
        match Pin::new(&mut pin.listener).poll_accept(cx) {
            Poll::Ready(Ok((stream, _addr))) => Poll::Ready(Some(Ok(stream))),
            Poll::Ready(Err(e)) => Poll::Ready(Some(Err(e))),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logger
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    info!("Starting Rust-Connect - Kafka Connect Clone");

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let config_path = args
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or("config/connect.json");

    // Load configuration
    info!("Loading configuration from {}", config_path);
    let config = match utils::config::load_config(config_path) {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to load configuration: {}", e);
            return Err(anyhow::anyhow!("Failed to load configuration: {}", e));
        }
    };

    // Initialize connector manager
    let manager = Arc::new(Mutex::new(ConnectorManager::new(config.clone())));

    // Initialize connectors
    {
        let mut manager = manager.lock().await;
        if let Err(e) = manager.initialize().await {
            error!("Failed to initialize connectors: {}", e);
            return Err(anyhow::anyhow!("Failed to initialize connectors: {}", e));
        }
    }

    // Set up gRPC server
    let connector_service = ConnectorServiceImpl::new(config.clone(), manager.clone());

    // Start connectors
    {
        let mut manager = manager.lock().await;
        if let Err(e) = manager.start().await {
            error!("Failed to start connectors: {}", e);
            return Err(anyhow::anyhow!("Failed to start connectors: {}", e));
        }
    }

    // Start TCP server if configured
    if let Some(tcp_addr) = config.tcp_address {
        let addr: SocketAddr = tcp_addr.parse()?;
        info!("Starting gRPC server on {}", addr);

        let tcp_server = Server::builder()
            .add_service(
                kafka_connect::connector_service_server::ConnectorServiceServer::new(
                    connector_service.clone(),
                ),
            )
            .serve(addr);

        tokio::spawn(async move {
            if let Err(e) = tcp_server.await {
                error!("gRPC server error: {}", e);
            }
        });
    }

    // Start Unix socket server if configured
    if let Some(unix_socket_path) = config.unix_socket_path {
        let path = Path::new(&unix_socket_path);
        info!("Starting gRPC server on Unix socket {}", unix_socket_path);

        // Remove socket file if it already exists
        if path.exists() {
            std::fs::remove_file(path)?;
        }

        let uds_server = tonic::transport::Server::builder()
            .add_service(
                kafka_connect::connector_service_server::ConnectorServiceServer::new(
                    connector_service,
                ),
            )
            .serve_with_incoming(UnixIncoming::new(tokio::net::UnixListener::bind(path)?));

        tokio::spawn(async move {
            if let Err(e) = uds_server.await {
                error!("Unix socket gRPC server error: {}", e);
            }
        });
    }

    // Print status information
    {
        let manager_ref = manager.clone();
        let manager = manager_ref.lock().await;
        let status = manager.status().await;
        info!("Connector status: {:?}", status);
    }

    // Keep the main thread alive
    tokio::signal::ctrl_c().await?;
    info!("Shutting down Rust-Connect");

    // Stop connectors
    {
        let mut manager = manager.lock().await;
        if let Err(e) = manager.stop().await {
            error!("Failed to stop connectors: {}", e);
        }
    }

    Ok(())
}
