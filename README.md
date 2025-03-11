# Franzoxide

A high-performance Kafka Connect clone written in Rust.

## Project Description

Franzoxide is a high-performance alternative to Kafka Connect, implemented in Rust. It connects Kafka with S3 storage and aims to provide similar functionality with better performance and resource efficiency. The name combines "Franz" (referencing Franz Kafka) with "oxide" (a nod to Rust, as rust is iron oxide).

## Features

- Kafka source connector to read data from Kafka topics
- S3 sink connector to write data to S3-compatible storage
- gRPC API for connector management
- Support for both TCP and Unix socket communication
- Docker support for easy deployment and testing

## Requirements

- Rust 1.70 or later
- Docker and Docker Compose (for containerized deployment)
- Kafka cluster
- S3-compatible storage (e.g., MinIO, AWS S3)

## Building from Source

```bash
# Clone the repository
git clone https://github.com/valdo404/rust-connect.git
cd rust-connect

# Build the project
cargo build --release
```

## Running with Docker

The easiest way to run Rust Connect is using Docker Compose, which sets up a complete environment including Kafka (in KRaft mode without Zookeeper), Schema Registry, and MinIO (S3-compatible storage).

```bash
# Start the services
docker-compose up -d

# Check the logs
docker-compose logs -f rust-connect

# Stop the services
docker-compose down
```

## Running Integration Tests

Integration tests are included to verify the functionality of the Kafka to S3 connector flow:

```bash
# Run the integration tests
docker-compose run --rm rust-connect-test
```

## Configuration

The default configuration is in `config/connect.json`. You can modify this file to change the connector settings:

```json
{
  "tcp_address": "0.0.0.0:50051",
  "unix_socket_path": "/tmp/rust-connect.sock",
  "kafka": {
    "bootstrap_servers": ["kafka:9092"],
    "group_id": "rust-connect",
    "properties": {
      "auto.offset.reset": "earliest",
      "enable.auto.commit": "false"
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
        "s3.bucket.name": "kafka-connect-bucket",
        "s3.region": "us-east-1",
        "s3.endpoint": "http://minio:9000",
        "s3.access.key": "minioadmin",
        "s3.secret.key": "minioadmin",
        "s3.prefix": "data",
        "format.class": "json",
        "partitioner.class": "default",
        "flush.size": "100"
      }
    }
  ]
}
```

## Architecture

Rust Connect follows a modular architecture:

- **Connector Manager**: Manages the lifecycle of connectors
- **Source Connectors**: Read data from external systems (e.g., Kafka)
- **Sink Connectors**: Write data to external systems (e.g., S3)
- **gRPC API**: Provides a management interface for connectors

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
