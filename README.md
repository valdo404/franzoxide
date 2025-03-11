# Rust Connect

A Kafka Connect clone written in Rust with gRPC interface.

## Project Description

Rust Connect is a high-performance alternative to Kafka Connect, implemented in Rust. It aims to provide similar functionality with better performance and resource efficiency.

## Coming Soon

- Kafka source connector
- S3 sink connector
- gRPC API for connector management
- Docker support for easy deployment
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
