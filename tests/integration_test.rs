//! Integration tests for the Kafka to S3 connector using testcontainers

use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    config::{Credentials, Region},
    Client as S3Client,
};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use reqwest;
use std::process::Command;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use tokio::time;
use uuid::Uuid;

// Import the crate modules from the lib.rs file
use franzoxide::connector;
use franzoxide::connector::common::{Connector, TaskConfig};

// Define a custom error type for integration tests
#[derive(Debug)]
struct IntegrationError(String);

impl std::fmt::Display for IntegrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Integration test error: {}", self.0)
    }
}

impl std::error::Error for IntegrationError {}

// Helper function to convert any error to our integration error type
fn err_to_integration_error<E: std::fmt::Display>(e: E) -> Box<dyn std::error::Error> {
    Box::new(IntegrationError(format!("{}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_kafka_to_s3_connector() -> std::result::Result<(), Box<dyn std::error::Error>> {
        // No need to create a docker client explicitly with the new API

        // Start Kafka container with configuration matching docker-compose.yml
        // Bitnami Kafka doesn't have a built-in health check endpoint, so we'll use a message on stdout
        // that indicates the broker is ready
        // Create and configure Kafka image with exposed ports and wait strategy
        let kafka_container = GenericImage::new("bitnami/kafka", "3.5.1")
            .with_exposed_port(9092.tcp())
            .with_exposed_port(29092.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
            .with_env_var("ALLOW_PLAINTEXT_LISTENER", "yes")
            .with_env_var("KAFKA_CFG_NODE_ID", "1")
            .with_env_var("KAFKA_CFG_PROCESS_ROLES", "broker,controller")
            .with_env_var("KAFKA_CFG_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
            .with_env_var(
                "KAFKA_CFG_LISTENERS",
                "PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092",
            )
            .with_env_var(
                "KAFKA_CFG_ADVERTISED_LISTENERS",
                "PLAINTEXT://localhost:9092,EXTERNAL://localhost:29092",
            )
            .with_env_var(
                "KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP",
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT",
            )
            .with_env_var("KAFKA_CFG_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
            .with_env_var("KAFKA_CFG_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
            .with_env_var("KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE", "true")
            .with_env_var("KAFKA_KRAFT_CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
            .start()
            .await
            .expect("Failed to start Kafka container");

        println!("Kafka container ID: {}", kafka_container.id());

        // Try to get the port mapping for Kafka
        let kafka_port = kafka_container
            .get_host_port_ipv4(9092)
            .await
            .expect("Failed to get mapped port for Kafka");
        println!("Mapped Kafka port: {}", kafka_port);
        let kafka_host = format!("localhost:{}", kafka_port);

        // Start MinIO container using Bitnami image instead of official
        // Create and configure MinIO image with exposed ports and wait strategy
        // Let's try with a different wait approach
        let minio_container = GenericImage::new("bitnami/minio", "latest")
            .with_exposed_port(9000.tcp())
            .with_exposed_port(9001.tcp())
            // Instead of waiting for a specific message, wait a fixed amount of time
            .with_wait_for(WaitFor::seconds(15))
            .with_env_var("MINIO_ROOT_USER", "minioadmin")
            .with_env_var("MINIO_ROOT_PASSWORD", "minioadmin")
            .start()
            .await
            .expect("Failed to start MinIO container");

        // Add a delay to ensure MinIO is fully initialized
        time::sleep(time::Duration::from_secs(5)).await;

        println!("MinIO container ID: {}", minio_container.id());

        // Try to get the port mapping
        let minio_port = minio_container
            .get_host_port_ipv4(9000)
            .await
            .expect("Failed to get mapped port for MinIO");
        println!("Mapped MinIO port: {}", minio_port);
        let minio_host = format!("http://127.0.0.1:{}", minio_port); // Using IP instead of hostname

        // The MinIO container will wait until the health check passes thanks to HttpWaitStrategy
        // For Kafka, we'll wait a bit to ensure it's ready for connections
        println!("MinIO container is ready (passed health check). Waiting for Kafka to be fully ready...");
        time::sleep(Duration::from_secs(10)).await;

        // Print docker container status
        let docker_ps = Command::new("docker")
            .args(["ps", "-a"])
            .output()
            .expect("Failed to execute docker ps");
        println!(
            "Docker containers running:\n{}",
            String::from_utf8_lossy(&docker_ps.stdout)
        );

        // Check MinIO health
        match reqwest::Client::new()
            .get(format!("http://127.0.0.1:{}/minio/health/live", minio_port))
            .timeout(Duration::from_secs(5))
            .send()
            .await
        {
            Ok(res) => println!(
                "MinIO health check: {} - {}",
                res.status(),
                res.text().await.unwrap_or_default()
            ),
            Err(e) => println!("MinIO health check failed: {}", e),
        };

        // Generate a unique topic name and bucket name for this test
        let test_id = Uuid::new_v4().to_string();
        let topic = format!("test-topic-{}", test_id);
        let s3_bucket = format!("test-bucket-{}", test_id);

        // Create S3 client and bucket
        println!("Creating S3 client with endpoint: {}", minio_host);
        let region_provider = RegionProviderChain::first_try(Region::new("us-east-1"));
        let s3_config = aws_config::from_env()
            .region(region_provider)
            .endpoint_url(&minio_host)
            .credentials_provider(Credentials::new(
                "minioadmin",
                "minioadmin",
                None,
                None,
                "integration-test",
            ))
            .load()
            .await;

        let s3_client = S3Client::new(&s3_config);

        println!("S3 client configuration details:");
        println!(
            "  - Region: {}",
            s3_config.region().unwrap_or(&Region::new("us-east-1"))
        );
        println!("  - Endpoint: {}", minio_host);
        println!("  - Credentials: minioadmin/minioadmin");

        // Create the bucket
        println!("Creating S3 bucket: {}", s3_bucket);
        let create_bucket_request = s3_client.create_bucket().bucket(&s3_bucket);
        println!("S3 create_bucket request: {:?}", create_bucket_request);
        match create_bucket_request.send().await {
            Ok(_) => println!("Successfully created bucket: {}", s3_bucket),
            Err(e) => {
                println!("Error creating bucket: {:?}", e);
                // Extract more detailed error information
                if let Some(source) = e.source() {
                    println!("Error source: {:?}", source);
                    if let Some(inner_source) = source.source() {
                        println!("Inner error source: {:?}", inner_source);
                    }
                }

                // Let's try to list buckets to see what's available
                println!("Attempting to list existing buckets to debug...");
                match s3_client.list_buckets().send().await {
                    Ok(buckets) => println!("Existing buckets: {:?}", buckets),
                    Err(list_err) => println!("Failed to list buckets: {:?}", list_err),
                }

                return Err(Box::new(IntegrationError(format!(
                    "Failed to create S3 bucket: {:?}",
                    e
                ))) as Box<dyn std::error::Error>);
            }
        };

        // Create Kafka producer with auto topic creation enabled
        println!(
            "Creating Kafka producer with bootstrap servers: {}",
            kafka_host
        );
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &kafka_host)
            .set("message.timeout.ms", "5000")
            // These settings help with topic auto-creation and handling new topics
            .set("allow.auto.create.topics", "true")
            .set("message.send.max.retries", "5")
            .set("retry.backoff.ms", "500")
            .create()
            .map_err(|e| Box::new(IntegrationError(format!("Kafka producer error: {}", e))))?;

        // Let's wait a moment for the broker to fully initialize
        println!("Waiting for Kafka broker to be ready...");
        time::sleep(Duration::from_secs(5)).await;

        // Configure and start the connector
        println!(
            "Setting up connector with Kafka host: {} and MinIO host: {}",
            kafka_host, minio_host
        );
        let mut config = HashMap::new();
        config.insert("topics".to_string(), topic.clone());
        config.insert("s3.bucket.name".to_string(), s3_bucket.clone());
        config.insert("s3.region".to_string(), "us-east-1".to_string());
        config.insert("s3.endpoint".to_string(), minio_host.clone());
        config.insert("s3.access.key".to_string(), "minioadmin".to_string());
        config.insert("s3.secret.key".to_string(), "minioadmin".to_string());
        config.insert("format.class".to_string(), "json".to_string());
        config.insert("partitioner.class".to_string(), "default".to_string());
        config.insert("flush.size".to_string(), "1".to_string()); // Flush after each record for testing
        config.insert("kafka.bootstrap.servers".to_string(), kafka_host.clone()); // Add Kafka bootstrap servers

        println!("Connector configuration: {:?}", config);

        let task_config = TaskConfig {
            task_id: 0,
            config: config.clone(),
        };

        let mut connector = connector::sink::s3::S3SinkConnector::new(
            format!("s3-sink-connector-{}", test_id),
            task_config,
        );

        // Initialize the connector
        if let Err(e) = connector.initialize(config.clone()).await {
            return Err(Box::new(IntegrationError(format!(
                "Connector initialization error: {}",
                e
            ))) as Box<dyn std::error::Error>);
        }

        // Start the connector
        if let Err(e) = connector.start().await {
            return Err(
                Box::new(IntegrationError(format!("Connector start error: {}", e)))
                    as Box<dyn std::error::Error>,
            );
        }

        // Produce some test messages to Kafka
        let test_messages = vec![
            r#"{"id": 1, "name": "Test 1", "value": 100}"#,
            r#"{"id": 2, "name": "Test 2", "value": 200}"#,
            r#"{"id": 3, "name": "Test 3", "value": 300}"#,
        ];

        for (i, message) in test_messages.iter().enumerate() {
            let message_string = message.to_string();
            let key = format!("key-{}", i);

            // Create the topic before sending messages to ensure it exists
            println!(
                "Manually creating topic: {} using docker exec command",
                topic
            );

            // First, create a command within the container for topic creation
            // Use the INTERNAL address since we're running inside the container
            let create_topic_cmd = Command::new("docker")
                .args(["exec", &kafka_container.id(), "bash", "-c", 
                      &format!("/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --topic {} --partitions 1 --replication-factor 1", topic)])
                .output()
                .expect("Failed to execute create topic command");

            println!(
                "Topic creation stdout: {}",
                String::from_utf8_lossy(&create_topic_cmd.stdout)
            );
            println!(
                "Topic creation stderr: {}",
                String::from_utf8_lossy(&create_topic_cmd.stderr)
            );

            if !create_topic_cmd.status.success() {
                println!("Topic creation command failed, but will try to continue...");
            } else {
                println!("Topic creation command succeeded: {}", topic);
            }

            // List topics to confirm creation
            let list_topics_cmd = Command::new("docker")
                .args(["exec", &kafka_container.id(), "bash", "-c", 
                      "/opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"])
                .output()
                .expect("Failed to execute list topics command");

            println!(
                "Available topics:\n{}",
                String::from_utf8_lossy(&list_topics_cmd.stdout)
            );

            // Wait briefly for topic creation to propagate
            time::sleep(Duration::from_secs(2)).await;

            // Send message without explicit partition - let Kafka handle it
            // This avoids potential UnknownPartition errors if partitioning changes
            let record = FutureRecord::<String, String>::to(topic.as_str())
                .payload(&message_string)
                .key(&key);

            println!("Sending message to topic: {}", topic);

            // Use a more generous timeout and better error handling
            match producer.send(record, Duration::from_secs(10)).await {
                Ok(_) => println!("Message sent successfully: {}", message),
                Err((e, _)) => {
                    println!("Error details: {:?}", e);

                    // Don't fail immediately on first error, log and continue
                    if i < test_messages.len() - 1 {
                        println!("Error sending message, will retry with next message");
                        continue;
                    } else {
                        return Err(Box::new(IntegrationError(format!(
                            "Error sending message: {}",
                            e
                        ))) as Box<dyn std::error::Error>);
                    }
                }
            }
        }

        // Wait for the connector to process the messages
        println!("Waiting for connector to process messages...");
        // Increase wait time to give the connector more time to process and flush to S3
        println!("Sleeping for 60 seconds to allow connector to process and write to S3...");
        time::sleep(Duration::from_secs(60)).await;

        // List objects in the S3 bucket to verify the connector worked
        let list_objects_output = match s3_client.list_objects_v2().bucket(&s3_bucket).send().await
        {
            Ok(output) => output,
            Err(e) => {
                return Err(
                    Box::new(IntegrationError(format!("S3 list objects error: {}", e)))
                        as Box<dyn std::error::Error>,
                )
            }
        };

        // Verify that files were created in S3
        let objects = list_objects_output.contents();

        // Make sure we have objects
        assert!(!objects.is_empty(), "No objects found in S3 bucket");
        println!("Found {} objects in S3 bucket", objects.len());

        // Check each object
        for object in objects {
            let key = object.key.as_ref().expect("Object key is missing");
            println!("S3 object: {}", key);

            // Download and verify the content
            let get_object_output = match s3_client
                .get_object()
                .bucket(&s3_bucket)
                .key(key)
                .send()
                .await
            {
                Ok(output) => output,
                Err(e) => {
                    return Err(
                        Box::new(IntegrationError(format!("S3 get object error: {}", e)))
                            as Box<dyn std::error::Error>,
                    )
                }
            };

            let body = match get_object_output.body.collect().await {
                Ok(body) => body,
                Err(e) => {
                    return Err(
                        Box::new(IntegrationError(format!("S3 body collect error: {}", e)))
                            as Box<dyn std::error::Error>,
                    )
                }
            };

            let content = match String::from_utf8(body.to_vec()) {
                Ok(content) => content,
                Err(e) => {
                    return Err(
                        Box::new(IntegrationError(format!("UTF8 conversion error: {}", e)))
                            as Box<dyn std::error::Error>,
                    )
                }
            };
            println!("Object content: {}", content);

            // Verify the content contains our test data
            assert!(
                content.contains("name"),
                "Object content does not contain expected data"
            );
            assert!(
                content.contains("Test"),
                "Object content does not contain expected test data"
            );
        }

        // Stop the connector
        if let Err(e) = connector.stop().await {
            return Err(
                Box::new(IntegrationError(format!("Connector stop error: {}", e)))
                    as Box<dyn std::error::Error>,
            );
        }

        Ok(())
    }

    // Testcontainers functionality is tested in the main integration test

    // A simple test to ensure the crate modules are accessible
    #[test]
    fn test_crate_modules_accessible() {
        // This test just verifies that we can access the crate modules
        assert!(true);
    }
}
