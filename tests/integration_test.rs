//! Integration tests for the Kafka to S3 connector

use aws_sdk_s3::{Client as S3Client, config::Region};
use aws_config::meta::region::RegionProviderChain;
use aws_credential_types::Credentials;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::config::ClientConfig;
use std::time::Duration;
use std::env;
use tokio::time;
use tonic::transport::Channel;
use uuid::Uuid;

// Import the generated gRPC client code
use rust_connect::connector_client::ConnectorClient;
use rust_connect::{ConnectorConfig, StartConnectorRequest, StopConnectorRequest};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn test_kafka_to_s3_connector() -> Result<()> {
    // Read environment variables
    let kafka_bootstrap_servers = env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "kafka:9092".to_string());
    let s3_endpoint = env::var("S3_ENDPOINT").unwrap_or_else(|_| "http://minio:9000".to_string());
    let s3_access_key = env::var("S3_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let s3_secret_key = env::var("S3_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
    let s3_bucket = env::var("S3_BUCKET").unwrap_or_else(|_| "kafka-connect-bucket".to_string());
    let rust_connect_endpoint = env::var("RUST_CONNECT_ENDPOINT").unwrap_or_else(|_| "http://rust-connect:50051".to_string());
    
    // Generate a unique topic name for this test
    let test_id = Uuid::new_v4().to_string();
    let topic = format!("test-topic-{}", test_id);
    
    // Create Kafka producer
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &kafka_bootstrap_servers)
        .set("message.timeout.ms", "5000")
        .create()?;
    
    // Create S3 client
    let region_provider = RegionProviderChain::first_try(Region::new("us-east-1"));
    let s3_config = aws_config::from_env()
        .region(region_provider)
        .endpoint_url(s3_endpoint)
        .credentials_provider(Credentials::new(
            s3_access_key,
            s3_secret_key,
            None,
            None,
            "integration-test",
        ))
        .load()
        .await;
    
    let s3_client = S3Client::new(&s3_config);
    
    // Connect to the Rust Connect service
    let channel = Channel::from_shared(rust_connect_endpoint)?
        .connect()
        .await?;
    let mut client = ConnectorClient::new(channel);
    
    // Configure and start the connector
    let connector_config = ConnectorConfig {
        name: format!("s3-sink-connector-{}", test_id),
        class: "S3SinkConnector".to_string(),
        config: vec![
            ("topics".to_string(), topic.clone()),
            ("s3.bucket".to_string(), s3_bucket.clone()),
            ("s3.region".to_string(), "us-east-1".to_string()),
            ("s3.endpoint".to_string(), s3_endpoint),
            ("s3.access.key".to_string(), s3_access_key),
            ("s3.secret.key".to_string(), s3_secret_key),
            ("format".to_string(), "json".to_string()),
            ("partitioner".to_string(), "default".to_string()),
            ("flush.size".to_string(), "1".to_string()), // Flush after each record for testing
        ].into_iter().collect(),
    };
    
    let start_request = StartConnectorRequest {
        config: Some(connector_config.clone()),
    };
    
    let start_response = client.start_connector(start_request).await?;
    println!("Started connector: {:?}", start_response);
    
    // Produce some test messages to Kafka
    let test_messages = vec![
        r#"{"id": 1, "name": "Test 1", "value": 100}"#,
        r#"{"id": 2, "name": "Test 2", "value": 200}"#,
        r#"{"id": 3, "name": "Test 3", "value": 300}"#,
    ];
    
    for (i, message) in test_messages.iter().enumerate() {
        let record = FutureRecord::to(topic.as_str())
            .payload(message)
            .key(format!("key-{}", i));
        
        producer.send(record, Duration::from_secs(0))
            .await
            .map_err(|e| Box::new(e.0) as Box<dyn std::error::Error>)?;
        println!("Produced message: {}", message);
    }
    
    // Wait for the connector to process the messages
    println!("Waiting for connector to process messages...");
    time::sleep(Duration::from_secs(10)).await;
    
    // List objects in the S3 bucket to verify the connector worked
    let list_objects_output = s3_client
        .list_objects_v2()
        .bucket(&s3_bucket)
        .prefix(&topic)
        .send()
        .await?;
    
    // Verify that files were created in S3
    if let Some(contents) = list_objects_output.contents() {
        println!("Found {} objects in S3 bucket", contents.len());
        for object in contents {
            println!("S3 object: {:?}", object.key());
            
            // Download and verify the content
            if let Some(key) = &object.key {
                let get_object_output = s3_client
                    .get_object()
                    .bucket(&s3_bucket)
                    .key(key)
                    .send()
                    .await?;
                
                let body = get_object_output.body.collect().await?;
                let content = String::from_utf8(body.to_vec())?;
                println!("Object content: {}", content);
            }
        }
        
        // Assert that we have at least one object
        assert!(!contents.is_empty(), "No objects found in S3 bucket");
    } else {
        panic!("No objects found in S3 bucket");
    }
    
    // Stop the connector
    let stop_request = StopConnectorRequest {
        name: connector_config.name,
    };
    
    let stop_response = client.stop_connector(stop_request).await?;
    println!("Stopped connector: {:?}", stop_response);
    
    Ok(())
}
