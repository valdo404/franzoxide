// Franzoxide - A Kafka Connect clone written in Rust
// This file exports the modules for use in integration tests

// Include the proto module with file descriptor set for reflection
pub mod proto;
pub use proto::kafka_connect;

// Re-export the modules
pub mod connector;
pub mod grpc;
pub mod utils;
