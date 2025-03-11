# Franzoxide Gap Analysis

This document identifies the gaps between the current implementation of Franzoxide and the target feature set for S3 sink connectors, focusing on Parquet partitioning and Apache Iceberg integration.

## Feature Gap Analysis

| Priority | Feature | Description | Status | Complexity |
|----------|---------|-------------|--------|------------|
| 1 | Time-Based Partitioning | Implement TimeBasedPartitioner to create Hive-compatible partitions (year/month/day/hour) | Not Started | Medium |
| 2 | Parquet File Generation | Support for writing Parquet files with schema information and compression | Partial | High |
| 3 | S3 Upload Management | Configurable flush strategies and atomic file operations | Partial | Medium |
| 4 | Schema Registry Integration | Support for AWS Glue Schema Registry for schema evolution | Not Started | High |
| 5 | Partition Management | Direct management of partitions without relying on external crawlers | Not Started | Medium |
| 6 | Iceberg Basic Support | Implement basic Apache Iceberg table format support | Not Started | Very High |
| 7 | Exactly-Once Semantics | Ensure records are written exactly once, even during failures | Not Started | High |
| 8 | AWS Glue Catalog Integration | Integration with AWS Glue Data Catalog for table management | Not Started | Medium |
| 9 | Multi-Table Fan-Out | Support routing different records to different tables | Not Started | Medium |
| 10 | Schema Evolution | Automatic handling of schema changes in streaming data | Not Started | High |
| 11 | Commit Coordination | Implement commit coordination through Kafka control topics | Not Started | High |

## Implementation Roadmap

### Phase 1: Basic Parquet Partitioning
- Implement TimeBasedPartitioner
- Complete Parquet file generation with compression options
- Enhance S3 upload management with configurable flush strategies

### Phase 2: Schema Management
- Add AWS Glue Schema Registry integration
- Implement schema evolution capabilities
- Implement direct partition management and registration

### Phase 3: Iceberg Integration
- Implement basic Apache Iceberg table format support
- Add AWS Glue Catalog integration
- Implement commit coordination through Kafka

### Phase 4: Advanced Features
- Add multi-table fan-out capabilities
- Implement exactly-once semantics

## Technical Challenges

1. **Rust Ecosystem Maturity**: Limited Rust libraries for Iceberg compared to Java
2. **Memory Management**: Efficient buffering and memory management in Rust
3. **AWS Integration**: Proper integration with AWS services (S3, Glue) from Rust
4. **Schema Evolution**: Handling complex schema changes in a type-safe language like Rust
5. **Performance Optimization**: Ensuring the Rust implementation outperforms the Java version

## Comparison with Java Kafka Connect

| Feature | Java Kafka Connect | Rust Connect (Current) | Rust Connect (Target) |
|---------|-------------------|------------------------|------------------------|
| Parquet Support | Full | Partial | Full |
| Partitioning Schemes | Multiple | Basic | Multiple |
| Iceberg Support | Available via connector | Not available | Full support |
| Schema Registry | Multiple options | Not implemented | AWS Glue Schema Registry |
| Performance | Good | Better | Significantly better |
| Memory Footprint | High | Low | Low |
| Exactly-Once Semantics | Supported | Not implemented | Supported |
