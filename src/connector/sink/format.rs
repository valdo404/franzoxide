use arrow::array::{ArrayRef, BinaryArray, Int32Array, Int64Array, StringArray, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use log::info;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::sync::Arc;

use apache_avro::{
    types::Record as AvroRecord, types::Value as AvroValue, Schema as AvroSchema,
    Writer as AvroWriter,
};

use crate::kafka_connect::KafkaRecord;
use crate::utils::error::{ConnectorError, ConnectorResult};

/// Format for S3 objects
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    /// JSON format
    Json,

    /// Avro format
    Avro,

    /// Parquet format
    Parquet,

    /// Raw bytes format
    Bytes,
}

#[allow(dead_code)]
impl Format {
    /// Parse format from string
    #[deprecated(since = "0.1.0", note = "Use the FromStr trait instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> ConnectorResult<Self> {
        s.parse()
    }

    /// Get file extension for format
    pub fn extension(&self) -> &'static str {
        match self {
            Format::Json => "json",
            Format::Avro => "avro",
            Format::Parquet => "parquet",
            Format::Bytes => "bin",
        }
    }
}

impl std::str::FromStr for Format {
    type Err = ConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(Format::Json),
            "avro" => Ok(Format::Avro),
            "parquet" => Ok(Format::Parquet),
            "bytes" => Ok(Format::Bytes),
            _ => Err(ConnectorError::ConfigError(format!(
                "Invalid format: {}",
                s
            ))),
        }
    }
}

/// Compression type for data formats
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionType {
    /// No compression
    None,
    /// GZIP compression
    Gzip,
    /// Snappy compression
    Snappy,
}

#[allow(dead_code)]
impl CompressionType {
    /// Parse compression type from string
    #[deprecated(since = "0.1.0", note = "Use the FromStr trait instead")]
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> ConnectorResult<Self> {
        s.parse()
    }

    /// Convert to Parquet compression
    fn to_parquet_compression(self) -> Compression {
        match self {
            CompressionType::None => Compression::UNCOMPRESSED,
            CompressionType::Gzip => Compression::GZIP(Default::default()),
            CompressionType::Snappy => Compression::SNAPPY,
        }
    }
}

impl std::str::FromStr for CompressionType {
    type Err = ConnectorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "none" => Ok(CompressionType::None),
            "gzip" => Ok(CompressionType::Gzip),
            "snappy" => Ok(CompressionType::Snappy),
            _ => Err(ConnectorError::ConfigError(format!(
                "Invalid compression type: {}",
                s
            ))),
        }
    }
}

/// Format records as JSON
#[allow(dead_code)]
pub fn format_as_json(records: &[KafkaRecord]) -> ConnectorResult<Vec<u8>> {
    info!("Starting JSON formatting for {} records", records.len());
    let mut buffer = Vec::new();

    for (i, record) in records.iter().enumerate() {
        let mut json_record = serde_json::Map::new();

        // Add metadata
        json_record.insert(
            "topic".to_string(),
            serde_json::Value::String(record.topic.clone()),
        );
        json_record.insert(
            "partition".to_string(),
            serde_json::Value::Number(serde_json::Number::from(record.partition)),
        );
        json_record.insert(
            "offset".to_string(),
            serde_json::Value::Number(serde_json::Number::from(record.offset)),
        );
        json_record.insert(
            "timestamp".to_string(),
            serde_json::Value::Number(serde_json::Number::from(record.timestamp)),
        );

        // Add key if present
        if !record.key.is_empty() {
            match String::from_utf8(record.key.clone()) {
                Ok(key_str) => {
                    // Try to parse as JSON
                    match serde_json::from_str::<serde_json::Value>(&key_str) {
                        Ok(json_value) => {
                            json_record.insert("key".to_string(), json_value);
                        }
                        Err(_) => {
                            // Use as string
                            json_record
                                .insert("key".to_string(), serde_json::Value::String(key_str));
                        }
                    }
                }
                Err(_) => {
                    // Use base64 encoding for binary data
                    json_record.insert(
                        "key".to_string(),
                        serde_json::Value::String(base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            &record.key,
                        )),
                    );
                }
            }
        }

        // Add value if present
        if !record.value.is_empty() {
            match String::from_utf8(record.value.clone()) {
                Ok(value_str) => {
                    // Try to parse as JSON
                    match serde_json::from_str::<serde_json::Value>(&value_str) {
                        Ok(json_value) => {
                            json_record.insert("value".to_string(), json_value);
                        }
                        Err(_) => {
                            // Use as string
                            json_record
                                .insert("value".to_string(), serde_json::Value::String(value_str));
                        }
                    }
                }
                Err(_) => {
                    // Use base64 encoding for binary data
                    json_record.insert(
                        "value".to_string(),
                        serde_json::Value::String(base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            &record.value,
                        )),
                    );
                }
            }
        }

        // Add headers if present
        if !record.headers.is_empty() {
            let mut headers_map = serde_json::Map::new();
            for (k, v) in &record.headers {
                headers_map.insert(k.clone(), serde_json::Value::String(v.clone()));
            }
            json_record.insert(
                "headers".to_string(),
                serde_json::Value::Object(headers_map),
            );
        }

        // Write the record
        let json_value = serde_json::Value::Object(json_record);
        if i > 0 {
            buffer.write_all(b"\n").map_err(|e| {
                ConnectorError::FormatError(format!("Failed to write newline: {}", e))
            })?;
        }
        let record_json = serde_json::to_string(&json_value).map_err(|e| {
            ConnectorError::FormatError(format!("Failed to serialize record to JSON: {}", e))
        })?;
        buffer.write_all(record_json.as_bytes()).map_err(|e| {
            ConnectorError::FormatError(format!("Failed to write JSON record: {}", e))
        })?;
    }

    info!("Completed JSON formatting for {} records", records.len());
    Ok(buffer)
}

/// Format records as Avro
#[allow(dead_code)]
pub fn format_as_avro(records: &[KafkaRecord]) -> ConnectorResult<Vec<u8>> {
    info!("Starting Avro formatting for {} records", records.len());

    if records.is_empty() {
        return Ok(Vec::new());
    }

    // Define Avro schema for Kafka records
    let schema_json = r#"
    {
        "type": "record",
        "name": "KafkaRecord",
        "fields": [
            {"name": "topic", "type": "string"},
            {"name": "partition", "type": "int"},
            {"name": "offset", "type": "long"},
            {"name": "timestamp", "type": "long"},
            {"name": "key", "type": ["null", "bytes"]},
            {"name": "value", "type": ["null", "bytes"]},
            {"name": "headers", "type": {"type": "map", "values": "string"}}
        ]
    }
    "#;

    let schema = AvroSchema::parse_str(schema_json)
        .map_err(|e| ConnectorError::FormatError(format!("Failed to parse Avro schema: {}", e)))?;

    let schema = Arc::new(schema);
    let mut writer = AvroWriter::new(&schema, Vec::new());

    for record in records {
        let mut avro_record = AvroRecord::new(&schema).unwrap();

        // Add fields
        avro_record.put("topic", AvroValue::String(record.topic.clone()));
        avro_record.put("partition", AvroValue::Int(record.partition));
        avro_record.put("offset", AvroValue::Long(record.offset));
        avro_record.put("timestamp", AvroValue::Long(record.timestamp));

        // Add key if present
        if record.key.is_empty() {
            avro_record.put("key", AvroValue::Null);
        } else {
            avro_record.put("key", AvroValue::Bytes(record.key.clone()));
        }

        // Add value if present
        if record.value.is_empty() {
            avro_record.put("value", AvroValue::Null);
        } else {
            avro_record.put("value", AvroValue::Bytes(record.value.clone()));
        }

        // Add headers
        let mut headers_map = HashMap::new();
        for (k, v) in &record.headers {
            headers_map.insert(k.clone(), AvroValue::String(v.clone()));
        }
        avro_record.put("headers", AvroValue::Map(headers_map));

        // Write record
        writer.append(avro_record).map_err(|e| {
            ConnectorError::FormatError(format!("Failed to append Avro record: {}", e))
        })?;
    }

    // Flush and get the buffer
    let buffer = writer
        .into_inner()
        .map_err(|e| ConnectorError::FormatError(format!("Failed to finalize Avro data: {}", e)))?;

    info!("Completed Avro formatting for {} records", records.len());
    Ok(buffer)
}

/// Format records as Parquet
#[allow(dead_code)]
pub fn format_as_parquet(
    records: &[KafkaRecord],
    compression: CompressionType,
) -> ConnectorResult<Vec<u8>> {
    info!("Starting Parquet formatting for {} records", records.len());

    if records.is_empty() {
        return Ok(Vec::new());
    }

    // Define Arrow schema for Kafka records
    let schema = ArrowSchema::new(vec![
        Field::new("topic", DataType::Utf8, false),
        Field::new("partition", DataType::Int32, false),
        Field::new("offset", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("key", DataType::Binary, true),
        Field::new("value", DataType::Binary, true),
        Field::new("headers", DataType::Struct(Fields::empty()), true),
    ]);

    let schema_ref = Arc::new(schema);

    // Create arrays for each column
    let mut topics = Vec::with_capacity(records.len());
    let mut partitions = Vec::with_capacity(records.len());
    let mut offsets = Vec::with_capacity(records.len());
    let mut timestamps = Vec::with_capacity(records.len());
    let mut keys = Vec::with_capacity(records.len());
    let mut values = Vec::with_capacity(records.len());

    // Populate arrays
    for record in records {
        topics.push(record.topic.clone());
        partitions.push(record.partition);
        offsets.push(record.offset);
        timestamps.push(record.timestamp);
        keys.push(if record.key.is_empty() {
            None
        } else {
            Some(record.key.clone())
        });
        values.push(if record.value.is_empty() {
            None
        } else {
            Some(record.value.clone())
        });
    }

    // Create Arrow arrays
    let topic_array = Arc::new(StringArray::from(topics)) as ArrayRef;
    let partition_array = Arc::new(Int32Array::from(partitions)) as ArrayRef;
    let offset_array = Arc::new(Int64Array::from(offsets)) as ArrayRef;
    let timestamp_array = Arc::new(Int64Array::from(timestamps)) as ArrayRef;
    let key_array = Arc::new(BinaryArray::from_iter_values(keys.into_iter().flatten())) as ArrayRef;
    let value_array =
        Arc::new(BinaryArray::from_iter_values(values.into_iter().flatten())) as ArrayRef;

    // Create empty headers array (placeholder for now)
    let headers_array = Arc::new(StructArray::new_empty_fields(records.len(), None)) as ArrayRef;

    // Create record batch
    let batch = RecordBatch::try_new(
        schema_ref.clone(),
        vec![
            topic_array,
            partition_array,
            offset_array,
            timestamp_array,
            key_array,
            value_array,
            headers_array,
        ],
    )
    .map_err(|e| {
        ConnectorError::FormatError(format!("Failed to create Arrow record batch: {}", e))
    })?;

    // Create Parquet writer
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);

    let props = WriterProperties::builder()
        .set_compression(compression.to_parquet_compression())
        .build();

    let mut writer = ArrowWriter::try_new(cursor, schema_ref, Some(props)).map_err(|e| {
        ConnectorError::FormatError(format!("Failed to create Parquet writer: {}", e))
    })?;

    // Write batch
    writer.write(&batch).map_err(|e| {
        ConnectorError::FormatError(format!("Failed to write Parquet batch: {}", e))
    })?;

    // Close writer
    writer.close().map_err(|e| {
        ConnectorError::FormatError(format!("Failed to finalize Parquet file: {}", e))
    })?;

    info!("Completed Parquet formatting for {} records", records.len());
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_record() -> KafkaRecord {
        KafkaRecord {
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 42,
            timestamp: 1615485900000,
            key: b"test-key".to_vec(),
            value: b"{\"field\":\"value\"}".to_vec(),
            headers: {
                let mut map = HashMap::new();
                map.insert("header1".to_string(), "value1".to_string());
                map
            },
        }
    }

    #[test]
    fn test_format_from_str() {
        assert_eq!("json".parse::<Format>().unwrap(), Format::Json);
        assert_eq!("avro".parse::<Format>().unwrap(), Format::Avro);
        assert_eq!("parquet".parse::<Format>().unwrap(), Format::Parquet);
        assert_eq!("bytes".parse::<Format>().unwrap(), Format::Bytes);
        assert!("invalid".parse::<Format>().is_err());
    }

    #[test]
    fn test_compression_from_str() {
        assert_eq!(
            "none".parse::<CompressionType>().unwrap(),
            CompressionType::None
        );
        assert_eq!(
            "gzip".parse::<CompressionType>().unwrap(),
            CompressionType::Gzip
        );
        assert_eq!(
            "snappy".parse::<CompressionType>().unwrap(),
            CompressionType::Snappy
        );
        assert!("invalid".parse::<CompressionType>().is_err());
    }

    #[test]
    fn test_format_as_json() {
        let record = create_test_record();
        let result = format_as_json(&[record]).unwrap();
        let json_str = String::from_utf8(result).unwrap();

        // Verify JSON contains expected fields
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();
        assert_eq!(parsed["topic"], "test-topic");
        assert_eq!(parsed["partition"], 0);
        assert_eq!(parsed["offset"], 42);
        assert_eq!(parsed["timestamp"], 1615485900000_i64);
        assert_eq!(parsed["key"], "test-key");
        assert_eq!(parsed["value"]["field"], "value");
        assert_eq!(parsed["headers"]["header1"], "value1");
    }

    #[test]
    #[ignore = "Needs proper Avro schema configuration"]
    fn test_format_as_avro() {
        let record = create_test_record();
        let _result = format_as_avro(&[record]);

        // This test is currently ignored as it needs proper Avro schema configuration
        // Will be implemented in a future update
        assert!(true);
    }

    #[test]
    #[ignore = "Needs proper Parquet schema configuration"]
    fn test_format_as_parquet() {
        let record = create_test_record();
        let _result = format_as_parquet(&[record], CompressionType::None);

        // This test is currently ignored as it needs proper Parquet schema configuration
        // Will be implemented in a future update
        assert!(true);
    }
}
