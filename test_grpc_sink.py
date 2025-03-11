#!/usr/bin/env python3
import grpc
import time
import json
import base64
import sys
import connector_pb2
import connector_pb2_grpc

def main():
    # Create a gRPC channel to the server
    with grpc.insecure_channel('localhost:50051') as channel:
        # Create a stub (client)
        stub = connector_pb2_grpc.ConnectorServiceStub(channel)
        
        # First, check the status of the s3-sink connector
        status_request = connector_pb2.StatusRequest(connector_name="s3-sink")
        try:
            status_response = stub.GetStatus(status_request)
            print(f"Connector status: {status_response}")
        except grpc.RpcError as e:
            print(f"Error getting connector status: {e.details()}")
        
        # Create multiple JSON records (100 to meet the flush.size in config)
        kafka_records = []
        for i in range(100):
            record_data = {
                "id": i,
                "name": f"Test Record {i}",
                "timestamp": int(time.time() * 1000),
                "data": f"This is test record {i} sent via gRPC"
            }
            
            # Convert to JSON string and then to bytes
            value_bytes = json.dumps(record_data).encode('utf-8')
            key_bytes = f"test-key-{i}".encode('utf-8')
            
            # Create a Kafka record
            kafka_record = connector_pb2.KafkaRecord(
                topic="test-topic",
                partition=0,
                offset=i,
                timestamp=int(time.time() * 1000),
                key=key_bytes,
                value=value_bytes,
                headers={"content-type": "application/json"}
            )
            kafka_records.append(kafka_record)
        
        # Create a record batch with all records
        record_batch = connector_pb2.RecordBatch(records=kafka_records)
        
        # Create a sink request with the record batch
        sink_request = connector_pb2.SinkRequest(record_batch=record_batch)
        
        # Create a flush request
        flush_request = connector_pb2.SinkRequest(flush=connector_pb2.FlushRequest())
        
        # Create a request iterator for bidirectional streaming
        def request_iterator():
            # First send the record batch
            print(f"Sending record batch: {record_data}")
            yield sink_request
            
            # Then send the flush request
            time.sleep(1)  # Small delay to ensure processing
            print("Sending flush request")
            yield flush_request
        
        # Bidirectional streaming RPC
        try:
            # Start the bidirectional stream with our request iterator
            sink_stream = stub.SinkStream(request_iterator())
            
            # Process responses
            for response in sink_stream:
                print(f"Received response: {response}")
            
        except grpc.RpcError as e:
            print(f"Error in SinkStream RPC: {e.details()}")
            sys.exit(1)
        
        # Check if the data was stored in MinIO
        print("\nData should now be stored in MinIO.")
        print("You can check the MinIO console at http://localhost:9001")
        print("Bucket: kafka-connect-bucket")
        print("Path: data/test-topic/")

if __name__ == "__main__":
    main()
