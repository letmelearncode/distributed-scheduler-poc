#!/bin/bash
# Kafka topic initialization script

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
cub kafka-ready -b kafka:9092 1 40

# Create topics with 12 partitions for parallelism
kafka-topics --create --topic scheduled-jobs-input --partitions 12 --replication-factor 1 --bootstrap-server kafka:9092
kafka-topics --create --topic scheduled-jobs-output --partitions 12 --replication-factor 1 --bootstrap-server kafka:9092
kafka-topics --create --topic scheduled-jobs-dlq --partitions 12 --replication-factor 1 --bootstrap-server kafka:9092

# List topics
kafka-topics --list --bootstrap-server kafka:9092

echo "Kafka topics created successfully"