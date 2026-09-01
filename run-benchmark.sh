#!/bin/bash
# Scheduler POC Benchmark Runner

set -e

MODE=${1:-benchmark}
SCHEDULER=${2:-both}
JOBS=${3:-200000}

echo "=========================================="
echo "Scheduler POC Benchmark"
echo "Mode: $MODE"
echo "Scheduler: $SCHEDULER"
echo "Jobs: $JOBS"
echo "=========================================="

# Build
echo "Building project..."
mvn clean install -DskipTests -q

# Run based on mode
case $MODE in
    "benchmark")
        echo "Running benchmark mode..."
        java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
            --benchmark.mode=benchmark \
            --benchmark.scheduler=$SCHEDULER \
            --benchmark.job-count=$JOBS
        ;;
    "kafka-produce")
        echo "Producing messages to Kafka..."
        java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
            --benchmark.mode=kafka-produce \
            --benchmark.scheduler=$SCHEDULER \
            --benchmark.job-count=$JOBS
        ;;
    "kafka-consume")
        echo "Starting Kafka consumer mode..."
        java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
            --benchmark.mode=kafka-consume \
            --benchmark.scheduler=$SCHEDULER
        ;;
    "distribution-test")
        echo "Running distribution test..."
        java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
            --benchmark.mode=distribution-test \
            --benchmark.scheduler=$SCHEDULER \
            --benchmark.job-count=$JOBS
        ;;
    "recovery-test")
        echo "Running recovery test..."
        java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
            --benchmark.mode=recovery-test \
            --benchmark.scheduler=$SCHEDULER \
            --benchmark.job-count=$JOBS
        ;;
    *)
        echo "Unknown mode: $MODE"
        echo "Usage: $0 [benchmark|kafka-produce|kafka-consume|distribution-test|recovery-test] [quartz|db-scheduler|both] [job-count]"
        exit 1
        ;;
esac