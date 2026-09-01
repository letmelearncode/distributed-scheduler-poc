# Scheduler POC: Quartz vs DB-Scheduler

Benchmark comparison for 200,000 concurrent jobs scheduled at the same timestamp daily with Kafka integration.

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kafka     │────▶│  Scheduler  │────▶│   Kafka     │
│  (Input)    │     │  (Quartz/   │     │  (Output)   │
│             │     │  DB-Sched)  │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │ PostgreSQL  │
                    │  (Shared)   │
                    └─────────────┘
```

## Quick Start

### 1. Start Infrastructure

```bash
docker-compose up -d
```

Wait for all services to be healthy (~30 seconds).

### 2. Build Project

```bash
mvn clean install -DskipTests
```

### 3. Run Benchmarks

#### Run Quartz Only
```bash
java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
  --benchmark.scheduler=quartz \
  --benchmark.job-count=200000
```

#### Run DB-Scheduler Only
```bash
java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
  --benchmark.scheduler=db-scheduler \
  --benchmark.job-count=200000
```

#### Run Both (Sequential)
```bash
java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
  --benchmark.scheduler=both \
  --benchmark.job-count=200000
```

### 4. View Metrics

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)

## Key Configuration

### Job Count
```bash
--benchmark.job-count=200000
```

### Scheduler Type
```bash
--benchmark.scheduler=both|quartz|db-scheduler
```

### Kafka (for future integration)
```bash
--benchmark.kafka.bootstrap-servers=localhost:9092
```

## Expected Results Comparison

| Metric | Quartz | DB-Scheduler | Winner |
|--------|--------|--------------|--------|
| Schedule 200k jobs | ~45-60s | ~15-25s | DB-Scheduler |
| p99 Schedule Latency | ~200-500ms | ~50-150ms | DB-Scheduler |
| p99 Execution Latency | ~50-100ms | ~30-80ms | DB-Scheduler |
| DB Connections | 100+ | 50-80 | DB-Scheduler |
| Schema Complexity | 11 tables | 3 tables | DB-Scheduler |
| Recovery Time | ~10-15s | ~3-5s | DB-Scheduler |
| Operational Overhead | High | Low | DB-Scheduler |

## Kafka Integration Pattern

Both schedulers support the same Kafka integration pattern:

```java
// 1. Consume from Kafka
ConsumerRecords<String, JobPayload> records = consumer.poll(Duration.ofMillis(100));

// 2. Schedule jobs
records.forEach(record -> {
    scheduler.schedule(record.value().getJobId(), 
                       record.value().getPayload(), 
                       record.value().getScheduledTime());
});

// 3. Job executes → Produce to Kafka
public void execute(JobContext ctx) {
    // Process job
    String result = process(ctx.getPayload());
    
    // Produce result to output topic
    producer.send(new ProducerRecord<>("job-results", ctx.getJobId(), result));
}
```

## Partitioning Strategy for 200k Jobs

Both implementations use hash-based partitioning (16 partitions):

```sql
-- Partition key: hash(job_id) % 16
-- Distributes load evenly across partitions
-- Enables parallel scheduling/execution
```

## Cleanup

```bash
docker-compose down -v
```

## Next Steps

1. Run benchmarks with different job counts (50k, 100k, 200k, 500k)
2. Test failure scenarios (scheduler restart, DB failover)
3. Add Kafka producer/consumer implementation
4. Test with actual business logic in jobs
5. Compare resource utilization (CPU, Memory, Network)