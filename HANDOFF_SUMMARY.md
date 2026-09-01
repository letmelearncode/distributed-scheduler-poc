# Scheduler POC - Handoff Summary

## Project Overview
**Goal**: Compare Quartz vs DB-Scheduler for 200k concurrent jobs scheduled at same timestamp daily with Kafka integration.

---

## ✅ Completed Implementation

### Project Structure (Maven Multi-Module)
```
scheduler-poc/
├── pom.xml                          # Parent POM with dependency management
├── docker-compose.yml               # PostgreSQL + Kafka + Prometheus + Grafana
├── run-benchmark.sh                 # Unified runner script
├── quartz-module/                   # Quartz implementation
├── db-scheduler-module/             # DB-Scheduler implementation  
├── benchmark/                       # Benchmark runner
└── init-scripts/                    # SQL schemas (62 tables total)
```

### Quartz Module
- **QuartzConfig.java**: Clustered Quartz with JDBC JobStoreTX, 100 threads, PostgreSQL
- **QuartzJob.java**: Spring-managed job with metrics + Kafka result producer
- **QuartzSchedulerService.java**: Bulk scheduling with cleanup (`clearAllJobs()`)
- **Kafka integration**: Producer/Consumer with separate beans, DLQ support

### DB-Scheduler Module (v15.6.0)
- **DbSchedulerConfig.java**: SchedulerBuilder with ExecutionHandler
- **DbSchedulerJob.java**: Task executor with metrics
- **DbSchedulerService.java**: `scheduleBatch()` + cleanup
- **Separate Kafka beans** to avoid conflicts

### Benchmark Runner
- **5 Modes**: `benchmark`, `kafka-produce`, `kafka-consume`, `distribution-test`, `recovery-test`
- **Dynamic auto-configuration exclusion** via System properties
- **ResourceMonitor**: CPU, Heap, GC, Threads
- **Prometheus metrics** for both schedulers

### Infrastructure (Docker Compose)
- PostgreSQL (max_connections=200)
- Kafka (12 partitions per topic)
- Prometheus + Grafana (14-panel dashboard)
- All schemas auto-initialized (62 tables with partitioning)

---

## ✅ Resolved Issues (fixed 2026-09-01)

Validated end-to-end on a fresh DB: `quartz` 500 jobs and `db-scheduler` 100 jobs
each schedule, execute every job, and exit cleanly (0 failures, connections return
to baseline, no leaked JVMs).

1. **Benchmark hangs** *(root cause)* — `waitForCompletion()` built a
   `CountDownLatch(expectedCount)` that **nothing ever counted down**, so it always
   blocked the full 30-min timeout. Rewritten to poll the cumulative executed+failed
   count with progress logging + a 120s stall timeout.
   (See `BenchmarkApplication.waitForCompletion`, `QuartzMetrics/DbSchedulerJob.getCompletedCount`.)
2. **PostgreSQL connection exhaustion** — caused by (a) hung benchmark JVMs from #1
   never releasing pools, and (b) Quartz opening **two** Hikari pools + no scheduler
   isolation. Fixed by #1, by profile isolation (#4 below), and by trimming the pool
   (`maximum-pool-size` 50 → 30, `minimum-idle` 10 → 5).
3. **"Jobs not persisting" (`qrtz_triggers_partitioned` = 0)** — **misdiagnosis.**
   Quartz's JDBC JobStore only writes `qrtz_triggers` (verified: 179k rows from the
   killed run). The `*_partitioned` tables are wired to nothing — annotated as
   reference-only in `init-scripts/01-quartz-schema.sql`.
4. **Scheduler isolation ("both initialize in single mode")** — two bugs:
   (a) `run-benchmark.sh` passes `--benchmark.scheduler=quartz` but `main()` only
   parsed the space form, so exclusion never fired; and (b) the hand-written
   `@Configuration`/`@Service` beans aren't auto-config, so excluding auto-config
   couldn't stop them. Fixed by parsing both arg forms and gating every bean with
   Spring `@Profile("quartz")` / `@Profile("db-scheduler")`, activated from the arg.

### Also fixed (db-scheduler never actually worked before)

5. **No executable task** — only a stray `ExecutionHandler` bean existed; the starter
   never registered a `Task`, so nothing ran. Added a real
   `OneTimeTask<DbJobData>` "benchmark-job" bean (`DbSchedulerConfig`).
6. **Wrong table schema** — `scheduled_tasks` used `instance_id` / `data JSONB`, but
   db-scheduler's SQL requires `task_instance` / `task_data BYTEA`, so every insert
   failed. Corrected in `init-scripts/02-db-scheduler-schema.sql`.
7. **Double-start** — the Spring Boot starter auto-starts the scheduler and the
   benchmark called `start()` again (no internal guard). `DbSchedulerService.start()`
   now checks `getSchedulerState().isStarted()` first.

### Note on the recovery-test mode
db-scheduler's `stop()` shuts executors down permanently, so its stop-then-start
recovery path can't truly restart in-process. Left as a known limitation.

---

### Quick Start for Testing
```bash
docker-compose up -d
mvn clean install -DskipTests
./run-benchmark.sh benchmark quartz 100
```

### Key Files to Investigate
- `benchmark/src/main/java/.../BenchmarkApplication.java` - Main entry, auto-config exclusion logic
- `quartz-module/src/main/java/.../QuartzSchedulerService.java` - Scheduling + cleanup
- `db-scheduler-module/src/main/java/.../DbSchedulerService.java` - Batch scheduling
- `docker-compose.yml` - Infrastructure config

---

### Architecture Decision Points
- **Quartz**: 11 tables, complex clustering, mature but heavy
- **DB-Scheduler**: 3 tables, built-in partitioning, simpler API, Spring Boot native
- Both use **16 hash partitions** for 200k jobs
- Kafka topics: `scheduled-jobs-input/output/dlq` (12 partitions)

The project is ~90% complete - mainly needs debugging the auto-configuration and connection pooling issues.