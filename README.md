# Scheduler POC: Quartz vs DB-Scheduler

Evaluation of a **cluster-aware distributed scheduler** for one-time jobs fired at a
future time, at high scale — target **~1.4M jobs/day with same-timestamp daily spikes**
(modelled as a 200,000-job same-instant spike), with Kafka result publishing.

**Verdict: adopt db-scheduler.** It scales near-linearly to ~8,700 jobs/sec on 3 nodes;
Quartz's clustered job store serializes on a global lock and tops out ~25–30× lower.
Full write-up in [BENCHMARK_FINDINGS.md](BENCHMARK_FINDINGS.md).

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Kafka     │────▶│  Scheduler  │────▶│   Kafka     │
│  (Input)    │     │  (Quartz/   │     │  (Output)   │
│             │     │  DB-Sched)  │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
                           │
                    ┌──────┴──────┐
                    │ PostgreSQL  │  ← shared state; the scaling bottleneck
                    │  (Shared)   │
                    └─────────────┘
```

---

## Quick Start

### 1. Start infrastructure
```bash
docker compose up -d
```
Brings up PostgreSQL, Kafka, Prometheus, Grafana, plus `kafka-exporter` and
`postgres-exporter`. Wait ~30s for health.

### 2. Build
```bash
mvn clean install -DskipTests
```

### 3. Run a benchmark
```bash
# db-scheduler, 200k same-instant spike
java -jar benchmark/target/benchmark-1.0.0-SNAPSHOT.jar \
  --benchmark.scheduler=db-scheduler \
  --benchmark.job-count=200000 \
  --benchmark.fire-delay-seconds=30 \
  --db-scheduler.scheduler-name=node-1
```
`--benchmark.scheduler` accepts `quartz | db-scheduler | both`.
Add `--benchmark.keep-alive=true` to keep the app up after the run so Prometheus keeps
scraping it.

### 4. Observe
- **Grafana** — http://localhost:3000 (admin/admin) → dashboard **"Scheduler POC"**
  (scheduling & fire rate, latency percentiles, per-node split, Kafka + PostgreSQL rows)
- **Prometheus** — http://localhost:9090
- **Kafka UI** — http://localhost:8085

### 5. Stress test (horizontal scaling step-up)
```bash
./stress-test.sh          # steps node count 1..4 at a fixed 200k spike
JOBS=200000 ./stress-test.sh 3
```

### 6. Cleanup
```bash
docker compose down        # keep data volumes
docker compose down -v     # also wipe volumes
```

---

## Steps taken

1. **Fixed the broken POC.** The benchmark hung on a `CountDownLatch` that was never
   counted down (→ connection exhaustion from stuck JVMs); rewrote completion detection.
   Added true scheduler isolation via Spring `@Profile`, fixed CLI arg parsing, trimmed the
   Hikari pools. Made db-scheduler actually execute (registered a real `Task` bean and
   corrected the `scheduled_tasks` schema — it used the wrong column names). Confirmed the
   "jobs not persisting" report was a misread of an unused partitioned table.
2. **Fair head-to-head.** Aligned both schedulers at 50 worker threads, enabled
   db-scheduler `lock-and-fetch`, and made Quartz load in standby + raised its misfire
   threshold so a spike drains via normal acquisition instead of the slow misfire handler.
3. **Full observability.** App exposes a merged `/all-metrics` (JVM/Hikari + scheduler
   counters); added `kafka-exporter` and `postgres-exporter` and a side-by-side Grafana
   dashboard (scheduler + Kafka + Postgres rows).
4. **Scaling stress test.** `stress-test.sh` steps node count 1→4 at a 200k spike,
   recording throughput and peak DB pressure per step.
5. **Sustained-churn tuning.** Aggressive per-table autovacuum on `scheduled_tasks`.
6. **Resilience test.** `kill -9` mid-drain + restart, verifying recovery semantics.

---

## Observations (measured)

Single local host, ~10ms simulated job body, PostgreSQL 16 (`max_connections=200`),
50 threads and a 60-connection Hikari pool per node. Numbers are **comparative/directional**.

### Head-to-head (200k spike, 50 threads each)
| Metric | Quartz 2.3 | DB-Scheduler 15.6 |
|---|---|---|
| Enqueue 200k | per-job insert (no bulk API) | bulk `scheduleBatch` ~3.4s |
| Fire throughput (1 node) | ~80–100/sec* | ~2,400–3,333/sec |
| Execution latency p99 | — | ~13.6 ms |
| 200k same-instant spike | impractical (~30+ min) | ~3.4s + ~60s drain, 0 failures |
| Cluster fetch | global lock (`QRTZ_LOCKS`), serialized | `SELECT … FOR UPDATE SKIP LOCKED` |

\*Quartz collapsed to ~7/sec under a naive misfire storm; ~80–100/sec is after tuning — still lock-bound.

### db-scheduler horizontal scaling (200k spike)
| Nodes | Schedule | Drain | Throughput | Peak conns | Peak dead tuples |
|------:|---------:|------:|-----------:|-----------:|-----------------:|
| 1 | 3.3s | 60s | 3,333/s | 52/200 | 346k |
| 2 | 3.4s | 32s | 6,250/s | 104/200 | 327k |
| 3 | 3.7s | 23s | **8,695/s** | 157/200 | 386k |
| 4 | 3.6s | 141s | **1,418/s** 💥 | **200/200** | 400k |

- Near-linear to **3 nodes**; **collapses at 4** — 4 × 60 = 240 connections > `max_connections=200`,
  so workers starve. **The limit is Postgres connections, not the scheduler.**
- Load splits evenly across nodes with zero coordination (2-node: 99,542 / 100,458).

### Database churn — the real governor
`scheduled_tasks` produces **~350–400k dead tuples per 200k drain** (delete ~3.5k rows/sec).
On Postgres defaults this bloats over time; aggressive per-table autovacuum keeps it bounded.

### Restart / crash recovery
- **Pending jobs** are durable — survive any restart; a restarted node resumes and drains them.
- **In-flight jobs** at a crash are detected dead (stale heartbeat) and **revived** → **at-least-once**
  (job bodies must be idempotent). Recovery speed = `heartbeat-interval × missed-heartbeats-limit`
  (default ≈ tens of minutes; tune down).
- **Gotcha:** every instance needs a **unique `db-scheduler.scheduler-name`** — sharing one
  (the default, on the same host) deadlocks the cluster. `missed-heartbeats-limit` minimum is 4.

---

## Next steps

1. **Sustained load / soak test** — steady ~16–50 jobs/sec for hours; watch dead-tuple and
   table-size panels stay flat (not yet run; the spike tests cover bursts only).
2. **Connection pooling for >3 nodes** — front Postgres with **PgBouncer** (transaction
   pooling) or lower per-node pools / raise `max_connections`; re-run `stress-test.sh` past 4 nodes.
3. **Realistic job bodies** — replace the 10ms sleep with representative work + Kafka I/O to
   get production-shaped latency/throughput.
4. **Dedicated infrastructure** — separate DB host, real network hops, multiple Kafka brokers;
   re-measure absolutes.
5. **Failure injection** — Postgres failover/restart, network partitions, PgBouncer restarts.
6. **Priority & retry behavior** — exercise db-scheduler priorities and failure/retry handlers
   for the real job mix.

---

## Reference
- [BENCHMARK_FINDINGS.md](BENCHMARK_FINDINGS.md) — detailed findings & recommendations
- `stress-test.sh` — horizontal-scaling step-up harness
- `HANDOFF_SUMMARY.md` — original issues and fixes
