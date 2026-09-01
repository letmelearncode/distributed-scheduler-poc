# Scheduler POC — Benchmark Findings

**Question:** which cluster-aware distributed scheduler fits one-time jobs fired at a
future time, at high scale (~1.4M jobs/day, with same-timestamp daily spikes)?

**Answer:** **db-scheduler** (com.github.kagkarlsson), by a wide margin. Quartz's clustered
JDBC job store serializes on a global lock and cannot keep up with a large same-instant
spike; db-scheduler uses `SELECT ... FOR UPDATE SKIP LOCKED` and scales near-linearly
across nodes until Postgres connections become the limit.

All numbers below are from this POC on a single local Postgres 16 + a 12-partition Kafka,
50 worker threads per node, ~10ms simulated job body.

---

## 1. Head-to-head (fairly configured, 50 threads each)

| | Scheduling (enqueue) | Firing / drain rate | 200k same-instant spike |
|---|---|---|---|
| **Quartz** | per-job insert, no bulk API | **~80–100 jobs/sec** | impractical (~30+ min) |
| **DB-Scheduler** | bulk `scheduleBatch` (~3–7s for 200k) | **~2,400–3,300 jobs/sec** (1 node) | 3.4s schedule + ~30–60s drain, 0 failures |

Why the ~25–30× firing gap:
- **Quartz** clustered `JobStoreTX` acquires triggers under a single global row lock
  (`QRTZ_LOCKS.TRIGGER_ACCESS`); every acquire cycle is serialized cluster-wide. A large
  backlog past its misfire threshold also drops onto the slow MisfireHandler (default 20 at
  a time) and collapses to ~7/sec. Raising `misfireThreshold` and batch sizes lifts it to
  ~100/sec — still lock-bound.
- **DB-Scheduler** picks with `FOR UPDATE SKIP LOCKED`, so multiple threads/nodes claim
  disjoint batches concurrently with no global lock, and enqueues in bulk.

---

## 2. Horizontal scaling step-up (fixed 200k spike, step node count)

Run with `./stress-test.sh`. Each node adds a Hikari pool of 60 connections.

| Nodes | Schedule | Drain | Throughput | Peak conns | Peak dead tuples |
|------:|---------:|------:|-----------:|-----------:|-----------------:|
| 1 | 3.3s | 60s | 3,333/s | 52/200 | 346k |
| 2 | 3.4s | 32s | 6,250/s | 104/200 | 327k |
| 3 | 3.7s | 23s | **8,695/s** | 157/200 | 386k |
| 4 | 3.6s | 141s | **1,418/s** 💥 | **200/200** | 400k |

- **Near-linear to 3 nodes** (~2.6× on 3 nodes, ~8,700 jobs/sec).
- **4 nodes collapses**: 4 × 60 = 240 connections requested vs `max_connections=200`. The
  pool saturates at 200 and worker threads starve waiting for connections (Hikari
  `connectionTimeout` thrash, not a clean error), so throughput drops *below* a single node.

**The bottleneck is Postgres connections, not the scheduler.**

---

## 3. The real scaling governor: `scheduled_tasks` churn

`scheduled_tasks` is a high-churn queue table (insert-on-schedule, update-on-pick,
delete-on-complete). A single 200k drain produces ~350–400k dead tuples. At 1.4M/day the
default autovacuum (fires at 20% dead, throttled) would let the table bloat and slow the
pick/delete over time.

Mitigation applied (see `init-scripts/02-db-scheduler-schema.sql`): per-table "queue table"
autovacuum — `autovacuum_vacuum_scale_factor=0` + `threshold=5000`, `cost_delay=0`,
`fillfactor=80`. With this, dead tuples stayed bounded (~330–400k) across the entire step-up
instead of growing unbounded.

---

## 4. Recommendations for production

- **Use db-scheduler.** Enable the high-throughput `lock-and-fetch` polling strategy
  (`db-scheduler.polling-strategy=lock-and-fetch`).
- **Give every instance a unique `db-scheduler.scheduler-name`** (e.g. from hostname/pod
  name). Two instances sharing a name (the default, when on the same host) deadlock the
  pick/heartbeat logic — this stalled the cluster completely until fixed.
- **Plan connections, not just nodes.** ~3 app nodes per Postgres at a 60-connection pool.
  To scale further: lower the per-node pool, raise `max_connections`, or front Postgres
  with **PgBouncer** (transaction pooling).
- **Tune autovacuum on `scheduled_tasks`** aggressively (above); monitor dead tuples and
  table size for creep.
- ~8,700 jobs/sec on 3 nodes is far above the ~16 jobs/sec average of 1.4M/day, and drains
  even a full-day-in-one-spike (200k) in ~23s — comfortable headroom.

---

## 5. Observability (how these numbers were captured)

`docker compose up -d` brings up Postgres, Kafka, Prometheus, Grafana, plus a
`kafka-exporter` and `postgres-exporter`. The benchmark app exposes a merged
`/all-metrics` endpoint (JVM/Hikari + `quartz.*`/`dbscheduler.*`). Grafana dashboard
**"Scheduler POC"** compares both schedulers side-by-side (scheduling & fire rate, latency
percentiles, per-node load split) and includes Kafka and PostgreSQL rows (connections vs
max, spike backlog, dead vs live tuples, tuple churn, table size, longest txn).

- Grafana: http://localhost:3000 (admin/admin)
- Prometheus: http://localhost:9090
- Kafka UI: http://localhost:8085
