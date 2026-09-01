#!/bin/bash
# Horizontal-scaling step-up stress test for the db-scheduler cluster.
#
# Holds the spike size fixed and steps the node count 1..MAX_NODES, running one
# "primary" (schedules the spike + executes) and (n-1) "worker" nodes (execute only),
# all sharing one Postgres via SELECT ... FOR UPDATE SKIP LOCKED. For each step it
# records schedule time, drain window, effective throughput, and peak DB pressure
# (dead tuples, connections, delete rate) sampled from the postgres-exporter.
#
# Prereqs: docker compose stack up (postgres + prometheus + exporters) and the
# benchmark jar built (mvn -q clean install -DskipTests).
#
# Usage:   ./stress-test.sh [max_nodes]
#   JOBS=200000 FIRE_DELAY=15 ./stress-test.sh 4
set +e

PROJ="$(cd "$(dirname "$0")" && pwd)"
JAR="$PROJ/benchmark/target/benchmark-1.0.0-SNAPSHOT.jar"
LOGDIR="${TMPDIR:-/tmp}/scheduler-stress"
mkdir -p "$LOGDIR"
RES="$LOGDIR/stress-results.txt"

N="${JOBS:-200000}"
FIRE="${FIRE_DELAY:-15}"
MAX_NODES="${1:-4}"
PGC="scheduler-poc-postgres"
DB="scheduler_poc"

[ -f "$JAR" ] || { echo "Build the jar first: mvn -q clean install -DskipTests"; exit 1; }
: > "$RES"

pq(){ curl -s "http://localhost:9090/api/v1/query" --data-urlencode "query=$1" 2>/dev/null | python3 -c "import sys,json
try:
 d=json.load(sys.stdin);r=d['data']['result'];print(int(float(r[0]['value'][1])) if r else 0)
except: print(0)"; }
qcount(){ docker exec "$PGC" psql -U postgres -d "$DB" -t -A -c "SELECT count(*) FROM scheduled_tasks;" 2>/dev/null | tr -d '[:space:]'; }

NODES=1
while [ "$NODES" -le "$MAX_NODES" ]; do
  echo "===== STEP: $NODES node(s), spike=$N ====="
  pkill -9 -f "benchmark-1.0.0-SNAPSHOT.jar" 2>/dev/null; sleep 6

  # start workers 2..NODES (job-count=1 so they allocate no payloads)
  w=2
  while [ "$w" -le "$NODES" ]; do
    port=$((8079 + w))
    nohup java -Xmx1g -jar "$JAR" --benchmark.mode=kafka-consume --benchmark.scheduler=db-scheduler \
      --benchmark.job-count=1 --db-scheduler.scheduler-name=node-$w --server.port=$port > "$LOGDIR/w$w-$NODES.log" 2>&1 &
    w=$((w+1))
  done
  w=2
  while [ "$w" -le "$NODES" ]; do
    port=$((8079 + w))
    for i in $(seq 1 40); do [ "$(curl -s -o /dev/null -w '%{http_code}' http://localhost:$port/all-metrics 2>/dev/null)" = "200" ] && break; sleep 2; done
    w=$((w+1))
  done

  # start primary
  PLOG="$LOGDIR/primary-$NODES.log"
  nohup java -Xmx2g -jar "$JAR" --benchmark.mode=benchmark --benchmark.scheduler=db-scheduler \
    --benchmark.job-count=$N --benchmark.fire-delay-seconds=$FIRE --benchmark.keep-alive=false \
    --db-scheduler.scheduler-name=node-1 --server.port=8080 > "$PLOG" 2>&1 &
  for i in $(seq 1 90); do grep -qa "Scheduled $N jobs" "$PLOG" 2>/dev/null && break; sleep 1; done
  sched=$(grep -a "Scheduled $N jobs" "$PLOG" | grep -oE '[0-9]+ms' | head -1)

  peak_dead=0; peak_conn=0; peak_del=0; drain_start=-1; t0=$(date +%s)
  while true; do
    el=$(( $(date +%s) - t0 ))
    rem=$(qcount); [ -z "$rem" ] && rem=-1
    dead=$(pq 'scheduler_table_dead_tuples'); conn=$(pq 'scheduler_conn_total'); del=$(pq 'rate(pg_stat_database_tup_deleted{datname="scheduler_poc"}[1m])')
    [ "$dead" -gt "$peak_dead" ] && peak_dead=$dead
    [ "$conn" -gt "$peak_conn" ] && peak_conn=$conn
    [ "$del" -gt "$peak_del" ] && peak_del=$del
    [ "$rem" != "$N" ] && [ "$rem" -ge 0 ] 2>/dev/null && [ "$drain_start" = "-1" ] && drain_start=$el
    if [ "$rem" = "0" ] 2>/dev/null; then dend=$el; break; fi
    if [ "$el" -gt 300 ]; then dend=-1; break; fi
    sleep 3
  done

  if [ "$dend" = "-1" ]; then
    line="$NODES node(s): DID NOT COMPLETE in 300s (rem=$rem) peak_conn=$peak_conn/200 peak_dead=$peak_dead"
  else
    ds=$drain_start; [ "$ds" = "-1" ] && ds=0
    window=$((dend - ds)); [ "$window" -lt 1 ] && window=1
    thr=$(( N / window ))
    line="$NODES node(s): schedule=$sched drain=${window}s throughput=${thr}/s peak_dead=$peak_dead peak_conn=$peak_conn/200 peak_del=${peak_del}/s"
  fi
  echo "$line" | tee -a "$RES"
  NODES=$((NODES+1))
done

pkill -9 -f "benchmark-1.0.0-SNAPSHOT.jar" 2>/dev/null
echo "========== STRESS STEP-UP RESULTS (spike=$N) =========="
cat "$RES"
