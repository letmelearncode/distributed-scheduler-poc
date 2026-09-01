-- DB-Scheduler tables for PostgreSQL.
--
-- IMPORTANT: db-scheduler (com.github.kagkarlsson) uses a FIXED table/column layout in its
-- JdbcTaskRepository. The single table it reads/writes is `scheduled_tasks`, and the column
-- names below (task_name, task_instance, task_data, ...) are exactly what its SQL expects.
-- The previous version of this file used `instance_id` and `data JSONB`, which do not match
-- db-scheduler's queries, so every insert/fetch failed and `scheduled_tasks` stayed empty.
CREATE TABLE scheduled_tasks (
  task_name            VARCHAR(100)             NOT NULL,
  task_instance        VARCHAR(200)             NOT NULL,
  task_data            BYTEA,
  execution_time       TIMESTAMP WITH TIME ZONE NOT NULL,
  picked               BOOLEAN                  NOT NULL,
  picked_by            VARCHAR(50),
  last_success         TIMESTAMP WITH TIME ZONE,
  last_failure         TIMESTAMP WITH TIME ZONE,
  consecutive_failures INT,
  last_heartbeat       TIMESTAMP WITH TIME ZONE,
  version              BIGINT                   NOT NULL,
  priority             SMALLINT,
  PRIMARY KEY (task_name, task_instance)
);

CREATE INDEX idx_scheduled_tasks_execution_time ON scheduled_tasks (execution_time);
CREATE INDEX idx_scheduled_tasks_last_heartbeat ON scheduled_tasks (last_heartbeat);
CREATE INDEX idx_scheduled_tasks_priority_execution ON scheduled_tasks (priority DESC, execution_time ASC);

-- Aggressive "queue table" autovacuum tuning. scheduled_tasks is extremely high
-- churn (insert-on-schedule, update-on-pick, delete-on-complete): at ~1.4M jobs/day
-- the default autovacuum (fires at 20% dead tuples, throttled) lets dead tuples and
-- bloat accumulate, which slows the FOR-UPDATE-SKIP-LOCKED pick and the delete. These
-- settings vacuum whenever dead tuples exceed a small fixed threshold, at full speed.
--   scale_factor = 0 + threshold = N  -> trigger on absolute dead-tuple count, not %
--   cost_delay   = 0                  -> do not throttle the autovacuum worker
-- fillfactor 80 leaves room for HOT updates (picked/version/heartbeat) to avoid index churn.
ALTER TABLE scheduled_tasks SET (
  autovacuum_vacuum_scale_factor  = 0.0,
  autovacuum_vacuum_threshold     = 5000,
  autovacuum_vacuum_cost_delay    = 0,
  autovacuum_vacuum_cost_limit    = 10000,
  autovacuum_analyze_scale_factor = 0.0,
  autovacuum_analyze_threshold    = 5000,
  fillfactor                      = 80
);

-- ---------------------------------------------------------------------------------------------
-- The tables below are illustrative extras for the POC's "200k jobs / retention" discussion.
-- They are NOT used by db-scheduler at runtime (db-scheduler only touches `scheduled_tasks`
-- above). They are kept for reference only.
-- ---------------------------------------------------------------------------------------------

-- Recurring tasks table (for cron-like tasks) - reference only, not used by db-scheduler.
CREATE TABLE recurring_tasks (
  task_name VARCHAR(100) NOT NULL PRIMARY KEY,
  task_type VARCHAR(50) NOT NULL,
  cron_expression VARCHAR(100) NULL,
  fixed_delay BIGINT NULL,
  fixed_rate BIGINT NULL,
  initial_delay BIGINT NULL,
  data JSONB NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE
);

-- Executions log - reference only, not used by db-scheduler.
CREATE TABLE executions (
  id BIGSERIAL PRIMARY KEY,
  task_name VARCHAR(100) NOT NULL,
  instance_id VARCHAR(200) NOT NULL,
  execution_time TIMESTAMP WITH TIME ZONE NOT NULL,
  started TIMESTAMP WITH TIME ZONE NOT NULL,
  completed TIMESTAMP WITH TIME ZONE NULL,
  success BOOLEAN NULL,
  error TEXT NULL,
  data JSONB NULL
);

CREATE INDEX idx_executions_task_time ON executions (task_name, execution_time);
CREATE INDEX idx_executions_started ON executions (started);

-- Partition executions by month for retention - reference only, not used by db-scheduler.
CREATE TABLE executions_partitioned (
  id BIGSERIAL NOT NULL,
  task_name VARCHAR(100) NOT NULL,
  instance_id VARCHAR(200) NOT NULL,
  execution_time TIMESTAMP WITH TIME ZONE NOT NULL,
  started TIMESTAMP WITH TIME ZONE NOT NULL,
  completed TIMESTAMP WITH TIME ZONE NULL,
  success BOOLEAN NULL,
  error TEXT NULL,
  data JSONB NULL,
  PRIMARY KEY (id, execution_time)
) PARTITION BY RANGE (execution_time);

-- Monthly partitions for 1 year
DO $$
DECLARE
  start_date DATE := DATE_TRUNC('month', CURRENT_DATE);
  end_date DATE := start_date + INTERVAL '13 months';
  partition_name TEXT;
  partition_start DATE;
  partition_end DATE;
BEGIN
  WHILE start_date < end_date LOOP
    partition_name := 'executions_' || TO_CHAR(start_date, 'YYYY_MM');
    partition_start := start_date;
    partition_end := start_date + INTERVAL '1 month';
    EXECUTE format('CREATE TABLE %I PARTITION OF executions_partitioned FOR VALUES FROM (%L) TO (%L)', partition_name, partition_start, partition_end);
    start_date := partition_end;
  END LOOP;
END $$;
