package com.example.dbscheduler;

import com.github.kagkarlsson.scheduler.Scheduler;
import com.github.kagkarlsson.scheduler.task.SchedulableInstance;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Profile("db-scheduler")
public class DbSchedulerService {

    private static final Logger log = LoggerFactory.getLogger(DbSchedulerService.class);

    @Autowired
    private Scheduler scheduler;

    public void scheduleJobs(List<JobPayload> payloads, long scheduledTimestamp) {
        clearAllJobs(); // Clean up from previous runs
        
        long startTime = System.currentTimeMillis();
        Instant scheduledInstant = Instant.ofEpochMilli(scheduledTimestamp);

        List<SchedulableInstance<?>> schedulableInstances = payloads.stream()
                .map(payload -> new TaskInstance.Builder<>("benchmark-job", payload.getJobId())
                        .data(new DbJobData(payload.getPayload()))
                        .scheduledTo(scheduledInstant))
                .collect(Collectors.toList());

        scheduler.scheduleBatch(schedulableInstances);

        log.info("Scheduled {} jobs in {}ms", payloads.size(), System.currentTimeMillis() - startTime);
    }

    private void clearAllJobs() {
        try {
            scheduler.fetchScheduledExecutionsForTask("benchmark-job", DbJobData.class, execution -> {
                try {
                    scheduler.cancel(execution.getTaskInstance());
                } catch (Exception e) {
                    log.warn("Could not cancel execution {}: {}", execution.getTaskInstance(), e.getMessage());
                }
            });
            log.info("Cleared existing benchmark-job executions");
        } catch (Exception e) {
            log.warn("Could not clear existing jobs: {}", e.getMessage());
        }
    }

    public void start() {
        // The db-scheduler Spring Boot starter already auto-starts the scheduler on context
        // ready. Scheduler.start() has no internal double-start guard, so guard here to avoid
        // spinning up duplicate polling/housekeeping threads.
        if (scheduler.getSchedulerState().isStarted()) {
            log.info("DB-Scheduler already started (by Spring Boot starter) - skipping start()");
            return;
        }
        scheduler.start();
        log.info("DB-Scheduler started");
    }

    public void shutdown() {
        scheduler.stop();
        log.info("DB-Scheduler shutdown");
    }
}