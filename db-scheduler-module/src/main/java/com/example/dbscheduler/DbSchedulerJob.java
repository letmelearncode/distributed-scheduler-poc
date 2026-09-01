package com.example.dbscheduler;

import io.micrometer.core.instrument.*;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

@Component
@Profile("db-scheduler")
public class DbSchedulerJob {

    private static final Logger log = LoggerFactory.getLogger(DbSchedulerJob.class);

    private static final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private static final Timer scheduleLatencyTimer = Timer.builder("dbscheduler.schedule.latency")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

    private static final Timer executionTimer = Timer.builder("dbscheduler.execution.time")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);

    private static final Counter executedCounter = Counter.builder("dbscheduler.jobs.executed.total").register(registry);
    private static final Counter failedCounter = Counter.builder("dbscheduler.jobs.failed.total").register(registry);
    private static final Gauge activeJobsGauge = Gauge.builder("dbscheduler.jobs.active", DbSchedulerJob::getActiveJobCount).register(registry);

    private static volatile int activeJobCount = 0;
    private static final LongAdder executedCount = new LongAdder();
    private static final LongAdder failedCount = new LongAdder();

    @Autowired
    private DbSchedulerKafkaJobResultProducer resultProducer;

    public void execute(String taskName, String instanceId, long scheduledTime, String payload) {
        long startTime = System.currentTimeMillis();
        activeJobCount++;

        try {
            long latency = startTime - scheduledTime;
            scheduleLatencyTimer.record(latency, TimeUnit.MILLISECONDS);

            processJob(instanceId, payload);

            long executionTime = System.currentTimeMillis() - startTime;
            executionTimer.record(executionTime, TimeUnit.MILLISECONDS);
            executedCount.increment();
            executedCounter.increment();

            resultProducer.sendResult(instanceId, payload, scheduledTime, executionTime, true, null);

        } catch (Exception e) {
            failedCount.increment();
            failedCounter.increment();
            long executionTime = System.currentTimeMillis() - startTime;
            resultProducer.sendResult(instanceId, payload, scheduledTime, executionTime, false, e.getMessage());
            resultProducer.sendToDlq(instanceId, payload, scheduledTime, e.getMessage());
            log.error("Job {} failed: {}", instanceId, e.getMessage(), e);
            throw e;
        } finally {
            activeJobCount--;
        }
    }

    private void processJob(String instanceId, String payload) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int getActiveJobCount() {
        return activeJobCount;
    }

    public static long getExecutedCount() {
        return executedCount.sum();
    }

    public static long getFailedCount() {
        return failedCount.sum();
    }

    /** Terminal jobs (executed + failed) — used to detect benchmark completion. */
    public static long getCompletedCount() {
        return executedCount.sum() + failedCount.sum();
    }

    public static PrometheusMeterRegistry getRegistry() {
        return registry;
    }

    public static void printSummary() {
        System.out.println("\n=== DB-SCHEDULER METRICS SUMMARY ===");
        System.out.println("Executed: " + executedCount.sum());
        System.out.println("Failed: " + failedCount.sum());
        printTimer("Fire Latency (scheduled->start)", scheduleLatencyTimer);
        printTimer("Execution Time", executionTimer);
    }

    /** Prints mean/p50/p95/p99/max in milliseconds for a percentile-enabled Timer. */
    static void printTimer(String label, Timer timer) {
        io.micrometer.core.instrument.distribution.HistogramSnapshot snap = timer.takeSnapshot();
        double p50 = 0, p95 = 0, p99 = 0;
        for (io.micrometer.core.instrument.distribution.ValueAtPercentile v : snap.percentileValues()) {
            double ms = v.value(TimeUnit.MILLISECONDS);
            if (v.percentile() == 0.5) p50 = ms;
            else if (v.percentile() == 0.95) p95 = ms;
            else if (v.percentile() == 0.99) p99 = ms;
        }
        System.out.printf("%s -> mean=%.1fms p50=%.1fms p95=%.1fms p99=%.1fms max=%.1fms%n",
                label, timer.mean(TimeUnit.MILLISECONDS), p50, p95, p99, timer.max(TimeUnit.MILLISECONDS));
    }
}