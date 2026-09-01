package com.example.quartz;

import io.micrometer.core.instrument.*;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class QuartzMetrics implements MeterBinder {

    private static final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

    private static final LongAdder executedCount = new LongAdder();
    private static final LongAdder failedCount = new LongAdder();

    private static final Timer scheduleLatencyTimer = Timer.builder("quartz.schedule.latency")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram() // emit _bucket series for Grafana histogram_quantile()
            .register(registry);

    private static final Timer executionTimer = Timer.builder("quartz.execution.time")
            .publishPercentiles(0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(registry);

    private static final Counter scheduledCounter = Counter.builder("quartz.jobs.scheduled.total").register(registry);
    private static final Counter executedCounter = Counter.builder("quartz.jobs.executed.total").register(registry);
    private static final Counter failedCounter = Counter.builder("quartz.jobs.failed.total").register(registry);
    private static final Gauge activeJobsGauge = Gauge.builder("quartz.jobs.active", QuartzMetrics::getActiveJobCount).register(registry);

    private static volatile int activeJobCount = 0;

    public static void recordScheduleLatency(long latencyMs) {
        scheduleLatencyTimer.record(latencyMs, TimeUnit.MILLISECONDS);
    }

    public static void recordExecutionTime(long timeMs) {
        executionTimer.record(timeMs, TimeUnit.MILLISECONDS);
    }

    public static void incrementScheduled() {
        scheduledCounter.increment();
    }

    public static void incrementScheduled(long n) {
        scheduledCounter.increment(n);
    }

    public static void incrementExecuted() {
        executedCount.increment();
        executedCounter.increment();
    }

    public static void incrementFailed() {
        failedCount.increment();
        failedCounter.increment();
    }

    public static void incrementActive() {
        activeJobCount++;
    }

    public static void decrementActive() {
        activeJobCount--;
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

    @Override
    public void bindTo(MeterRegistry meterRegistry) {
    }

    public static void printSummary() {
        System.out.println("\n=== QUARTZ METRICS SUMMARY ===");
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