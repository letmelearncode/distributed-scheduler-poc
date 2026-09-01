package com.example.benchmark;

import com.example.dbscheduler.DbSchedulerJob;
import com.example.dbscheduler.DbSchedulerService;
import com.example.quartz.QuartzJob;
import com.example.quartz.QuartzMetrics;
import com.example.quartz.QuartzSchedulerService;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
@ComponentScan(basePackages = {"com.example.quartz", "com.example.dbscheduler", "com.example.benchmark"})
public class BenchmarkApplication implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkApplication.class);

    // Optional: only the active scheduler's service bean is present (profile-gated).
    @Autowired(required = false)
    private QuartzSchedulerService quartzSchedulerService;

    @Autowired(required = false)
    private DbSchedulerService dbSchedulerService;

    // Optional: only needed for kafka-produce mode.
    @Autowired(required = false)
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${benchmark.job-count:200000}")
    private int jobCount;

    @Value("${benchmark.scheduler:both}")
    private String schedulerType;

    @Value("${benchmark.mode:benchmark}")
    private String benchmarkMode;

    @Value("${benchmark.keep-alive:false}")
    private boolean keepAlive;

    // Seconds from now until the common fire timestamp. For large runs set this above the
    // expected load time so all jobs fire as a clean simultaneous spike (no misfire handling).
    @Value("${benchmark.fire-delay-seconds:30}")
    private int fireDelaySeconds;

    @Value("${kafka.topics.input:scheduled-jobs-input}")
    private String inputTopic;

    @Value("${kafka.topics.output:scheduled-jobs-output}")
    private String outputTopic;

    public static void main(String[] args) {
        // Parse scheduler type from args. Support BOTH "--benchmark.scheduler=quartz"
        // (what run-benchmark.sh passes) and the space-separated "--benchmark.scheduler quartz".
        String schedulerType = parseSchedulerType(args);

        // Scheduler isolation is done via Spring profiles: each scheduler's @Configuration
        // and @Service beans are annotated @Profile("quartz") / @Profile("db-scheduler").
        // "both" activates both profiles for a side-by-side comparison run.
        String activeProfiles = "both".equals(schedulerType) ? "quartz,db-scheduler" : schedulerType;
        System.setProperty("spring.profiles.active", activeProfiles);

        // When running Quartz-only, also exclude the DB-Scheduler starter auto-configuration
        // so it does not spin up a Scheduler / grab a DB pool. (The Quartz side is hand-wired
        // in QuartzConfig and gated purely by profile, so no auto-config exclusion is needed
        // for the db-scheduler-only case.)
        if ("quartz".equals(schedulerType)) {
            System.setProperty("spring.autoconfigure.exclude",
                "com.github.kagkarlsson.scheduler.boot.autoconfigure.DbSchedulerAutoConfiguration,com.github.kagkarlsson.scheduler.boot.autoconfigure.DbSchedulerMetricsAutoConfiguration");
        }

        SpringApplication.run(BenchmarkApplication.class, args);
    }

    private static String parseSchedulerType(String[] args) {
        String prefix = "--benchmark.scheduler";
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.startsWith(prefix + "=")) {
                return arg.substring((prefix + "=").length());
            }
            if (arg.equals(prefix) && i + 1 < args.length) {
                return args[i + 1];
            }
        }
        return "both";
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting benchmark with {} jobs, scheduler: {}, mode: {}", jobCount, schedulerType, benchmarkMode);

        // Start resource monitoring
        ResourceMonitor resourceMonitor = new ResourceMonitor();
        resourceMonitor.start();

        long scheduledTimestamp = calculateNextMidnight();
        List<com.example.quartz.JobPayload> quartzPayloads = generateQuartzPayloads(jobCount);
        List<com.example.dbscheduler.JobPayload> dbSchedulerPayloads = generateDbSchedulerPayloads(jobCount);

        try {
            switch (benchmarkMode) {
                case "benchmark" -> runBenchmark(quartzPayloads, dbSchedulerPayloads, scheduledTimestamp);
                case "kafka-produce" -> produceKafkaMessages(quartzPayloads, scheduledTimestamp);
                case "kafka-consume" -> consumeKafkaMessages();
                case "distribution-test" -> runDistributionTest(quartzPayloads, dbSchedulerPayloads, scheduledTimestamp);
                case "recovery-test" -> runRecoveryTest(quartzPayloads, dbSchedulerPayloads, scheduledTimestamp);
                default -> runBenchmark(quartzPayloads, dbSchedulerPayloads, scheduledTimestamp);
            }
        } finally {
            resourceMonitor.stop();
            resourceMonitor.printSummary();
        }

        printComparisonReport();

        if (keepAlive) {
            log.info("benchmark.keep-alive=true -> staying up so Prometheus can scrape "
                    + "http://localhost:{}/all-metrics . Press Ctrl+C to exit.", 8080);
            new CountDownLatch(1).await(); // block forever until the process is killed
        } else {
            System.exit(0);
        }
    }

    private long calculateNextMidnight() {
        // Common fire timestamp for the spike, `fireDelaySeconds` from now.
        return Instant.now().plusSeconds(fireDelaySeconds).toEpochMilli();
    }

    private List<com.example.quartz.JobPayload> generateQuartzPayloads(int count) {
        List<com.example.quartz.JobPayload> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(new com.example.quartz.JobPayload("job-" + i, "payload-" + i, 0));
        }
        return payloads;
    }

    private List<com.example.dbscheduler.JobPayload> generateDbSchedulerPayloads(int count) {
        List<com.example.dbscheduler.JobPayload> payloads = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            payloads.add(new com.example.dbscheduler.JobPayload("job-" + i, "payload-" + i, 0));
        }
        return payloads;
    }

    private void runBenchmark(List<com.example.quartz.JobPayload> quartzPayloads, 
                              List<com.example.dbscheduler.JobPayload> dbSchedulerPayloads,
                              long scheduledTimestamp) throws Exception {
        if ("quartz".equals(schedulerType) || "both".equals(schedulerType)) {
            runQuartzBenchmark(quartzPayloads, scheduledTimestamp);
        }

        if ("db-scheduler".equals(schedulerType) || "both".equals(schedulerType)) {
            runDbSchedulerBenchmark(dbSchedulerPayloads, scheduledTimestamp);
        }
    }

    private void runQuartzBenchmark(List<com.example.quartz.JobPayload> payloads, long scheduledTimestamp) throws Exception {
        log.info("=== Starting Quartz Benchmark ===");

        // Load all triggers with the scheduler in STANDBY (not started yet). This keeps the
        // bulk insert from fighting the worker/misfire threads over the global cluster lock.
        // The scheduler is then started to fire the whole batch as a single spike.
        long scheduleStart = System.currentTimeMillis();
        quartzSchedulerService.scheduleJobs(payloads, scheduledTimestamp);
        long scheduleTime = System.currentTimeMillis() - scheduleStart;
        log.info("Quartz scheduling (standby load) completed in {}ms", scheduleTime);

        quartzSchedulerService.start();
        log.info("Quartz scheduler started - firing spike");

        waitForCompletion("Quartz", payloads.size(), QuartzMetrics::getCompletedCount);

        QuartzMetrics.printSummary();
        quartzSchedulerService.shutdown();
    }

    private void runDbSchedulerBenchmark(List<com.example.dbscheduler.JobPayload> payloads, long scheduledTimestamp) throws Exception {
        log.info("=== Starting DB-Scheduler Benchmark ===");
        dbSchedulerService.start();

        long scheduleStart = System.currentTimeMillis();
        dbSchedulerService.scheduleJobs(payloads, scheduledTimestamp);
        long scheduleTime = System.currentTimeMillis() - scheduleStart;

        log.info("DB-Scheduler scheduling completed in {}ms", scheduleTime);

        waitForCompletion("DB-Scheduler", payloads.size(), DbSchedulerJob::getCompletedCount);

        DbSchedulerJob.printSummary();
        dbSchedulerService.shutdown();
    }

    private void produceKafkaMessages(List<com.example.quartz.JobPayload> payloads, long scheduledTimestamp) throws Exception {
        log.info("=== Producing {} messages to Kafka topic: {} ===", payloads.size(), inputTopic);
        
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        long produceStart = System.currentTimeMillis();
        
        for (int i = 0; i < payloads.size(); i += 1000) {
            int end = Math.min(i + 1000, payloads.size());
            List<CompletableFuture<Void>> futures = new ArrayList<>();
            
            for (int j = i; j < end; j++) {
                com.example.quartz.JobPayload p = payloads.get(j);
                com.example.quartz.KafkaJobPayload kafkaPayload = 
                    new com.example.quartz.KafkaJobPayload(p.getJobId(), p.getPayload(), scheduledTimestamp, inputTopic);
                String json = mapper.writeValueAsString(kafkaPayload);
                
                CompletableFuture<Void> future = kafkaTemplate.send(inputTopic, p.getJobId(), json)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to send message: {}", ex.getMessage());
                        }
                    })
                    .thenAccept(result -> {});
                futures.add(future);
            }
            
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            
            if (i % 50000 == 0) {
                log.info("Produced {}/{} messages", end, payloads.size());
            }
        }
        
        long produceTime = System.currentTimeMillis() - produceStart;
        log.info("Produced {} messages in {}ms ({} msg/sec)", payloads.size(), produceTime, 
            (payloads.size() * 1000L / produceTime));
    }

    private void consumeKafkaMessages() throws Exception {
        log.info("=== Starting Kafka Consumer Mode ===");
        
        if ("quartz".equals(schedulerType) || "both".equals(schedulerType)) {
            quartzSchedulerService.start();
        }
        if ("db-scheduler".equals(schedulerType) || "both".equals(schedulerType)) {
            dbSchedulerService.start();
        }

        // Keep running to consume messages
        Thread.sleep(Long.MAX_VALUE);
    }

    private void runDistributionTest(List<com.example.quartz.JobPayload> quartzPayloads,
                                     List<com.example.dbscheduler.JobPayload> dbSchedulerPayloads,
                                     long scheduledTimestamp) throws Exception {
        log.info("=== Starting Multi-Instance Distribution Test ===");
        
        int instanceCount = 3;
        log.info("Simulating {} scheduler instances", instanceCount);
        
        // In a real test, you'd start multiple Spring Boot instances
        // Here we simulate by running multiple scheduling rounds
        for (int i = 0; i < instanceCount; i++) {
            log.info("=== Instance {} ===", i + 1);
            if ("quartz".equals(schedulerType) || "both".equals(schedulerType)) {
                quartzSchedulerService.start();
                runQuartzBenchmark(quartzPayloads, scheduledTimestamp);
            }
            if ("db-scheduler".equals(schedulerType) || "both".equals(schedulerType)) {
                dbSchedulerService.start();
                runDbSchedulerBenchmark(dbSchedulerPayloads, scheduledTimestamp);
            }
            Thread.sleep(5000); // Cool down between instances
        }
    }

    private void runRecoveryTest(List<com.example.quartz.JobPayload> quartzPayloads,
                                 List<com.example.dbscheduler.JobPayload> dbSchedulerPayloads,
                                 long scheduledTimestamp) throws Exception {
        log.info("=== Starting Failure Recovery Test ===");
        
        // Test 1: Scheduler restart during execution
        log.info("Test 1: Scheduler restart during execution");
        if ("quartz".equals(schedulerType) || "both".equals(schedulerType)) {
            runRecoveryTestQuartz(quartzPayloads, scheduledTimestamp);
        }
        if ("db-scheduler".equals(schedulerType) || "both".equals(schedulerType)) {
            runRecoveryTestDbScheduler(dbSchedulerPayloads, scheduledTimestamp);
        }
        
        // Test 2: Job replay after failure
        log.info("Test 2: Job replay after failure");
        // Simulate by checking DLQ and replaying
    }

    private void runRecoveryTestQuartz(List<com.example.quartz.JobPayload> payloads, long scheduledTimestamp) throws Exception {
        quartzSchedulerService.start();
        quartzSchedulerService.scheduleJobs(payloads, scheduledTimestamp);
        
        // Wait for some jobs to start
        Thread.sleep(10000);
        int activeBefore = quartzSchedulerService.getCurrentlyExecutingJobs();
        log.info("Active jobs before restart: {}", activeBefore);
        
        // Simulate restart
        quartzSchedulerService.shutdown();
        Thread.sleep(2000);
        quartzSchedulerService.start();
        
        // Wait for recovery
        waitForCompletion("Quartz Recovery", payloads.size(), QuartzMetrics::getCompletedCount);
        log.info("Quartz recovery completed");
    }

    private void runRecoveryTestDbScheduler(List<com.example.dbscheduler.JobPayload> payloads, long scheduledTimestamp) throws Exception {
        dbSchedulerService.start();
        dbSchedulerService.scheduleJobs(payloads, scheduledTimestamp);
        
        Thread.sleep(10000);
        int activeBefore = getActiveDbSchedulerJobs();
        log.info("Active jobs before restart: {}", activeBefore);
        
        dbSchedulerService.shutdown();
        Thread.sleep(2000);
        dbSchedulerService.start();
        
        waitForCompletion("DB-Scheduler Recovery", payloads.size(), DbSchedulerJob::getCompletedCount);
        log.info("DB-Scheduler recovery completed");
    }

    private int getActiveDbSchedulerJobs() {
        Gauge gauge = DbSchedulerJob.getRegistry().get("dbscheduler.jobs.active").gauge();
        return gauge != null ? ((Number) gauge.value()).intValue() : 0;
    }

    // How long to keep waiting after the last observed progress before giving up.
    private static final long STALL_TIMEOUT_MS = 120_000;
    // Absolute ceiling for a single wait, regardless of progress.
    private static final long MAX_WAIT_MS = 30 * 60 * 1000L;

    /**
     * Waits until {@code completedSupplier} (cumulative executed+failed jobs) reaches
     * {@code expectedCount}. Terminates early if no progress is seen for STALL_TIMEOUT_MS,
     * so a stuck run fails fast instead of blocking for the full 30 minutes.
     */
    private void waitForCompletion(String name, long expectedCount,
                                   java.util.function.Supplier<Long> completedSupplier) throws InterruptedException {
        long start = System.currentTimeMillis();
        long lastProgressAt = start;
        long lastCompleted = -1;

        while (true) {
            long completed = completedSupplier.get();
            long now = System.currentTimeMillis();

            if (completed != lastCompleted) {
                long elapsed = now - start;
                double rate = elapsed > 0 ? (completed * 1000.0 / elapsed) : 0.0;
                log.info("{} - Completed: {}/{}, Elapsed: {}ms, Rate: {}/sec",
                        name, completed, expectedCount, elapsed, String.format("%.0f", rate));
                lastCompleted = completed;
                lastProgressAt = now;
            }

            if (completed >= expectedCount) {
                log.info("{} - All {} jobs completed in {}ms", name, expectedCount, now - start);
                return;
            }
            if (now - lastProgressAt > STALL_TIMEOUT_MS) {
                log.warn("{} - Stalled: no progress for {}ms. Completed {}/{}. Giving up.",
                        name, STALL_TIMEOUT_MS, completed, expectedCount);
                return;
            }
            if (now - start > MAX_WAIT_MS) {
                log.warn("{} - Timed out after {}ms. Completed {}/{}.",
                        name, MAX_WAIT_MS, completed, expectedCount);
                return;
            }

            Thread.sleep(1000);
        }
    }

    private void printComparisonReport() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("BENCHMARK COMPARISON REPORT");
        System.out.println("=".repeat(60));
        System.out.println("Job Count: " + jobCount);
        System.out.println("Scheduler(s) tested: " + schedulerType);
        System.out.println("Mode: " + benchmarkMode);
        System.out.println("=".repeat(60));
    }

    // Resource Monitoring
    public static class ResourceMonitor {
        private static final Logger log = LoggerFactory.getLogger(ResourceMonitor.class);
        private final MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        private final OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        private final AtomicLong peakHeapUsed = new AtomicLong(0);
        private final AtomicLong peakNonHeapUsed = new AtomicLong(0);
        private final AtomicLong peakCpuLoad = new AtomicLong(0);
        private final AtomicLong sampleCount = new AtomicLong(0);
        private volatile boolean running = false;
        private Thread monitorThread;

        public void start() {
            running = true;
            monitorThread = new Thread(this::monitorLoop, "ResourceMonitor");
            monitorThread.setDaemon(true);
            monitorThread.start();
            log.info("Resource monitoring started");
        }

        public void stop() {
            running = false;
            if (monitorThread != null) {
                monitorThread.interrupt();
            }
            log.info("Resource monitoring stopped");
        }

        private void monitorLoop() {
            while (running) {
                try {
                    long heapUsed = memoryBean.getHeapMemoryUsage().getUsed();
                    long nonHeapUsed = memoryBean.getNonHeapMemoryUsage().getUsed();
                    
                    peakHeapUsed.updateAndGet(prev -> Math.max(prev, heapUsed));
                    peakNonHeapUsed.updateAndGet(prev -> Math.max(prev, nonHeapUsed));
                    sampleCount.incrementAndGet();

                    // CPU load (may not be available on all platforms)
                    try {
                        double cpuLoad = getCpuLoad();
                        peakCpuLoad.updateAndGet(prev -> Math.max(prev, (long)(cpuLoad * 100)));
                    } catch (Exception ignored) {}

                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.warn("Resource monitoring error: {}", e.getMessage());
                }
            }
        }

        private double getCpuLoad() {
            try {
                java.lang.reflect.Method method = osBean.getClass().getMethod("getProcessCpuLoad");
                Object result = method.invoke(osBean);
                if (result instanceof Double) {
                    return (Double) result;
                }
            } catch (Exception ignored) {}
            return 0.0;
        }

        public void printSummary() {
            System.out.println("\n=== RESOURCE MONITORING SUMMARY ===");
            System.out.println("Samples collected: " + sampleCount.get());
            System.out.println("Peak Heap Used: " + (peakHeapUsed.get() / 1024 / 1024) + " MB");
            System.out.println("Peak Non-Heap Used: " + (peakNonHeapUsed.get() / 1024 / 1024) + " MB");
            System.out.println("Peak CPU Load: " + (peakCpuLoad.get() / 100.0) + "%");
            System.out.println("Current Heap: " + (memoryBean.getHeapMemoryUsage().getUsed() / 1024 / 1024) + " MB");
            System.out.println("Max Heap: " + (memoryBean.getHeapMemoryUsage().getMax() / 1024 / 1024) + " MB");
            System.out.println("=".repeat(40));
        }
    }
}