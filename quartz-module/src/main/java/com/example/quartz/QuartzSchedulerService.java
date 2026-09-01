package com.example.quartz;

import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Service
@Profile("quartz")
public class QuartzSchedulerService {

    private static final Logger log = LoggerFactory.getLogger(QuartzSchedulerService.class);

    @Autowired
    @Qualifier("quartzScheduler")
    private Scheduler scheduler;

    public void scheduleJobs(List<JobPayload> payloads, long scheduledTimestamp) throws SchedulerException {
        clearAllJobs(); // Clean up from previous runs
        
        long startTime = System.currentTimeMillis();
        int batchSize = 1000;

        for (int i = 0; i < payloads.size(); i += batchSize) {
            List<JobPayload> batch = payloads.subList(i, Math.min(i + batchSize, payloads.size()));
            scheduleBatch(batch, scheduledTimestamp);
        }

        log.info("Scheduled {} jobs in {}ms", payloads.size(), System.currentTimeMillis() - startTime);
    }

    private void clearAllJobs() throws SchedulerException {
        try {
            List<String> jobGroups = scheduler.getJobGroupNames();
            for (String group : jobGroups) {
                Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.jobGroupEquals(group));
                if (!jobKeys.isEmpty()) {
                    scheduler.deleteJobs(new ArrayList<>(jobKeys));
                    log.info("Cleared {} jobs from group: {}", jobKeys.size(), group);
                }
            }
        } catch (SchedulerException e) {
            log.warn("Could not clear existing jobs: {}", e.getMessage());
        }
    }

    private void scheduleBatch(List<JobPayload> batch, long scheduledTimestamp) throws SchedulerException {
        for (JobPayload payload : batch) {
            JobDetail job = JobBuilder.newJob(QuartzJob.class)
                    .withIdentity("job-" + payload.getJobId(), "benchmark-group")
                    .usingJobData("jobId", payload.getJobId())
                    .usingJobData("payload", payload.getPayload())
                    .usingJobData("scheduledTime", scheduledTimestamp)
                    .build();

            Trigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity("trigger-" + payload.getJobId(), "benchmark-group")
                    .startAt(new java.util.Date(scheduledTimestamp))
                    .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                            .withMisfireHandlingInstructionFireNow())
                    .build();

            scheduler.scheduleJob(job, trigger);
        }
    }

    public void start() throws SchedulerException {
        if (!scheduler.isStarted()) {
            scheduler.start();
            log.info("Quartz scheduler started");
        }
    }

    public void shutdown() throws SchedulerException {
        scheduler.shutdown(true);
        log.info("Quartz scheduler shutdown");
    }

    public int getCurrentlyExecutingJobs() {
        try {
            return scheduler.getCurrentlyExecutingJobs().size();
        } catch (SchedulerException e) {
            return -1;
        }
    }
}