package com.example.quartz;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@PersistJobDataAfterExecution
@DisallowConcurrentExecution
@Component
@Profile("quartz")
public class QuartzJob implements Job {

    private static final Logger log = LoggerFactory.getLogger(QuartzJob.class);

    @Autowired
    private KafkaJobResultProducer resultProducer;

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap dataMap = context.getMergedJobDataMap();
        String jobId = dataMap.getString("jobId");
        String payload = dataMap.getString("payload");
        long scheduledTime = dataMap.getLong("scheduledTime");
        long startTime = System.currentTimeMillis();

        QuartzMetrics.incrementActive();

        try {
            long latency = startTime - scheduledTime;
            QuartzMetrics.recordScheduleLatency(latency);

            processJob(jobId, payload);

            long executionTime = System.currentTimeMillis() - startTime;
            QuartzMetrics.recordExecutionTime(executionTime);
            QuartzMetrics.incrementExecuted();

            resultProducer.sendResult(jobId, payload, scheduledTime, executionTime, true, null);

        } catch (Exception e) {
            QuartzMetrics.incrementFailed();
            long executionTime = System.currentTimeMillis() - startTime;
            resultProducer.sendResult(jobId, payload, scheduledTime, executionTime, false, e.getMessage());
            resultProducer.sendToDlq(jobId, payload, scheduledTime, e.getMessage());
            log.error("Job {} failed: {}", jobId, e.getMessage(), e);
            throw new JobExecutionException(e);
        } finally {
            QuartzMetrics.decrementActive();
        }
    }

    private void processJob(String jobId, String payload) {
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}