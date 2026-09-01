package com.example.quartz;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

@Service
@Profile("quartz")
public class KafkaJobResultProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaJobResultProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${kafka.topics.output:scheduled-jobs-output}")
    private String outputTopic;

    @Value("${kafka.topics.dlq:scheduled-jobs-dlq}")
    private String dlqTopic;

    public void sendResult(String jobId, String payload, long scheduledTime, long executionTime, boolean success, String error) {
        JobResult result = new JobResult(jobId, payload, scheduledTime, executionTime, success, error, Instant.now().toEpochMilli());
        sendAsync(outputTopic, jobId, result);
    }

    public void sendToDlq(String jobId, String payload, long scheduledTime, String error) {
        DlqMessage dlq = new DlqMessage(jobId, payload, scheduledTime, error, Instant.now().toEpochMilli());
        sendAsync(dlqTopic, jobId, dlq);
    }

    private <T> void sendAsync(String topic, String key, T message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, json);
            
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send message to {}: {}", topic, ex.getMessage(), ex);
                }
            });
        } catch (Exception e) {
            log.error("Failed to serialize message for {}: {}", topic, e.getMessage(), e);
        }
    }

    public static class JobResult {
        private String jobId;
        private String payload;
        private long scheduledTime;
        private long executionTime;
        private boolean success;
        private String error;
        private long completedAt;

        public JobResult() {}
        public JobResult(String jobId, String payload, long scheduledTime, long executionTime, 
                        boolean success, String error, long completedAt) {
            this.jobId = jobId;
            this.payload = payload;
            this.scheduledTime = scheduledTime;
            this.executionTime = executionTime;
            this.success = success;
            this.error = error;
            this.completedAt = completedAt;
        }
        public String getJobId() { return jobId; }
        public void setJobId(String jobId) { this.jobId = jobId; }
        public String getPayload() { return payload; }
        public void setPayload(String payload) { this.payload = payload; }
        public long getScheduledTime() { return scheduledTime; }
        public void setScheduledTime(long scheduledTime) { this.scheduledTime = scheduledTime; }
        public long getExecutionTime() { return executionTime; }
        public void setExecutionTime(long executionTime) { this.executionTime = executionTime; }
        public boolean isSuccess() { return success; }
        public void setSuccess(boolean success) { this.success = success; }
        public String getError() { return error; }
        public void setError(String error) { this.error = error; }
        public long getCompletedAt() { return completedAt; }
        public void setCompletedAt(long completedAt) { this.completedAt = completedAt; }
    }

    public static class DlqMessage {
        private String jobId;
        private String payload;
        private long scheduledTime;
        private String error;
        private long failedAt;

        public DlqMessage() {}
        public DlqMessage(String jobId, String payload, long scheduledTime, String error, long failedAt) {
            this.jobId = jobId;
            this.payload = payload;
            this.scheduledTime = scheduledTime;
            this.error = error;
            this.failedAt = failedAt;
        }
        public String getJobId() { return jobId; }
        public void setJobId(String jobId) { this.jobId = jobId; }
        public String getPayload() { return payload; }
        public void setPayload(String payload) { this.payload = payload; }
        public long getScheduledTime() { return scheduledTime; }
        public void setScheduledTime(long scheduledTime) { this.scheduledTime = scheduledTime; }
        public String getError() { return error; }
        public void setError(String error) { this.error = error; }
        public long getFailedAt() { return failedAt; }
        public void setFailedAt(long failedAt) { this.failedAt = failedAt; }
    }
}