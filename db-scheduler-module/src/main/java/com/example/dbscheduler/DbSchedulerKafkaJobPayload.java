package com.example.dbscheduler;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public class DbSchedulerKafkaJobPayload {

    @JsonProperty("jobId")
    private String jobId;

    @JsonProperty("payload")
    private String payload;

    @JsonProperty("scheduledTime")
    private long scheduledTime;

    @JsonProperty("topic")
    private String topic;

    @JsonProperty("timestamp")
    private long timestamp;

    public DbSchedulerKafkaJobPayload() {
        this.timestamp = Instant.now().toEpochMilli();
    }

    public DbSchedulerKafkaJobPayload(String jobId, String payload, long scheduledTime, String topic) {
        this.jobId = jobId;
        this.payload = payload;
        this.scheduledTime = scheduledTime;
        this.topic = topic;
        this.timestamp = Instant.now().toEpochMilli();
    }

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public long getScheduledTime() { return scheduledTime; }
    public void setScheduledTime(long scheduledTime) { this.scheduledTime = scheduledTime; }
    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}