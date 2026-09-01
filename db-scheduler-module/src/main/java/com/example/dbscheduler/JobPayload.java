package com.example.dbscheduler;

public class JobPayload {
    private String jobId;
    private String payload;
    private long scheduledTimestamp;

    public JobPayload() {}

    public JobPayload(String jobId, String payload, long scheduledTimestamp) {
        this.jobId = jobId;
        this.payload = payload;
        this.scheduledTimestamp = scheduledTimestamp;
    }

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public long getScheduledTimestamp() { return scheduledTimestamp; }
    public void setScheduledTimestamp(long scheduledTimestamp) { this.scheduledTimestamp = scheduledTimestamp; }
}