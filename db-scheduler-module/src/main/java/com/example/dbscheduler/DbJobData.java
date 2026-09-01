package com.example.dbscheduler;

import java.io.Serializable;

/**
 * Serializable payload stored in the db-scheduler {@code task_data} column for each
 * scheduled benchmark job. The task and the scheduling call must agree on this type so
 * db-scheduler can deserialize it back on execution.
 */
public class DbJobData implements Serializable {

    private static final long serialVersionUID = 1L;

    private String payload;

    public DbJobData() {
    }

    public DbJobData(String payload) {
        this.payload = payload;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }
}
