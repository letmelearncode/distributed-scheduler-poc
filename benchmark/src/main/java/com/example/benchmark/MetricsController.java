package com.example.benchmark;

import com.example.dbscheduler.DbSchedulerJob;
import com.example.quartz.QuartzMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes a single Prometheus scrape endpoint that merges three registries:
 *  - the Spring/actuator PrometheusMeterRegistry (JVM, process, Hikari pool, HTTP), and
 *  - the two stand-alone static registries the schedulers record into
 *    ({@link QuartzMetrics} and {@link DbSchedulerJob}), which are NOT visible to actuator.
 *
 * Prometheus scrapes {@code /all-metrics} (see prometheus.yml). Only the active scheduler's
 * family carries real values in a given run; the other reads as zero, which is fine.
 */
@RestController
public class MetricsController {

    @Autowired(required = false)
    private PrometheusMeterRegistry springRegistry;

    @GetMapping(value = "/all-metrics", produces = "text/plain; version=0.0.4; charset=utf-8")
    public String allMetrics() {
        StringBuilder sb = new StringBuilder();
        if (springRegistry != null) {
            sb.append(springRegistry.scrape());
        }
        sb.append(QuartzMetrics.getRegistry().scrape());
        sb.append(DbSchedulerJob.getRegistry().scrape());
        return sb.toString();
    }
}
