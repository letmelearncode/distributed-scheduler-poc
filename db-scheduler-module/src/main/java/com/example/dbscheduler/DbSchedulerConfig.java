package com.example.dbscheduler;

import com.github.kagkarlsson.scheduler.task.helper.OneTimeTask;
import com.github.kagkarlsson.scheduler.task.helper.Tasks;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("db-scheduler")
public class DbSchedulerConfig {

    /**
     * The one-time task that every scheduled benchmark job runs. The db-scheduler Spring Boot
     * starter auto-registers all {@code Task<?>} beans with the scheduler, so declaring this
     * bean is what actually makes "benchmark-job" executable. (The previous version registered
     * a raw {@code ExecutionHandler} bean, which the starter does not pick up — so nothing ever
     * ran and {@code scheduled_tasks} stayed empty.)
     */
    @Bean
    public OneTimeTask<DbJobData> benchmarkTask(DbSchedulerJob jobHandler) {
        return Tasks.oneTime("benchmark-job", DbJobData.class)
                .execute((instance, ctx) -> {
                    DbJobData data = instance.getData();
                    String payload = data != null ? data.getPayload() : "";
                    jobHandler.execute(
                            instance.getTaskName(),
                            instance.getId(),
                            ctx.getExecution().getExecutionTime().toEpochMilli(),
                            payload);
                });
    }
}
