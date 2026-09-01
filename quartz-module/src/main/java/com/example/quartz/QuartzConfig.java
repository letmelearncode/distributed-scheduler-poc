package com.example.quartz;

import com.zaxxer.hikari.HikariDataSource;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.jdbcjobstore.JobStoreTX;
import org.quartz.impl.jdbcjobstore.PostgreSQLDelegate;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@Profile("quartz")
public class QuartzConfig {

    @Value("${spring.datasource.url}")
    private String datasourceUrl;

    @Value("${spring.datasource.username}")
    private String datasourceUsername;

    @Value("${spring.datasource.password}")
    private String datasourcePassword;

    @Value("${spring.datasource.hikari.maximum-pool-size:50}")
    private int maxPoolSize;

    @Value("${quartz.thread-count:50}")
    private int threadCount;

    @Autowired
    private ApplicationContext applicationContext;

    @Bean
    public DataSource quartzDataSource() {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl(datasourceUrl);
        ds.setUsername(datasourceUsername);
        ds.setPassword(datasourcePassword);
        ds.setMaximumPoolSize(maxPoolSize);
        ds.setPoolName("quartz-pool");
        return ds;
    }

    @Bean
    public PlatformTransactionManager transactionManager(DataSource dataSource) {
        return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    public JobFactory jobFactory() {
        return new SpringJobFactory(applicationContext.getAutowireCapableBeanFactory());
    }

    @Bean("quartzScheduler")
    public Scheduler quartzScheduler(DataSource dataSource, JobFactory jobFactory) throws SchedulerException {
        Properties props = new Properties();
        props.setProperty("org.quartz.scheduler.instanceName", "QuartzPOCScheduler");
        props.setProperty("org.quartz.scheduler.instanceId", "AUTO");
        props.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
        props.setProperty("org.quartz.threadPool.threadCount", String.valueOf(threadCount));
        props.setProperty("org.quartz.threadPool.threadPriority", "5");
        props.setProperty("org.quartz.jobStore.class", JobStoreTX.class.getName());
        props.setProperty("org.quartz.jobStore.driverDelegateClass", PostgreSQLDelegate.class.getName());
        props.setProperty("org.quartz.jobStore.dataSource", "quartzDS");
        props.setProperty("org.quartz.jobStore.tablePrefix", "QRTZ_");
        props.setProperty("org.quartz.jobStore.isClustered", "true");
        props.setProperty("org.quartz.jobStore.clusterCheckinInterval", "5000");
        // Spike tuning. A LARGE misfire threshold is the key: with the default 60s, a 200k
        // same-instant backlog that takes longer than 60s to start draining is treated as
        // "misfired" and routed through the slow MisfireHandler (20 at a time), collapsing to
        // ~7 jobs/sec. With a 1h threshold the backlog stays on the normal fast acquisition
        // path (batches of 1000 via the worker pool) instead.
        props.setProperty("org.quartz.jobStore.misfireThreshold", "3600000");
        props.setProperty("org.quartz.scheduler.batchTriggerAcquisitionMaxCount", "1000");
        props.setProperty("org.quartz.jobStore.maxMisfiresToHandleAtATime", "2000");
        props.setProperty("org.quartz.jobStore.selectWithLockSQL", "SELECT * FROM {0}LOCKS WHERE LOCK_NAME = ? FOR UPDATE");
        props.setProperty("org.quartz.dataSource.quartzDS.provider", "hikaricp");
        props.setProperty("org.quartz.dataSource.quartzDS.driver", "org.postgresql.Driver");
        props.setProperty("org.quartz.dataSource.quartzDS.URL", datasourceUrl);
        props.setProperty("org.quartz.dataSource.quartzDS.user", datasourceUsername);
        props.setProperty("org.quartz.dataSource.quartzDS.password", datasourcePassword);
        props.setProperty("org.quartz.dataSource.quartzDS.maxConnections", String.valueOf(maxPoolSize));
        props.setProperty("org.quartz.dataSource.quartzDS.validationQuery", "SELECT 1");

        StdSchedulerFactory factory = new StdSchedulerFactory(props);
        Scheduler scheduler = factory.getScheduler();
        scheduler.setJobFactory(jobFactory);
        return scheduler;
    }

    public static class SpringJobFactory implements JobFactory {
        private final AutowireCapableBeanFactory beanFactory;

        public SpringJobFactory(AutowireCapableBeanFactory beanFactory) {
            this.beanFactory = beanFactory;
        }

        @Override
        public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
            Job job = (Job) beanFactory.createBean(bundle.getJobDetail().getJobClass());
            return job;
        }
    }
}