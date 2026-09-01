package com.example.quartz;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@Profile("quartz")
public class KafkaSchedulerConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaSchedulerConsumer.class);

    @Autowired
    private QuartzSchedulerService quartzSchedulerService;

    @Autowired
    private ObjectMapper objectMapper;

    @Value("${kafka.topics.input:scheduled-jobs-input}")
    private String inputTopic;

    @Value("${kafka.batch.size:1000}")
    private int batchSize;

    private final List<KafkaJobPayload> buffer = new ArrayList<>();

    @KafkaListener(topics = "${kafka.topics.input:scheduled-jobs-input}", 
                   groupId = "${spring.kafka.consumer.group-id:scheduler-poc}",
                   containerFactory = "kafkaListenerContainerFactory")
    public void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            KafkaJobPayload payload = objectMapper.readValue(record.value(), KafkaJobPayload.class);
            buffer.add(payload);

            if (buffer.size() >= batchSize) {
                flushBuffer();
            }
            ack.acknowledge();
        } catch (Exception e) {
            log.error("Failed to process Kafka message: {}", e.getMessage(), e);
        }
    }

    public void flushBuffer() {
        if (!buffer.isEmpty()) {
            try {
                long scheduledTime = buffer.get(0).getScheduledTime();
                List<JobPayload> jobPayloads = buffer.stream()
                    .map(p -> new JobPayload(p.getJobId(), p.getPayload(), scheduledTime))
                    .toList();
                quartzSchedulerService.scheduleJobs(jobPayloads, scheduledTime);
                log.info("Scheduled {} jobs from Kafka buffer", buffer.size());
                buffer.clear();
            } catch (Exception e) {
                log.error("Failed to schedule jobs from buffer: {}", e.getMessage(), e);
            }
        }
    }

    public int getBufferSize() {
        return buffer.size();
    }
}