package com.example.ledger.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/** Bounded retries, then route the record to <topic>.DLT. Spring Boot wires this into the listeners. */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> template) {
        // Republisher preserves the original headers and adds the failure reason.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(template);
        // 2 retries (3 deliveries total) at 300ms, then hand off to the DLT and move on.
        return new DefaultErrorHandler(recoverer, new FixedBackOff(300L, 2L));
    }
}
