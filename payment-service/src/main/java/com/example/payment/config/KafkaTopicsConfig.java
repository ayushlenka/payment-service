package com.example.payment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Declares the topics this service owns; KafkaAdmin creates them at startup. */
@Configuration
public class KafkaTopicsConfig {

    public static final String PAYMENT_COMMANDS = "payment.commands";

    // Multiple partitions so per-account ordering (key = fromAccountId) is real, not theoretical.
    @Bean
    NewTopic paymentCommands() {
        return TopicBuilder.name(PAYMENT_COMMANDS).partitions(3).replicas(1).build();
    }
}
