package com.example.payment.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Topics this service touches. It owns payment.commands and payment.events; it consumes ledger.events. */
@Configuration
public class KafkaTopicsConfig {

    public static final String FRAUD_REQUESTS = "fraud.requests";
    public static final String FRAUD_RESULTS = "fraud.results";
    public static final String FRAUD_RESULTS_DLT = FRAUD_RESULTS + ".DLT";
    public static final String PAYMENT_COMMANDS = "payment.commands";
    public static final String PAYMENT_EVENTS = "payment.events";
    public static final String LEDGER_EVENTS = "ledger.events";
    public static final String LEDGER_EVENTS_DLT = LEDGER_EVENTS + ".DLT";

    // We produce payment initiations here for the fraud service to screen.
    @Bean
    NewTopic fraudRequests() {
        return TopicBuilder.name(FRAUD_REQUESTS).partitions(3).replicas(1).build();
    }

    // Dead-letter topic for poison fraud verdicts we consume.
    @Bean
    NewTopic fraudResultsDlt() {
        return TopicBuilder.name(FRAUD_RESULTS_DLT).partitions(3).replicas(1).build();
    }

    // Multiple partitions so per-account ordering (key = fromAccountId) is real, not theoretical.
    @Bean
    NewTopic paymentCommands() {
        return TopicBuilder.name(PAYMENT_COMMANDS).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic paymentEvents() {
        return TopicBuilder.name(PAYMENT_EVENTS).partitions(3).replicas(1).build();
    }

    // Dead-letter topic for poison ledger events. Same partition count as the source.
    @Bean
    NewTopic ledgerEventsDlt() {
        return TopicBuilder.name(LEDGER_EVENTS_DLT).partitions(3).replicas(1).build();
    }
}
