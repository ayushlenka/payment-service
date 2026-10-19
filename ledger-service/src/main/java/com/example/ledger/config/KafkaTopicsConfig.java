package com.example.ledger.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Topics this service touches. It owns ledger.events; payment.commands is owned by the payment service. */
@Configuration
public class KafkaTopicsConfig {

    public static final String PAYMENT_COMMANDS = "payment.commands";
    public static final String PAYMENT_COMMANDS_DLT = PAYMENT_COMMANDS + ".DLT";
    public static final String LEDGER_EVENTS = "ledger.events";

    @Bean
    NewTopic ledgerEvents() {
        return TopicBuilder.name(LEDGER_EVENTS).partitions(3).replicas(1).build();
    }

    // Dead-letter topic for poison payment commands. Same partition count as the source.
    @Bean
    NewTopic paymentCommandsDlt() {
        return TopicBuilder.name(PAYMENT_COMMANDS_DLT).partitions(3).replicas(1).build();
    }
}
