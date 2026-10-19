package com.example.fraud.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Consumes fraud.requests; owns fraud.results (and the request DLT). */
@Configuration
public class KafkaTopicsConfig {

    public static final String FRAUD_REQUESTS = "fraud.requests";
    public static final String FRAUD_REQUESTS_DLT = FRAUD_REQUESTS + ".DLT";
    public static final String FRAUD_RESULTS = "fraud.results";

    @Bean
    NewTopic fraudResults() {
        return TopicBuilder.name(FRAUD_RESULTS).partitions(3).replicas(1).build();
    }

    @Bean
    NewTopic fraudRequestsDlt() {
        return TopicBuilder.name(FRAUD_REQUESTS_DLT).partitions(3).replicas(1).build();
    }
}
