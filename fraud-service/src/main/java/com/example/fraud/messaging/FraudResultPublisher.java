package com.example.fraud.messaging;

import com.example.fraud.config.KafkaTopicsConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Publishes verdicts to fraud.results, keyed by paymentId (spec §6). */
@Component
public class FraudResultPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public FraudResultPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    // Blocks on the broker ack so a failed emit propagates and the record is redelivered.
    public void publish(UUID paymentId, Object verdict) {
        try {
            kafkaTemplate.send(KafkaTopicsConfig.FRAUD_RESULTS, paymentId.toString(),
                    objectMapper.writeValueAsString(verdict)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing fraud verdict", e);
        } catch (ExecutionException | JsonProcessingException e) {
            throw new IllegalStateException("Failed to publish fraud verdict", e);
        }
    }
}
