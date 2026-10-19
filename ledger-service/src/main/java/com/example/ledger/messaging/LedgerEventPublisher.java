package com.example.ledger.messaging;

import com.example.ledger.config.KafkaTopicsConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** Publishes ledger results to ledger.events, keyed by fromAccountId (spec §6). */
@Component
public class LedgerEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public LedgerEventPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    // Blocks on the broker ack so a failed emit propagates and the record is redelivered.
    public void publish(UUID key, Object event) {
        try {
            kafkaTemplate.send(KafkaTopicsConfig.LEDGER_EVENTS, key.toString(),
                    objectMapper.writeValueAsString(event)).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing ledger event", e);
        } catch (ExecutionException | JsonProcessingException e) {
            throw new IllegalStateException("Failed to publish ledger event", e);
        }
    }
}
