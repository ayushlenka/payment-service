package com.example.payment.messaging;

import com.example.payment.config.KafkaTopicsConfig;
import com.example.payment.saga.PaymentSaga;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumes ledger results and advances the saga. */
@Component
public class LedgerEventListener {

    private final PaymentSaga saga;
    private final ObjectMapper objectMapper;

    public LedgerEventListener(PaymentSaga saga, ObjectMapper objectMapper) {
        this.saga = saga;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopicsConfig.LEDGER_EVENTS)
    public void onLedgerEvent(String message) {
        JsonNode event = parse(message);
        UUID paymentId = UUID.fromString(event.path("paymentId").asText());
        switch (event.path("type").asText()) {
            case "LedgerPosted" -> saga.onLedgerPosted(paymentId);
            case "LedgerRejected" -> saga.onLedgerRejected(paymentId, event.path("reason").asText());
            default -> { } // ignore event types we don't act on
        }
    }

    private JsonNode parse(String message) {
        try {
            return objectMapper.readTree(message);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unparseable ledger event", e);
        }
    }
}
