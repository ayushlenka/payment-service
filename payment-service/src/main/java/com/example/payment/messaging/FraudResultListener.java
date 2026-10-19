package com.example.payment.messaging;

import com.example.payment.config.KafkaTopicsConfig;
import com.example.payment.saga.PaymentSaga;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Consumes fraud verdicts and advances the saga out of FRAUD_CHECK. */
@Component
public class FraudResultListener {

    private final PaymentSaga saga;
    private final ObjectMapper objectMapper;

    public FraudResultListener(PaymentSaga saga, ObjectMapper objectMapper) {
        this.saga = saga;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopicsConfig.FRAUD_RESULTS)
    public void onFraudResult(String message) {
        JsonNode event = parse(message);
        UUID paymentId = UUID.fromString(event.path("paymentId").asText());
        switch (event.path("type").asText()) {
            case "FraudApproved" -> saga.onFraudApproved(paymentId);
            case "FraudRejected" -> saga.onFraudRejected(paymentId, event.path("reason").asText());
            default -> { } // ignore event types we don't act on
        }
    }

    private JsonNode parse(String message) {
        try {
            return objectMapper.readTree(message);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unparseable fraud result", e);
        }
    }
}
