package com.example.fraud.messaging;

import com.example.fraud.config.KafkaTopicsConfig;
import com.example.fraud.rules.FraudDecision;
import com.example.fraud.rules.FraudEvaluator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Evaluates each payment and publishes an approve/reject verdict. */
@Component
public class FraudRequestListener {

    private final FraudEvaluator evaluator;
    private final FraudResultPublisher publisher;
    private final ObjectMapper objectMapper;

    public FraudRequestListener(FraudEvaluator evaluator, FraudResultPublisher publisher, ObjectMapper objectMapper) {
        this.evaluator = evaluator;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopicsConfig.FRAUD_REQUESTS)
    public void onPaymentInitiated(String message) {
        PaymentInitiated payment = parse(message);
        FraudDecision decision = evaluator.evaluate(payment);
        if (decision.approved()) {
            publisher.publish(payment.paymentId(), FraudApproved.of(payment.paymentId()));
        } else {
            publisher.publish(payment.paymentId(), FraudRejected.of(payment.paymentId(), decision.reason()));
        }
    }

    private PaymentInitiated parse(String message) {
        try {
            return objectMapper.readValue(message, PaymentInitiated.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unparseable payment command", e);
        }
    }
}
