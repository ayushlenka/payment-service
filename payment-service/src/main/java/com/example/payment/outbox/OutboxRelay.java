package com.example.payment.outbox;

import com.example.payment.config.KafkaTopicsConfig;
import com.example.payment.domain.PaymentRepository;
import com.example.payment.messaging.PaymentCompleted;
import com.example.payment.messaging.PaymentFailed;
import com.example.payment.messaging.PaymentInitiated;
import com.example.payment.messaging.TransferRequested;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Polls unpublished outbox rows and publishes them to Kafka, stamping published_at.
 * Delivery is at-least-once: a crash after the send but before the stamp commits
 * replays the row, and consumers dedupe on paymentId/transferId.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 100;

    private final OutboxRepository outboxRepository;
    private final PaymentRepository paymentRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelay(OutboxRepository outboxRepository, PaymentRepository paymentRepository,
                       KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.paymentRepository = paymentRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:1000}",
            initialDelayString = "${outbox.relay.initial-delay-ms:0}")
    @Transactional
    public void publishPending() {
        List<OutboxMessage> batch = outboxRepository.findByPublishedAtIsNullOrderByIdAsc(Limit.of(BATCH_SIZE));
        for (OutboxMessage message : batch) {
            try {
                Route route = routeFor(message);
                // Block until the broker acks, so we only stamp rows that are truly on Kafka.
                kafkaTemplate.send(route.topic(), route.key(), message.getPayload().toString()).get();
                message.markPublished(Instant.now());
                advanceSaga(message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ExecutionException e) {
                // Stop at the first failure to preserve order; this row retries next poll.
                log.warn("Outbox publish failed for id={}; retrying next poll", message.getId(), e);
                break;
            }
        }
    }

    // Once the initiation is on the wire for fraud screening, the payment is in FRAUD_CHECK.
    private void advanceSaga(OutboxMessage message) {
        if (PaymentInitiated.TYPE.equals(message.getEventType())) {
            paymentRepository.markFraudCheck(message.getAggregateId());
        }
    }

    private Route routeFor(OutboxMessage message) {
        JsonNode payload = message.getPayload();
        return switch (message.getEventType()) {
            // Keyed by fromAccountId for per-account ordering (spec §6).
            case PaymentInitiated.TYPE ->
                    new Route(KafkaTopicsConfig.FRAUD_REQUESTS, payload.get("fromAccountId").asText());
            case TransferRequested.TYPE ->
                    new Route(KafkaTopicsConfig.PAYMENT_COMMANDS, payload.get("fromAccountId").asText());
            case PaymentCompleted.TYPE, PaymentFailed.TYPE ->
                    new Route(KafkaTopicsConfig.PAYMENT_EVENTS, payload.get("paymentId").asText());
            default -> throw new IllegalStateException("No route for event type " + message.getEventType());
        };
    }

    private record Route(String topic, String key) {
    }
}
