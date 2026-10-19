package com.example.payment.outbox;

import com.example.payment.config.KafkaTopicsConfig;
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
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelay(OutboxRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-interval-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxMessage> batch = outboxRepository.findByPublishedAtIsNullOrderByIdAsc(Limit.of(BATCH_SIZE));
        for (OutboxMessage message : batch) {
            try {
                // Block until the broker acks, so we only stamp rows that are truly on Kafka.
                kafkaTemplate.send(KafkaTopicsConfig.PAYMENT_COMMANDS, partitionKey(message),
                        message.getPayload().toString()).get();
                message.markPublished(Instant.now());
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

    // payment.commands is keyed by fromAccountId for per-account ordering (spec §6).
    private String partitionKey(OutboxMessage message) {
        return message.getPayload().get("fromAccountId").asText();
    }
}
