package com.example.ledger.messaging;

import com.example.ledger.config.KafkaTopicsConfig;
import com.example.ledger.posting.LedgerPostingService;
import com.example.ledger.posting.PostingResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes payment commands and posts them to the ledger. Redelivery is safe: the
 * posting is idempotent on transferId, and the result event is re-emitted (consumers
 * dedupe on paymentId).
 */
@Component
public class PaymentCommandListener {

    private final LedgerPostingService postingService;
    private final LedgerEventPublisher publisher;
    private final ObjectMapper objectMapper;

    public PaymentCommandListener(LedgerPostingService postingService, LedgerEventPublisher publisher,
                                  ObjectMapper objectMapper) {
        this.postingService = postingService;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = KafkaTopicsConfig.PAYMENT_COMMANDS)
    public void onPaymentCommand(String message) {
        PaymentInitiated event = parse(message);
        UUID transferId = event.paymentId(); // one transfer per payment
        PostingResult result = postingService.postTransfer(transferId, event.fromAccountId(),
                event.toAccountId(), event.amountMinor(), event.currency());

        if (result.outcome() == PostingResult.Outcome.REJECTED) {
            publisher.publish(event.fromAccountId(), LedgerRejected.of(event.paymentId(), result.reason()));
        } else {
            publisher.publish(event.fromAccountId(), LedgerPosted.of(event.paymentId(), transferId));
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
