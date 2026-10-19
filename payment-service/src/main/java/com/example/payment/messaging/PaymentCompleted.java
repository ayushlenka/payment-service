package com.example.payment.messaging;

import java.time.Instant;
import java.util.UUID;

/** Terminal saga event: the payment completed successfully. */
public record PaymentCompleted(String type, int version, UUID paymentId, Instant occurredAt) {

    public static final String TYPE = "PaymentCompleted";

    public static PaymentCompleted of(UUID paymentId) {
        return new PaymentCompleted(TYPE, 1, paymentId, Instant.now());
    }
}
