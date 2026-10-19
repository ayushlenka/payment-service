package com.example.payment.messaging;

import java.time.Instant;
import java.util.UUID;

/** Terminal saga event: the payment failed (and was compensated). */
public record PaymentFailed(String type, int version, UUID paymentId, String reason, Instant occurredAt) {

    public static final String TYPE = "PaymentFailed";

    public static PaymentFailed of(UUID paymentId, String reason) {
        return new PaymentFailed(TYPE, 1, paymentId, reason, Instant.now());
    }
}
