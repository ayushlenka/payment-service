package com.example.fraud.messaging;

import java.time.Instant;
import java.util.UUID;

/** Verdict: the payment passed the fraud rules. */
public record FraudApproved(String type, int version, UUID paymentId, Instant occurredAt) {

    public static final String TYPE = "FraudApproved";

    public static FraudApproved of(UUID paymentId) {
        return new FraudApproved(TYPE, 1, paymentId, Instant.now());
    }
}
