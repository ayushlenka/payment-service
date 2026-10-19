package com.example.fraud.messaging;

import java.time.Instant;
import java.util.UUID;

/** Verdict: the payment was flagged by a fraud rule. */
public record FraudRejected(String type, int version, UUID paymentId, String reason, Instant occurredAt) {

    public static final String TYPE = "FraudRejected";

    public static FraudRejected of(UUID paymentId, String reason) {
        return new FraudRejected(TYPE, 1, paymentId, reason, Instant.now());
    }
}
