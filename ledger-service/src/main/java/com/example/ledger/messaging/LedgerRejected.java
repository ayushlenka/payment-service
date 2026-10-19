package com.example.ledger.messaging;

import java.time.Instant;
import java.util.UUID;

/** Emitted when a transfer could not be posted (e.g. INSUFFICIENT_FUNDS). */
public record LedgerRejected(String type, int version, UUID paymentId, String reason, Instant occurredAt) {

    public static final String TYPE = "LedgerRejected";

    public static LedgerRejected of(UUID paymentId, String reason) {
        return new LedgerRejected(TYPE, 1, paymentId, reason, Instant.now());
    }
}
