package com.example.ledger.messaging;

import java.time.Instant;
import java.util.UUID;

/** Emitted when a transfer was posted to the ledger. */
public record LedgerPosted(String type, int version, UUID paymentId, UUID transferId, Instant occurredAt) {

    public static final String TYPE = "LedgerPosted";

    public static LedgerPosted of(UUID paymentId, UUID transferId) {
        return new LedgerPosted(TYPE, 1, paymentId, transferId, Instant.now());
    }
}
