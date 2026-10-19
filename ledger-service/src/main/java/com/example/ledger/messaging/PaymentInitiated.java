package com.example.ledger.messaging;

import java.time.Instant;
import java.util.UUID;

/** Inbound event from payment.commands. Deserialized from the payment service's JSON. */
public record PaymentInitiated(
        String type,
        int version,
        UUID paymentId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency,
        Instant occurredAt) {
}
