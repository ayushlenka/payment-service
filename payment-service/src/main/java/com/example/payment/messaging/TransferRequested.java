package com.example.payment.messaging;

import com.example.payment.domain.Payment;
import java.time.Instant;
import java.util.UUID;

/** Command published to the ledger once a payment passes fraud; carries the transfer to post. */
public record TransferRequested(
        String type,
        int version,
        UUID paymentId,
        UUID transferId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency,
        Instant occurredAt) {

    public static final String TYPE = "TransferRequested";

    public static TransferRequested from(Payment payment) {
        return new TransferRequested(TYPE, 1, payment.getId(), payment.getId(),
                payment.getFromAccountId(), payment.getToAccountId(),
                payment.getAmountMinor(), payment.getCurrency(), Instant.now());
    }
}
