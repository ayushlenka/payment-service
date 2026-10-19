package com.example.payment.messaging;

import com.example.payment.domain.Payment;
import java.time.Instant;
import java.util.UUID;

/** Event published when a payment is accepted; versioned via {@code type}/{@code version}. */
public record PaymentInitiated(
        String type,
        int version,
        UUID paymentId,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency,
        Instant occurredAt) {

    public static final String TYPE = "PaymentInitiated";

    public static PaymentInitiated from(Payment payment) {
        return new PaymentInitiated(
                TYPE,
                1,
                payment.getId(),
                payment.getFromAccountId(),
                payment.getToAccountId(),
                payment.getAmountMinor(),
                payment.getCurrency(),
                Instant.now());
    }
}
