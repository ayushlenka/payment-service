package com.example.payment.api;

import com.example.payment.domain.Payment;
import java.time.Instant;
import java.util.UUID;

/** Response for {@code POST /payments}; also stored verbatim for idempotent replay. */
public record PaymentResponse(
        UUID paymentId,
        String status,
        UUID fromAccountId,
        UUID toAccountId,
        long amountMinor,
        String currency,
        Instant createdAt) {

    public static PaymentResponse from(Payment payment) {
        return new PaymentResponse(
                payment.getId(),
                payment.getStatus().name(),
                payment.getFromAccountId(),
                payment.getToAccountId(),
                payment.getAmountMinor(),
                payment.getCurrency(),
                payment.getCreatedAt());
    }
}
