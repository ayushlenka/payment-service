package com.example.payment.domain;

/** Saga state of a payment, persisted as its name() in payments.status. */
public enum PaymentStatus {
    PENDING,
    FRAUD_CHECK,
    POSTING,
    COMPLETED,
    COMPENSATING,
    FAILED
}
