package com.example.payment.domain;

/**
 * The saga state of a payment. Persisted as its name() string in the
 * payments.status column (a VARCHAR guarded by a CHECK constraint).
 *
 * <p>Transitions (owned by the payment orchestrator):
 * <pre>
 *   PENDING ──PaymentInitiated published──▶ FRAUD_CHECK
 *   FRAUD_CHECK ──FraudApproved──▶ POSTING        (Fraud is optional; may skip to POSTING)
 *   FRAUD_CHECK ──FraudRejected──▶ FAILED
 *   POSTING ──LedgerPosted──▶ COMPLETED
 *   POSTING ──LedgerRejected──▶ COMPENSATING ──▶ FAILED
 * </pre>
 */
public enum PaymentStatus {
    PENDING,
    FRAUD_CHECK,
    POSTING,
    COMPLETED,
    COMPENSATING,
    FAILED
}
