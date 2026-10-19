package com.example.payment.saga;

import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentRepository;
import com.example.payment.messaging.PaymentCompleted;
import com.example.payment.messaging.PaymentFailed;
import com.example.payment.messaging.TransferRequested;
import com.example.payment.outbox.OutboxMessage;
import com.example.payment.outbox.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Drives the saga on ledger results: POSTING → COMPLETED on a posting, or
 * POSTING → COMPENSATING → FAILED on a rejection. Handlers are re-entrant, so a
 * redelivered ledger event resumes from the persisted state and emits once.
 */
@Service
public class PaymentSaga {

    private final PaymentRepository payments;
    private final OutboxRepository outbox;
    private final ObjectMapper objectMapper;

    public PaymentSaga(PaymentRepository payments, OutboxRepository outbox, ObjectMapper objectMapper) {
        this.payments = payments;
        this.outbox = outbox;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public void onFraudApproved(UUID paymentId) {
        if (payments.approveToPosting(paymentId) == 1) {
            // Now cleared, ask the ledger to post the transfer this payment represents.
            Payment payment = payments.findById(paymentId).orElseThrow();
            emit(paymentId, TransferRequested.TYPE, TransferRequested.from(payment));
        }
    }

    @Transactional
    public void onFraudRejected(UUID paymentId, String reason) {
        if (payments.rejectFromFraud(paymentId) == 1) {
            emit(paymentId, PaymentFailed.TYPE, PaymentFailed.of(paymentId, reason));
        }
    }

    @Transactional
    public void onLedgerPosted(UUID paymentId) {
        if (payments.complete(paymentId) == 1) {
            emit(paymentId, PaymentCompleted.TYPE, PaymentCompleted.of(paymentId));
        }
    }

    @Transactional
    public void onLedgerRejected(UUID paymentId, String reason) {
        payments.startCompensation(paymentId);   // → COMPENSATING (no-op if already past it)
        compensate(paymentId, reason);
        if (payments.fail(paymentId) == 1) {      // COMPENSATING → FAILED
            emit(paymentId, PaymentFailed.TYPE, PaymentFailed.of(paymentId, reason));
        }
    }

    /**
     * Reverses any real prior effect of the failed payment. Insufficient funds moved
     * no money, so there is nothing to reverse here — the PaymentFailed event emitted
     * by the caller is the compensating event. A money-reversal step belongs here once
     * a saga step actually moves funds before failing.
     */
    private void compensate(UUID paymentId, String reason) {
    }

    private void emit(UUID paymentId, String eventType, Object event) {
        outbox.save(OutboxMessage.forAggregate("payment", paymentId, eventType, objectMapper.valueToTree(event)));
    }
}
