package com.example.payment.service;

import com.example.payment.api.CreatePaymentRequest;
import com.example.payment.api.PaymentResponse;
import com.example.payment.domain.Payment;
import com.example.payment.domain.PaymentRepository;
import com.example.payment.idempotency.IdempotencyKey;
import com.example.payment.idempotency.IdempotencyKeyRepository;
import com.example.payment.messaging.PaymentInitiated;
import com.example.payment.outbox.OutboxMessage;
import com.example.payment.outbox.OutboxRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Writes the payment, its outbox event, and the idempotency record in one
 * transaction — never publishing to Kafka inline (the outbox avoids the
 * dual-write problem). A separate bean so Spring's @Transactional proxy applies.
 */
@Component
public class PaymentWriter {

    private final PaymentRepository paymentRepository;
    private final OutboxRepository outboxRepository;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final ObjectMapper objectMapper;

    public PaymentWriter(PaymentRepository paymentRepository,
                         OutboxRepository outboxRepository,
                         IdempotencyKeyRepository idempotencyKeyRepository,
                         ObjectMapper objectMapper) {
        this.paymentRepository = paymentRepository;
        this.outboxRepository = outboxRepository;
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public PaymentResponse write(String idempotencyKey, String requestFingerprint,
                                 CreatePaymentRequest request) {

        Payment payment = Payment.initiate(
                idempotencyKey,
                request.fromAccountId(),
                request.toAccountId(),
                request.amountMinor(),
                request.currency());
        // Flush now so a concurrent duplicate key fails fast inside this transaction.
        paymentRepository.saveAndFlush(payment);

        PaymentInitiated event = PaymentInitiated.from(payment);
        outboxRepository.save(OutboxMessage.forAggregate(
                "payment", payment.getId(), PaymentInitiated.TYPE, objectMapper.valueToTree(event)));

        PaymentResponse response = PaymentResponse.from(payment);
        idempotencyKeyRepository.save(IdempotencyKey.completed(
                idempotencyKey, requestFingerprint, 201, objectMapper.valueToTree(response)));

        return response;
    }
}
