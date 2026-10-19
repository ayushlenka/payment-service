package com.example.payment.service;

import com.example.payment.api.CreatePaymentRequest;
import com.example.payment.api.IdempotencyConflictException;
import com.example.payment.api.PaymentResponse;
import com.example.payment.idempotency.IdempotencyKey;
import com.example.payment.idempotency.IdempotencyKeyRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Optional;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Orchestrates POST /payments with idempotency: a new key creates the payment, a
 * repeated key replays the stored response, and a key reused with a different body
 * is rejected (409). Concurrent duplicates are resolved by the DB unique constraint
 * — the loser replays the winner's committed response.
 */
@Service
public class PaymentService {

    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final PaymentWriter paymentWriter;
    private final ObjectMapper objectMapper;

    public PaymentService(IdempotencyKeyRepository idempotencyKeyRepository,
                          PaymentWriter paymentWriter,
                          ObjectMapper objectMapper) {
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.paymentWriter = paymentWriter;
        this.objectMapper = objectMapper;
    }

    public PaymentResult createPayment(String idempotencyKey, CreatePaymentRequest request) {
        String fingerprint = fingerprint(request);

        Optional<IdempotencyKey> existing = idempotencyKeyRepository.findById(idempotencyKey);
        if (existing.isPresent()) {
            return replay(existing.get(), fingerprint);
        }

        try {
            PaymentResponse body = paymentWriter.write(idempotencyKey, fingerprint, request);
            return PaymentResult.created(body);
        } catch (DataIntegrityViolationException raceLost) {
            // A concurrent request with the same key won; replay its committed response.
            IdempotencyKey winner = idempotencyKeyRepository.findById(idempotencyKey)
                    .orElseThrow(() -> raceLost);
            return replay(winner, fingerprint);
        }
    }

    private PaymentResult replay(IdempotencyKey stored, String fingerprint) {
        if (!stored.getRequestFingerprint().equals(fingerprint)) {
            throw new IdempotencyConflictException(
                    "Idempotency-Key was already used with a different request body");
        }
        PaymentResponse body = objectMapper.convertValue(stored.getResponseBody(), PaymentResponse.class);
        return PaymentResult.replayed(stored.getResponseStatus(), body);
    }

    // Stable hash over the meaningful fields, independent of JSON formatting.
    private String fingerprint(CreatePaymentRequest r) {
        String canonical = r.fromAccountId() + "|" + r.toAccountId() + "|"
                + r.amountMinor() + "|" + r.currency().toUpperCase();
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256")
                    .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }
}
