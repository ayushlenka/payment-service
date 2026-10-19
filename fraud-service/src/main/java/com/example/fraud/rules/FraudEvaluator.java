package com.example.fraud.rules;

import com.example.fraud.messaging.PaymentInitiated;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Applies fraud rules to a payment. Two rules: a hard amount ceiling, and a
 * per-account velocity limit over a rolling window (a simple in-memory counter;
 * production would use Kafka Streams windowed state).
 */
@Component
public class FraudEvaluator {

    private final long amountThresholdMinor;
    private final int maxPerWindow;
    private final Duration window;
    private final Map<UUID, Deque<Instant>> recentByAccount = new ConcurrentHashMap<>();

    public FraudEvaluator(@Value("${fraud.amount-threshold-minor}") long amountThresholdMinor,
                          @Value("${fraud.velocity.max-per-window}") int maxPerWindow,
                          @Value("${fraud.velocity.window-seconds}") long windowSeconds) {
        this.amountThresholdMinor = amountThresholdMinor;
        this.maxPerWindow = maxPerWindow;
        this.window = Duration.ofSeconds(windowSeconds);
    }

    public FraudDecision evaluate(PaymentInitiated payment) {
        if (payment.amountMinor() >= amountThresholdMinor) {
            return FraudDecision.reject("AMOUNT_LIMIT");
        }
        if (velocityExceeded(payment.fromAccountId())) {
            return FraudDecision.reject("VELOCITY_LIMIT");
        }
        return FraudDecision.approve();
    }

    private boolean velocityExceeded(UUID accountId) {
        Instant now = Instant.now();
        Deque<Instant> timestamps = recentByAccount.computeIfAbsent(accountId, k -> new ArrayDeque<>());
        synchronized (timestamps) {
            timestamps.addLast(now);
            while (!timestamps.isEmpty() && timestamps.peekFirst().isBefore(now.minus(window))) {
                timestamps.pollFirst();
            }
            return timestamps.size() > maxPerWindow;
        }
    }
}
