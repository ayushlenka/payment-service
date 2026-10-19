package com.example.payment.domain;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    Optional<Payment> findByIdempotencyKey(String idempotencyKey);

    /**
     * Conditional saga transition. Updates the row only if it is in one of the
     * allowed source states, so concurrent transitions can't clobber each other;
     * the returned count tells the caller whether this call made the change.
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("update Payment p set p.status = :to, p.updatedAt = :now "
            + "where p.id = :id and p.status in :fromStates")
    int transition(@Param("id") UUID id,
                   @Param("fromStates") Collection<PaymentStatus> fromStates,
                   @Param("to") PaymentStatus to,
                   @Param("now") Instant now);

    default int markFraudCheck(UUID id) {
        return transition(id, List.of(PaymentStatus.PENDING), PaymentStatus.FRAUD_CHECK, Instant.now());
    }

    default int approveToPosting(UUID id) {
        return transition(id, List.of(PaymentStatus.PENDING, PaymentStatus.FRAUD_CHECK),
                PaymentStatus.POSTING, Instant.now());
    }

    default int rejectFromFraud(UUID id) {
        return transition(id, List.of(PaymentStatus.PENDING, PaymentStatus.FRAUD_CHECK),
                PaymentStatus.FAILED, Instant.now());
    }

    default int complete(UUID id) {
        return transition(id, List.of(PaymentStatus.POSTING), PaymentStatus.COMPLETED, Instant.now());
    }

    default int startCompensation(UUID id) {
        return transition(id, List.of(PaymentStatus.POSTING), PaymentStatus.COMPENSATING, Instant.now());
    }

    default int fail(UUID id) {
        return transition(id, List.of(PaymentStatus.COMPENSATING), PaymentStatus.FAILED, Instant.now());
    }
}
