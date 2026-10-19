package com.example.payment.domain;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

/**
 * Spring Data repository for {@link Payment}. Spring generates the implementation
 * at runtime; we only declare the query methods we need.
 */
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    /** Look up an existing payment by its idempotency key (unique in the DB). */
    Optional<Payment> findByIdempotencyKey(String idempotencyKey);
}
