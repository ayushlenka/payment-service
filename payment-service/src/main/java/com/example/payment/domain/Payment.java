package com.example.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

/**
 * The Payment aggregate — one row in the {@code payments} table, tracked through
 * the saga by its {@link PaymentStatus}.
 *
 * <p>Mapping notes:
 * <ul>
 *   <li>{@code ddl-auto=validate} means Hibernate checks these fields against the
 *       Flyway-created columns at startup but never alters the schema.</li>
 *   <li>Status is stored as its enum name ({@code EnumType.STRING}) so the DB stays
 *       human-readable and the CHECK constraint keeps working — never ORDINAL, which
 *       would silently corrupt if we reordered the enum.</li>
 * </ul>
 */
@Entity
@Table(name = "payments")
public class Payment {

    @Id
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "idempotency_key", nullable = false, updatable = false)
    private String idempotencyKey;

    @Column(name = "from_account_id", nullable = false, updatable = false)
    private UUID fromAccountId;

    @Column(name = "to_account_id", nullable = false, updatable = false)
    private UUID toAccountId;

    @Column(name = "amount_minor", nullable = false, updatable = false)
    private long amountMinor;

    // The DB column is char(3) (bpchar). Map it explicitly to JDBC CHAR so
    // ddl-auto=validate matches, instead of Hibernate's default varchar for String.
    @JdbcTypeCode(SqlTypes.CHAR)
    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private PaymentStatus status;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    /** JPA requires a no-arg constructor; not for application use. */
    protected Payment() {
    }

    private Payment(UUID id, String idempotencyKey, UUID fromAccountId, UUID toAccountId,
                    long amountMinor, String currency, PaymentStatus status) {
        this.id = id;
        this.idempotencyKey = idempotencyKey;
        this.fromAccountId = fromAccountId;
        this.toAccountId = toAccountId;
        this.amountMinor = amountMinor;
        this.currency = currency;
        this.status = status;
    }

    /**
     * Create a brand-new payment in {@link PaymentStatus#PENDING} with a fresh id.
     * This is the only way to construct one, so a Payment can never be born in a
     * downstream state.
     */
    public static Payment initiate(String idempotencyKey, UUID fromAccountId, UUID toAccountId,
                                   long amountMinor, String currency) {
        return new Payment(UUID.randomUUID(), idempotencyKey, fromAccountId, toAccountId,
                amountMinor, currency, PaymentStatus.PENDING);
    }

    public UUID getId() {
        return id;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public UUID getFromAccountId() {
        return fromAccountId;
    }

    public UUID getToAccountId() {
        return toAccountId;
    }

    public long getAmountMinor() {
        return amountMinor;
    }

    public String getCurrency() {
        return currency;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
