package com.example.payment.idempotency;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

/**
 * A processed Idempotency-Key and the response to replay. The key is the primary
 * key, so a duplicate insert fails and prevents re-processing.
 */
@Entity
@Table(name = "idempotency_keys")
public class IdempotencyKey {

    @Id
    @Column(name = "key", nullable = false, updatable = false, length = 255)
    private String key;

    @Column(name = "request_fingerprint", nullable = false, updatable = false, length = 128)
    private String requestFingerprint;

    @Column(name = "response_status")
    private Integer responseStatus;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "response_body")
    private JsonNode responseBody;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected IdempotencyKey() {
    }

    private IdempotencyKey(String key, String requestFingerprint, Integer responseStatus, JsonNode responseBody) {
        this.key = key;
        this.requestFingerprint = requestFingerprint;
        this.responseStatus = responseStatus;
        this.responseBody = responseBody;
    }

    public static IdempotencyKey completed(String key, String requestFingerprint,
                                           int responseStatus, JsonNode responseBody) {
        return new IdempotencyKey(key, requestFingerprint, responseStatus, responseBody);
    }

    public String getKey() {
        return key;
    }

    public String getRequestFingerprint() {
        return requestFingerprint;
    }

    public Integer getResponseStatus() {
        return responseStatus;
    }

    public JsonNode getResponseBody() {
        return responseBody;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
