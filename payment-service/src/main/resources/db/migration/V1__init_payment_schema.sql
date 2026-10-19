-- Milestone 1: payment-service schema.
-- Flyway runs this once and records it in the flyway_schema_history table.

-- ---------------------------------------------------------------------------
-- payments: one row per payment the orchestrator is driving through the saga.
-- ---------------------------------------------------------------------------
CREATE TABLE payments (
    id               UUID         NOT NULL PRIMARY KEY,
    idempotency_key  VARCHAR(255) NOT NULL,
    from_account_id  UUID         NOT NULL,
    to_account_id    UUID         NOT NULL,
    amount_minor     BIGINT       NOT NULL,
    currency         CHAR(3)      NOT NULL,
    -- Saga state. Modeled as VARCHAR + CHECK rather than a native PG enum so we
    -- can add states later with a plain migration (PG enums are awkward to alter).
    status           VARCHAR(32)  NOT NULL
        CONSTRAINT payments_status_check CHECK (status IN
            ('PENDING', 'FRAUD_CHECK', 'POSTING', 'COMPLETED', 'COMPENSATING', 'FAILED')),
    created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),

    -- Money is always a positive integer count of minor units (cents). Never float.
    CONSTRAINT payments_amount_positive CHECK (amount_minor > 0),
    -- A transfer must move money between two different accounts.
    CONSTRAINT payments_distinct_accounts CHECK (from_account_id <> to_account_id)
);

-- One payment per idempotency key. This unique constraint is the backbone of the
-- idempotency guarantee we build in Milestone 2.
CREATE UNIQUE INDEX ux_payments_idempotency_key ON payments (idempotency_key);

-- ---------------------------------------------------------------------------
-- idempotency_keys: remembers the response we returned for a given key so a
-- retry with the same key replays that response instead of doing work again.
-- ---------------------------------------------------------------------------
CREATE TABLE idempotency_keys (
    key                  VARCHAR(255) NOT NULL PRIMARY KEY,
    -- Hash of the normalized request body; lets us detect a key reused with a
    -- DIFFERENT payload (a client bug) and reject it rather than silently replay.
    request_fingerprint  VARCHAR(128) NOT NULL,
    response_status      INT,
    response_body        JSONB,
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- outbox: the transactional outbox. Rows are written in the SAME DB transaction
-- as the business change, then a relay publishes them to Kafka and stamps
-- published_at. This is what keeps Postgres and Kafka from disagreeing.
-- ---------------------------------------------------------------------------
CREATE TABLE outbox (
    id              BIGINT       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_type  VARCHAR(64)  NOT NULL,   -- e.g. 'payment'
    aggregate_id    UUID         NOT NULL,   -- e.g. the payment id
    event_type      VARCHAR(64)  NOT NULL,   -- e.g. 'PaymentInitiated'
    payload         JSONB        NOT NULL,
    headers         JSONB,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    published_at    TIMESTAMPTZ              -- NULL = not yet relayed to Kafka
);

-- The relay polls for unpublished rows in insertion order. A partial index on
-- (published_at IS NULL) keeps that scan cheap as the table grows.
CREATE INDEX ix_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
