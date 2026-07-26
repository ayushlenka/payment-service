-- Milestone 1: ledger-service schema — the double-entry ledger.

-- ---------------------------------------------------------------------------
-- accounts: the internal accounts money moves between.
-- ---------------------------------------------------------------------------
CREATE TABLE accounts (
    id          UUID        NOT NULL PRIMARY KEY,
    currency    CHAR(3)     NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- ledger_entries: the IMMUTABLE double-entry journal.
--
-- Every transfer of money is recorded as (at least) two rows sharing one
-- transfer_id: a DEBIT on the source account and a CREDIT on the destination,
-- of equal magnitude. Rows are append-only — corrections are new reversing
-- entries, never updates/deletes. This journal is the real audit trail; the
-- balances table below is just a cache of it.
-- ---------------------------------------------------------------------------
CREATE TABLE ledger_entries (
    id            UUID        NOT NULL PRIMARY KEY,
    transfer_id   UUID        NOT NULL,   -- groups the debit + credit of one transfer
    account_id    UUID        NOT NULL REFERENCES accounts (id),
    direction     VARCHAR(6)  NOT NULL
        CONSTRAINT ledger_entries_direction_check CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_minor  BIGINT      NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Amounts are always strictly positive; DEBIT vs CREDIT carries the sign.
    CONSTRAINT ledger_entries_amount_positive CHECK (amount_minor > 0),

    -- CONSUMER IDEMPOTENCY: the same posting redelivered by Kafka tries to insert
    -- the same (transfer_id, account_id, direction) triple again and hits this
    -- unique violation, which we catch and treat as an already-applied no-op.
    -- A simple transfer therefore has exactly one DEBIT and one CREDIT row.
    CONSTRAINT ux_ledger_entries_transfer_leg UNIQUE (transfer_id, account_id, direction)
);

-- We frequently read all legs of a transfer (e.g. to verify the zero-sum invariant).
CREATE INDEX ix_ledger_entries_transfer ON ledger_entries (transfer_id);
-- ...and all entries for an account (statement / balance recomputation).
CREATE INDEX ix_ledger_entries_account ON ledger_entries (account_id);

-- ---------------------------------------------------------------------------
-- account_balances: a MATERIALIZED balance per account, updated in the same
-- transaction as the journal entries.
--
-- 'version' backs JPA optimistic locking (@Version): a concurrent update that
-- read the same version loses on write and must retry, so two transfers hitting
-- the same account can never clobber each other's balance change. The DATABASE
-- transaction + this version column — not Kafka ordering — is our correctness
-- boundary (see the partition-key discussion in the spec §6 / README).
-- ---------------------------------------------------------------------------
CREATE TABLE account_balances (
    account_id     UUID   NOT NULL PRIMARY KEY REFERENCES accounts (id),
    balance_minor  BIGINT NOT NULL DEFAULT 0,
    version        BIGINT NOT NULL DEFAULT 0
);
