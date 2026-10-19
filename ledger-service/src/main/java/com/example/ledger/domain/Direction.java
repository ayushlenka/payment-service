package com.example.ledger.domain;

/** Side of a ledger entry. Persisted as its name() in ledger_entries.direction. */
public enum Direction {
    DEBIT,
    CREDIT
}
