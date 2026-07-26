package com.example.ledger;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the ledger service — the source of truth for account balances.
 *
 * <p>Consumes payment events, posts balanced debit/credit pairs to an immutable
 * journal in a single DB transaction, and enforces the zero-sum invariant.
 */
@SpringBootApplication
public class LedgerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(LedgerServiceApplication.class, args);
    }
}
