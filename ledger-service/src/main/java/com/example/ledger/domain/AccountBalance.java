package com.example.ledger.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.util.UUID;

/**
 * Materialized balance for an account. {@code @Version} gives optimistic locking:
 * two transfers touching the same account can't clobber each other — the losing
 * write fails and is retried.
 */
@Entity
@Table(name = "account_balances")
public class AccountBalance {

    @Id
    @Column(name = "account_id", nullable = false, updatable = false)
    private UUID accountId;

    @Column(name = "balance_minor", nullable = false)
    private long balanceMinor;

    @Version
    @Column(name = "version", nullable = false)
    private long version;

    protected AccountBalance() {
    }

    private AccountBalance(UUID accountId, long balanceMinor) {
        this.accountId = accountId;
        this.balanceMinor = balanceMinor;
    }

    public static AccountBalance opening(UUID accountId, long openingMinor) {
        return new AccountBalance(accountId, openingMinor);
    }

    public void credit(long amountMinor) {
        this.balanceMinor += amountMinor;
    }

    public void debit(long amountMinor) {
        this.balanceMinor -= amountMinor;
    }

    public UUID getAccountId() {
        return accountId;
    }

    public long getBalanceMinor() {
        return balanceMinor;
    }
}
