package com.example.ledger.posting;

import com.example.ledger.domain.Account;
import com.example.ledger.domain.AccountBalance;
import com.example.ledger.domain.AccountBalanceRepository;
import com.example.ledger.domain.AccountRepository;
import com.example.ledger.domain.LedgerEntry;
import com.example.ledger.domain.LedgerEntryRepository;
import java.util.UUID;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Posts one transfer as a balanced DEBIT + CREDIT pair under a shared transferId,
 * updating both balances, in a single transaction. Rejects if the source balance
 * would go negative. A separate bean so Spring's @Transactional proxy applies; the
 * retry/idempotency wrapper lives in {@link LedgerPostingService}.
 */
@Component
public class LedgerPoster {

    private final AccountRepository accounts;
    private final AccountBalanceRepository balances;
    private final LedgerEntryRepository ledgerEntries;

    public LedgerPoster(AccountRepository accounts, AccountBalanceRepository balances,
                        LedgerEntryRepository ledgerEntries) {
        this.accounts = accounts;
        this.balances = balances;
        this.ledgerEntries = ledgerEntries;
    }

    @Transactional
    public PostingResult post(UUID transferId, UUID fromAccountId, UUID toAccountId,
                              long amountMinor, String currency) {
        if (ledgerEntries.existsByTransferId(transferId)) {
            return PostingResult.alreadyPosted(transferId);
        }

        ensureAccount(fromAccountId, currency);
        ensureAccount(toAccountId, currency);

        AccountBalance from = balances.findById(fromAccountId).orElseThrow();
        if (from.getBalanceMinor() < amountMinor) {
            return PostingResult.rejected(transferId, "INSUFFICIENT_FUNDS");
        }
        AccountBalance to = balances.findById(toAccountId).orElseThrow();

        ledgerEntries.save(LedgerEntry.debit(transferId, fromAccountId, amountMinor));
        ledgerEntries.save(LedgerEntry.credit(transferId, toAccountId, amountMinor));

        applyBalancesInAccountOrder(from, to, amountMinor);
        return PostingResult.posted(transferId);
    }

    // Flush the two balance updates in ascending account order so concurrent transfers
    // between the same accounts lock rows in one order and can't deadlock.
    private void applyBalancesInAccountOrder(AccountBalance from, AccountBalance to, long amountMinor) {
        if (from.getAccountId().compareTo(to.getAccountId()) < 0) {
            from.debit(amountMinor);
            balances.saveAndFlush(from);
            to.credit(amountMinor);
            balances.saveAndFlush(to);
        } else {
            to.credit(amountMinor);
            balances.saveAndFlush(to);
            from.debit(amountMinor);
            balances.saveAndFlush(from);
        }
    }

    // Accounts are provisioned on first reference (zero balance); real systems would pre-create them.
    private void ensureAccount(UUID accountId, String currency) {
        if (!accounts.existsById(accountId)) {
            accounts.save(Account.open(accountId, currency));
            balances.save(AccountBalance.opening(accountId, 0));
        }
    }
}
