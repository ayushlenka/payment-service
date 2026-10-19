package com.example.ledger.posting;

import java.util.UUID;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;

/**
 * Entry point for posting transfers. Retries on optimistic-lock contention (a
 * concurrent transfer touched the same account) and treats a concurrent duplicate
 * of the same transfer as already posted.
 */
@Service
public class LedgerPostingService {

    private static final int MAX_ATTEMPTS = 20;

    private final LedgerPoster poster;

    public LedgerPostingService(LedgerPoster poster) {
        this.poster = poster;
    }

    public PostingResult postTransfer(UUID transferId, UUID fromAccountId, UUID toAccountId,
                                      long amountMinor, String currency) {
        for (int attempt = 1; ; attempt++) {
            try {
                return poster.post(transferId, fromAccountId, toAccountId, amountMinor, currency);
            } catch (ConcurrencyFailureException contention) {
                // Covers optimistic-version conflicts and transient lock failures.
                if (attempt >= MAX_ATTEMPTS) {
                    throw contention;
                }
                // Fresh transaction next iteration re-reads the current balance/version.
            } catch (DataIntegrityViolationException duplicate) {
                // Lost a race to post the same transfer; the unique constraint held.
                return PostingResult.alreadyPosted(transferId);
            }
        }
    }
}
