package com.example.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.domain.Account;
import com.example.ledger.domain.AccountBalance;
import com.example.ledger.domain.AccountBalanceRepository;
import com.example.ledger.domain.AccountRepository;
import com.example.ledger.domain.LedgerEntry;
import com.example.ledger.domain.LedgerEntryRepository;
import com.example.ledger.domain.Direction;
import com.example.ledger.posting.LedgerPostingService;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;

/** Property tests: no sequence of transfers may create or destroy money. */
@SpringBootTest
class LedgerInvariantTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("ledger")
            .withUsername("payments")
            .withPassword("payments");

    static {
        POSTGRES.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "20");
    }

    @Autowired LedgerPostingService posting;
    @Autowired AccountRepository accounts;
    @Autowired AccountBalanceRepository balances;
    @Autowired LedgerEntryRepository ledgerEntries;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void resetTables() {
        jdbc.execute("TRUNCATE ledger_entries, account_balances, accounts");
    }

    @Test
    void randomizedTransfers_conserveTotalBalance() {
        List<UUID> ids = seedAccounts(10, 100_000);
        long openingTotal = totalBalance();

        for (int i = 0; i < 500; i++) {
            UUID from = ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
            UUID to = differentFrom(ids, from);
            long amount = ThreadLocalRandom.current().nextLong(1, 5_000);
            posting.postTransfer(UUID.randomUUID(), from, to, amount, "USD");
        }

        assertThat(totalBalance()).isEqualTo(openingTotal);   // money conserved
        assertThat(minBalance()).isGreaterThanOrEqualTo(0);   // never overdrawn
        assertThat(debitTotal()).isEqualTo(creditTotal());    // every leg balanced
    }

    @Test
    void concurrentTransfers_conserveTotalBalance() throws Exception {
        List<UUID> ids = seedAccounts(16, 100_000);
        long openingTotal = totalBalance();

        int threads = 8;
        int perThread = 25;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            futures.add(pool.submit(() -> {
                for (int i = 0; i < perThread; i++) {
                    UUID from = ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
                    UUID to = differentFrom(ids, from);
                    posting.postTransfer(UUID.randomUUID(), from, to,
                            ThreadLocalRandom.current().nextLong(1, 1_000), "USD");
                }
            }));
        }
        for (Future<?> future : futures) {
            future.get(60, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(totalBalance()).isEqualTo(openingTotal);
        assertThat(minBalance()).isGreaterThanOrEqualTo(0);
        assertThat(debitTotal()).isEqualTo(creditTotal());
    }

    private List<UUID> seedAccounts(int count, long opening) {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            UUID id = UUID.randomUUID();
            accounts.save(Account.open(id, "USD"));
            balances.save(AccountBalance.opening(id, opening));
            ids.add(id);
        }
        return ids;
    }

    private UUID differentFrom(List<UUID> ids, UUID from) {
        UUID to;
        do {
            to = ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
        } while (to.equals(from));
        return to;
    }

    private long totalBalance() {
        return balances.findAll().stream().mapToLong(AccountBalance::getBalanceMinor).sum();
    }

    private long minBalance() {
        return balances.findAll().stream().mapToLong(AccountBalance::getBalanceMinor).min().orElse(0);
    }

    private long debitTotal() {
        return sumEntries(Direction.DEBIT);
    }

    private long creditTotal() {
        return sumEntries(Direction.CREDIT);
    }

    private long sumEntries(Direction direction) {
        return ledgerEntries.findAll().stream()
                .filter(entry -> entry.getDirection() == direction)
                .mapToLong(LedgerEntry::getAmountMinor)
                .sum();
    }
}
