package com.example.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.domain.Account;
import com.example.ledger.domain.AccountBalance;
import com.example.ledger.domain.AccountBalanceRepository;
import com.example.ledger.domain.AccountRepository;
import com.example.ledger.domain.LedgerEntryRepository;
import com.example.ledger.messaging.PaymentInitiated;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

/** A poison command is retried, routed to the DLT, and does not block the partition. */
class LedgerDltIntegrationTest extends AbstractLedgerIntegrationTest {

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper objectMapper;
    @Autowired AccountRepository accounts;
    @Autowired AccountBalanceRepository balances;
    @Autowired LedgerEntryRepository ledgerEntries;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void resetTables() {
        jdbc.execute("TRUNCATE ledger_entries, account_balances, accounts");
    }

    @Test
    void poisonCommand_routesToDlt_withoutBlockingPartition() throws Exception {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        accounts.save(Account.open(from, "USD"));
        balances.save(AccountBalance.opening(from, 1_000_000));

        String key = from.toString(); // same key => poison and valid share a partition

        kafkaTemplate.send("payment.commands", key, "not-a-valid-json-payload").get();
        String valid = objectMapper.writeValueAsString(
                new PaymentInitiated("PaymentInitiated", 1, paymentId, from, to, 2500L, "USD", Instant.now()));
        kafkaTemplate.send("payment.commands", key, valid).get();

        // The valid command behind the poison is still processed.
        awaitUntil(() -> ledgerEntries.existsByTransferId(paymentId));
        // The poison ended up in the DLT.
        assertThat(recordsFrom("payment.commands.DLT", "not-a-valid-json-payload")).isNotEmpty();
    }

    private void awaitUntil(BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep();
        }
        throw new AssertionError("condition not met in time");
    }

    private List<String> recordsFrom(String topic, String substring) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<String> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline && matched.isEmpty()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                    if (record.value().contains(substring)) {
                        matched.add(record.value());
                    }
                }
            }
        }
        return matched;
    }

    private static void sleep() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
