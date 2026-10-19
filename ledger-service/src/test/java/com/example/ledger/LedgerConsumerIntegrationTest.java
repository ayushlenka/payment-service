package com.example.ledger;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.ledger.domain.Account;
import com.example.ledger.domain.AccountBalance;
import com.example.ledger.domain.AccountBalanceRepository;
import com.example.ledger.domain.AccountRepository;
import com.example.ledger.domain.LedgerEntryRepository;
import com.example.ledger.messaging.PaymentInitiated;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;

/** End-to-end: a payment command on the topic drives a ledger posting and a result event. */
class LedgerConsumerIntegrationTest extends AbstractLedgerIntegrationTest {

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
    void fundedTransfer_isPostedAndEmitsLedgerPosted() throws Exception {
        UUID from = UUID.randomUUID();
        UUID to = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();
        accounts.save(Account.open(from, "USD"));
        balances.save(AccountBalance.opening(from, 1_000_000));

        publishPaymentCommand(paymentId, from, to, 2_500);

        List<String> events = awaitEventsFor(paymentId);
        assertThat(events).anyMatch(value -> value.contains("LedgerPosted"));
        assertThat(balances.findById(from).orElseThrow().getBalanceMinor()).isEqualTo(997_500);
        assertThat(balances.findById(to).orElseThrow().getBalanceMinor()).isEqualTo(2_500);
    }

    @Test
    void unfundedTransfer_isRejectedAndWritesNoEntries() throws Exception {
        UUID from = UUID.randomUUID();  // never funded -> zero balance on first reference
        UUID to = UUID.randomUUID();
        UUID paymentId = UUID.randomUUID();

        publishPaymentCommand(paymentId, from, to, 5_000);

        List<String> events = awaitEventsFor(paymentId);
        assertThat(events).anyMatch(value -> value.contains("LedgerRejected") && value.contains("INSUFFICIENT_FUNDS"));
        assertThat(ledgerEntries.existsByTransferId(paymentId)).isFalse();
    }

    private void publishPaymentCommand(UUID paymentId, UUID from, UUID to, long amount)
            throws JsonProcessingException {
        PaymentInitiated event =
                new PaymentInitiated("PaymentInitiated", 1, paymentId, from, to, amount, "USD", Instant.now());
        kafkaTemplate.send("payment.commands", from.toString(), objectMapper.writeValueAsString(event));
        kafkaTemplate.flush();
    }

    // Reads ledger.events until a record mentions this paymentId (or times out).
    private List<String> awaitEventsFor(UUID paymentId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<String> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("ledger.events"));
            long deadline = System.currentTimeMillis() + 20_000;
            while (System.currentTimeMillis() < deadline && matched.isEmpty()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                    if (record.value().contains(paymentId.toString())) {
                        matched.add(record.value());
                    }
                }
            }
        }
        return matched;
    }
}
