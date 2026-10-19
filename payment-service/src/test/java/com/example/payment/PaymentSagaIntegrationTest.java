package com.example.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/** Saga tests: ledger results drive a seeded payment to its terminal state and emit the outcome. */
@SpringBootTest
class PaymentSagaIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("payments")
            .withUsername("payments")
            .withPassword("payments");
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    static {
        POSTGRES.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", PaymentSagaIntegrationTest::bootstrapServers);
    }

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper objectMapper;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void resetTables() {
        jdbc.execute("TRUNCATE payments, outbox, idempotency_keys");
    }

    @Test
    void ledgerPosted_completesPayment_andEmitsPaymentCompleted() throws Exception {
        UUID paymentId = seedPostingPayment();

        publishLedgerEvent(paymentId, ledgerPosted(paymentId));

        awaitStatus(paymentId, "COMPLETED");
        assertThat(eventsFor(paymentId)).anyMatch(value -> value.contains("PaymentCompleted"));
    }

    @Test
    void ledgerRejected_failsViaCompensation_andEmitsPaymentFailed() throws Exception {
        UUID paymentId = seedPostingPayment();

        publishLedgerEvent(paymentId, ledgerRejected(paymentId, "INSUFFICIENT_FUNDS"));

        awaitStatus(paymentId, "FAILED");
        assertThat(eventsFor(paymentId))
                .anyMatch(value -> value.contains("PaymentFailed") && value.contains("INSUFFICIENT_FUNDS"));
    }

    @Test
    void redeliveredLedgerPosted_doesNotEmitTwice() throws Exception {
        UUID paymentId = seedPostingPayment();
        String posted = ledgerPosted(paymentId);

        publishLedgerEvent(paymentId, posted);
        awaitStatus(paymentId, "COMPLETED");
        publishLedgerEvent(paymentId, posted); // redelivery
        Thread.sleep(3000);

        Integer completedRows = jdbc.queryForObject(
                "SELECT count(*) FROM outbox WHERE event_type = 'PaymentCompleted' AND aggregate_id = ?",
                Integer.class, paymentId);
        assertThat(completedRows).isEqualTo(1);
    }

    @Test
    void poisonLedgerEvent_routesToDlt_withoutBlockingSaga() throws Exception {
        UUID paymentId = seedPostingPayment();
        String key = paymentId.toString(); // same key => poison and valid share a partition

        kafkaTemplate.send("ledger.events", key, "not-a-valid-json-payload").get();
        publishLedgerEvent(paymentId, ledgerPosted(paymentId));

        awaitStatus(paymentId, "COMPLETED");
        assertThat(recordsFrom("ledger.events.DLT", "not-a-valid-json-payload")).isNotEmpty();
    }

    @Test
    void fraudApproved_movesToPosting_andRequestsTheTransfer() throws Exception {
        UUID paymentId = seedPayment("FRAUD_CHECK");

        publish("fraud.results", paymentId.toString(), fraudApproved(paymentId));

        awaitStatus(paymentId, "POSTING");
        assertThat(recordsFrom("payment.commands", paymentId.toString()))
                .anyMatch(value -> value.contains("TransferRequested"));
    }

    @Test
    void fraudRejected_failsPayment_andEmitsPaymentFailed() throws Exception {
        UUID paymentId = seedPayment("FRAUD_CHECK");

        publish("fraud.results", paymentId.toString(), fraudRejected(paymentId, "AMOUNT_LIMIT"));

        awaitStatus(paymentId, "FAILED");
        assertThat(recordsFrom("payment.events", paymentId.toString()))
                .anyMatch(value -> value.contains("PaymentFailed") && value.contains("AMOUNT_LIMIT"));
    }

    private UUID seedPostingPayment() {
        return seedPayment("POSTING");
    }

    private UUID seedPayment(String status) {
        UUID paymentId = UUID.randomUUID();
        jdbc.update("INSERT INTO payments "
                        + "(id, idempotency_key, from_account_id, to_account_id, amount_minor, currency, status) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?)",
                paymentId, "key-" + paymentId, UUID.randomUUID(), UUID.randomUUID(), 2500L, "USD", status);
        return paymentId;
    }

    private String ledgerPosted(UUID paymentId) throws Exception {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "LedgerPosted");
        event.put("version", 1);
        event.put("paymentId", paymentId);
        event.put("transferId", paymentId);
        event.put("occurredAt", Instant.now().toString());
        return objectMapper.writeValueAsString(event);
    }

    private String ledgerRejected(UUID paymentId, String reason) throws Exception {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "LedgerRejected");
        event.put("version", 1);
        event.put("paymentId", paymentId);
        event.put("reason", reason);
        event.put("occurredAt", Instant.now().toString());
        return objectMapper.writeValueAsString(event);
    }

    private String fraudApproved(UUID paymentId) throws Exception {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "FraudApproved");
        event.put("version", 1);
        event.put("paymentId", paymentId);
        event.put("occurredAt", Instant.now().toString());
        return objectMapper.writeValueAsString(event);
    }

    private String fraudRejected(UUID paymentId, String reason) throws Exception {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", "FraudRejected");
        event.put("version", 1);
        event.put("paymentId", paymentId);
        event.put("reason", reason);
        event.put("occurredAt", Instant.now().toString());
        return objectMapper.writeValueAsString(event);
    }

    private void publishLedgerEvent(UUID paymentId, String json) {
        publish("ledger.events", paymentId.toString(), json);
    }

    private void publish(String topic, String key, String json) {
        kafkaTemplate.send(topic, key, json);
        kafkaTemplate.flush();
    }

    private void awaitStatus(UUID paymentId, String expected) {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            String status = jdbc.queryForObject("SELECT status FROM payments WHERE id = ?", String.class, paymentId);
            if (expected.equals(status)) {
                return;
            }
            sleep(200);
        }
        throw new AssertionError("Payment " + paymentId + " did not reach " + expected);
    }

    // Reads payment.events until a record mentions this paymentId (or times out).
    private List<String> eventsFor(UUID paymentId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<String> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("payment.events"));
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

    // Reads any topic until a record contains the given substring (or times out).
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

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String bootstrapServers() {
        return KAFKA.getBootstrapServers().replace("PLAINTEXT://", "");
    }
}
