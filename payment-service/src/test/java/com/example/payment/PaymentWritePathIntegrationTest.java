package com.example.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.payment.api.CreatePaymentRequest;
import com.example.payment.domain.PaymentRepository;
import com.example.payment.outbox.OutboxRelay;
import com.example.payment.outbox.OutboxRepository;
import com.example.payment.service.PaymentResult;
import com.example.payment.service.PaymentService;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.Limit;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;

/** End-to-end tests of the write path against real Postgres + Kafka (Testcontainers). */
@SpringBootTest
class PaymentWritePathIntegrationTest {

    // Singleton containers: started once and reused across the test class.
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
        registry.add("spring.kafka.bootstrap-servers", PaymentWritePathIntegrationTest::bootstrapServers);
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "20");
        // Park the scheduled relay so tests invoke it explicitly.
        registry.add("outbox.relay.initial-delay-ms", () -> "3600000");
    }

    @Autowired PaymentService paymentService;
    @Autowired OutboxRelay outboxRelay;
    @Autowired PaymentRepository payments;
    @Autowired OutboxRepository outbox;
    @Autowired JdbcTemplate jdbc;

    @BeforeEach
    void resetTables() {
        jdbc.execute("TRUNCATE payments, outbox, idempotency_keys");
    }

    @Test
    void concurrentDuplicateKeys_createExactlyOnePayment() throws Exception {
        int threads = 16;
        String idempotencyKey = "concurrent-" + UUID.randomUUID();
        CreatePaymentRequest request =
                new CreatePaymentRequest(UUID.randomUUID(), UUID.randomUUID(), 1000, "USD");

        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch startGun = new CountDownLatch(1);
        List<PaymentResult> results = new CopyOnWriteArrayList<>();
        List<Throwable> failures = new CopyOnWriteArrayList<>();
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                try {
                    startGun.await();
                    results.add(paymentService.createPayment(idempotencyKey, request));
                } catch (Throwable t) {
                    failures.add(t);
                }
            }));
        }
        startGun.countDown(); // release every thread at once
        for (Future<?> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(failures).isEmpty();
        assertThat(results).hasSize(threads);
        assertThat(payments.count()).isEqualTo(1);
        // Every caller received the same payment.
        assertThat(results.stream().map(r -> r.body().paymentId()).distinct().toList()).hasSize(1);
    }

    @Test
    void outboxEventSurvivesCrashBeforePublish_andIsDeliveredOnRestart() {
        CreatePaymentRequest request =
                new CreatePaymentRequest(UUID.randomUUID(), UUID.randomUUID(), 2500, "USD");

        // Payment + outbox committed, but the relay has not run yet (the crash window).
        PaymentResult created = paymentService.createPayment("relay-" + UUID.randomUUID(), request);
        UUID paymentId = created.body().paymentId();

        assertThat(outbox.findByPublishedAtIsNullOrderByIdAsc(Limit.of(10))).hasSize(1);
        assertThat(recordsFor(paymentId)).isEmpty();

        // Restart: the relay drains the outbox to Kafka.
        outboxRelay.publishPending();

        assertThat(outbox.findByPublishedAtIsNullOrderByIdAsc(Limit.of(10))).isEmpty();
        List<ConsumerRecord<String, String>> delivered = recordsFor(paymentId);
        assertThat(delivered).hasSize(1);
        assertThat(delivered.get(0).key()).isEqualTo(request.fromAccountId().toString());

        // A second relay pass must not republish an already-stamped row.
        outboxRelay.publishPending();
        assertThat(recordsFor(paymentId)).hasSize(1);
    }

    // Reads fraud.requests from the start and keeps records mentioning this paymentId.
    private List<ConsumerRecord<String, String>> recordsFor(UUID paymentId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<ConsumerRecord<String, String>> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("fraud.requests"));
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(300))) {
                    if (record.value().contains(paymentId.toString())) {
                        matched.add(record);
                    }
                }
            }
        }
        return matched;
    }

    private static String bootstrapServers() {
        return KAFKA.getBootstrapServers().replace("PLAINTEXT://", "");
    }
}
