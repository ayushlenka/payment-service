package com.example.fraud;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.fraud.messaging.PaymentInitiated;
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
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;

/** Verifies the fraud verdict for a payment, end to end over Kafka. */
@SpringBootTest
class FraudServiceIntegrationTest {

    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.8.0");

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", FraudServiceIntegrationTest::bootstrapServers);
    }

    @Autowired KafkaTemplate<String, String> kafkaTemplate;
    @Autowired ObjectMapper objectMapper;

    @Test
    void smallPayment_isApproved() throws Exception {
        UUID paymentId = publishRequest(2_500);
        assertThat(verdictFor(paymentId)).anyMatch(value -> value.contains("FraudApproved"));
    }

    @Test
    void largePayment_isRejected() throws Exception {
        UUID paymentId = publishRequest(2_000_000);
        assertThat(verdictFor(paymentId))
                .anyMatch(value -> value.contains("FraudRejected") && value.contains("AMOUNT_LIMIT"));
    }

    private UUID publishRequest(long amountMinor) throws Exception {
        UUID paymentId = UUID.randomUUID();
        UUID from = UUID.randomUUID();
        PaymentInitiated event = new PaymentInitiated(
                "PaymentInitiated", 1, paymentId, from, UUID.randomUUID(), amountMinor, "USD", Instant.now());
        kafkaTemplate.send("fraud.requests", from.toString(), objectMapper.writeValueAsString(event));
        kafkaTemplate.flush();
        return paymentId;
    }

    private List<String> verdictFor(UUID paymentId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<String> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("fraud.results"));
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

    private static String bootstrapServers() {
        return KAFKA.getBootstrapServers().replace("PLAINTEXT://", "");
    }
}
