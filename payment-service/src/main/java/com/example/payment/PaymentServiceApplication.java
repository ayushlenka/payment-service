package com.example.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the payment orchestrator service.
 *
 * <p>{@code @SpringBootApplication} enables component scanning over this package
 * ({@code com.example.payment}) and below, plus Spring Boot auto-configuration
 * (DataSource, JPA, Flyway, the embedded web server, Actuator, etc.).
 */
@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
