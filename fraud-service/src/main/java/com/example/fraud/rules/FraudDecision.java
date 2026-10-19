package com.example.fraud.rules;

/** Verdict of the fraud rules: approved, or rejected with a reason. */
public record FraudDecision(boolean approved, String reason) {

    public static FraudDecision approve() {
        return new FraudDecision(true, null);
    }

    public static FraudDecision reject(String reason) {
        return new FraudDecision(false, reason);
    }
}
