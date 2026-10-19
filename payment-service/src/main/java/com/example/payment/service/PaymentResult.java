package com.example.payment.service;

import com.example.payment.api.PaymentResponse;

/** Service result: the HTTP status and body, and whether it was an idempotent replay. */
public record PaymentResult(int httpStatus, PaymentResponse body, boolean replayed) {

    public static PaymentResult created(PaymentResponse body) {
        return new PaymentResult(201, body, false);
    }

    public static PaymentResult replayed(int httpStatus, PaymentResponse body) {
        return new PaymentResult(httpStatus, body, true);
    }
}
