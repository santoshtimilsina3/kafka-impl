package com.example.kafkaproducerpartioning.kafka;

public record OrderEvent(
    String orderId,
    String status, // e.g., CREATED, SHIPPED, DELIVERED
    double amount
) {}