package com.example.kafkaproducerpartioning.producer;

public record OrderEvent(
    String orderId,
    String status, // e.g., CREATED, SHIPPED, DELIVERED
    double amount
) {}