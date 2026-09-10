package com.example.kafkaproducerpartioning.consumer;

import com.example.kafkaproducerpartioning.producer.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    // 🚨 THE ENTERPRISE CONCEPT: CONSUMER GROUP ID 🚨
    // We assign this consumer to the "inventory-group".
    @KafkaListener(topics = "orders-topic", groupId = "inventory-group")
    public void handleOrderEvent(OrderEvent event) {
        log.info("📦 [INVENTORY SERVICE] Received event: {} | Reserving stock...", event);
        
        // Simulate some heavy database work to reserve stock
        try {
            Thread.sleep(1000); 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        log.info("✅ [INVENTORY SERVICE] Stock reserved successfully for Order: {}", event.orderId());
    }
}