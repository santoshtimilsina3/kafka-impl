package com.example.kafkaproducerpartioning.consumer;

import com.example.kafkaproducerpartioning.producer.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    // 🚨 THE ENTERPRISE CONCEPT: CONSUMER GROUP ID 🚨
    // Notice the groupId is DIFFERENT ("notification-group").
    // Because it's a different group, Kafka will send a COPY of the message to this consumer too!
    @KafkaListener(topics = "orders-topic", groupId = "notification-group")
    public void handleOrderEvent(OrderEvent event) {
        log.info("📧 [NOTIFICATION SERVICE] Received event: {} | Preparing email...", event);
        
        // Simulate calling an external email API
        try {
            Thread.sleep(1500); 
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        log.info("✅ [NOTIFICATION SERVICE] Email sent successfully to customer for Order: {}", event.orderId());
    }
}