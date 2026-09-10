package com.example.kafkaproducerpartioning.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);
    private static final String TOPIC = "orders-topic";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderEvent(OrderEvent event) {
        // 🚨 THE ENTERPRISE CONCEPT: PARTITION KEY 🚨
        // We pass event.orderId() as the KEY. 
        // This guarantees all events for "ORDER-123" go to the same partition,
        // ensuring they are processed in the exact order they were created.
        
        CompletableFuture<SendResult<String, OrderEvent>> future = 
            kafkaTemplate.send(TOPIC, event.orderId(), event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("✅ Sent event=[{}] with partition=[{}]", 
                    event, 
                    result.getRecordMetadata().partition());
            } else {
                log.error("❌ Unable to send event=[{}] due to: {}", 
                    event, ex.getMessage());
            }
        });
    }
}