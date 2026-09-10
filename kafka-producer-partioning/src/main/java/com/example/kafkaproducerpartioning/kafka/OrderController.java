package com.example.kafkaproducerpartioning.kafka;


import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducer orderProducer;

    public OrderController(OrderProducer orderProducer) {
        this.orderProducer = orderProducer;
    }

    @PostMapping("/simulate")
    public String simulateOrderLifecycle() {
        String orderId = "ORDER-101";

        // 1. Order Created
        orderProducer.publishOrderEvent(new OrderEvent(orderId, "CREATED", 150.00));
        
        // 2. Order Shipped
        orderProducer.publishOrderEvent(new OrderEvent(orderId, "SHIPPED", 150.00));
        
        // 3. Order Delivered
        orderProducer.publishOrderEvent(new OrderEvent(orderId, "DELIVERED", 150.00));

        return "Order lifecycle events published for " + orderId + ". Check the logs to see the partitions!";
    }
}