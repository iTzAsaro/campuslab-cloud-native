package com.campuslab.backend.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.stereotype.Service;

@Service
public class RabbitAdminService {

    private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);
    private final RabbitAdmin rabbitAdmin;

    public RabbitAdminService(RabbitAdmin rabbitAdmin) {
        this.rabbitAdmin = rabbitAdmin;
    }

    public void createQueue(String queueName, boolean durable) {
        log.info("Declarando cola: {} (durable={})", queueName, durable);
        Queue queue = new Queue(queueName, durable);
        rabbitAdmin.declareQueue(queue);
    }

    public boolean deleteQueue(String queueName) {
        log.info("Eliminando cola: {}", queueName);
        return rabbitAdmin.deleteQueue(queueName);
    }

    public void createExchange(String exchangeName, String type, boolean durable) {
        log.info("Declarando exchange: {} tipo: {} (durable={})", exchangeName, type, durable);
        Exchange exchange;
        switch (type.toLowerCase()) {
            case "topic":
                exchange = new TopicExchange(exchangeName, durable, false);
                break;
            case "fanout":
                exchange = new FanoutExchange(exchangeName, durable, false);
                break;
            case "headers":
                exchange = new HeadersExchange(exchangeName, durable, false);
                break;
            case "direct":
            default:
                exchange = new DirectExchange(exchangeName, durable, false);
                break;
        }
        rabbitAdmin.declareExchange(exchange);
    }

    public boolean deleteExchange(String exchangeName) {
        log.info("Eliminando exchange: {}", exchangeName);
        return rabbitAdmin.deleteExchange(exchangeName);
    }

    public void createBinding(String queueName, String exchangeName, String routingKey) {
        log.info("Vinculando cola '{}' con exchange '{}' (routingKey='{}')", queueName, exchangeName, routingKey);
        Binding binding = new Binding(
                queueName,
                Binding.DestinationType.QUEUE,
                exchangeName,
                routingKey,
                null
        );
        rabbitAdmin.declareBinding(binding);
    }

    public void removeBinding(String queueName, String exchangeName, String routingKey) {
        log.info("Desvinculando cola '{}' de exchange '{}' (routingKey='{}')", queueName, exchangeName, routingKey);
        Binding binding = new Binding(
                queueName,
                Binding.DestinationType.QUEUE,
                exchangeName,
                routingKey,
                null
        );
        rabbitAdmin.removeBinding(binding);
    }

    public void purgeQueue(String queueName) {
        log.info("Purgando mensajes de cola: {}", queueName);
        rabbitAdmin.purgeQueue(queueName);
    }
}
