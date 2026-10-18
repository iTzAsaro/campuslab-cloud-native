package com.campuslab.backend.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    @Value("${campuslab.rabbitmq.exchanges.direct:campuslab.direct}")
    private String directExchangeName;

    @Value("${campuslab.rabbitmq.exchanges.topic:campuslab.topic}")
    private String topicExchangeName;

    @Value("${campuslab.rabbitmq.queues.notifications:campuslab.booking.notifications}")
    private String notificationsQueueName;

    @Value("${campuslab.rabbitmq.queues.reports:campuslab.reports.generation}")
    private String reportsQueueName;

    @Value("${campuslab.rabbitmq.queues.audit:campuslab.audit.events}")
    private String auditQueueName;

    @Value("${campuslab.rabbitmq.queues.dlq.notifications:campuslab.booking.notifications.dlq}")
    private String dlqNotificationsName;

    @Value("${campuslab.rabbitmq.queues.dlq.reports:campuslab.reports.generation.dlq}")
    private String dlqReportsName;

    @Value("${campuslab.rabbitmq.queues.dlq.audit:campuslab.audit.events.dlq}")
    private String dlqAuditName;

    @Value("${campuslab.rabbitmq.routing-keys.notifications:routing.notifications}")
    private String routingNotifications;

    @Value("${campuslab.rabbitmq.routing-keys.reports:routing.reports.#}")
    private String routingReports;

    @Value("${campuslab.rabbitmq.routing-keys.audit:routing.audit.#}")
    private String routingAudit;

    // RabbitAdmin explicit bean
    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin admin = new RabbitAdmin(connectionFactory);
        admin.setAutoStartup(true);
        return admin;
    }

    // JSON Message Converter for robust serialization
    @Bean
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter jsonMessageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(jsonMessageConverter);
        return template;
    }

    // Exchanges
    @Bean
    public DirectExchange directExchange() {
        return new DirectExchange(directExchangeName);
    }

    @Bean
    public TopicExchange topicExchange() {
        return new TopicExchange(topicExchangeName);
    }

    // DLQs
    @Bean
    public Queue dlqNotifications() {
        return QueueBuilder.durable(dlqNotificationsName).build();
    }

    @Bean
    public Queue dlqReports() {
        return QueueBuilder.durable(dlqReportsName).build();
    }

    @Bean
    public Queue dlqAudit() {
        return QueueBuilder.durable(dlqAuditName).build();
    }

    // Normal Queues with Dead Letter routing
    @Bean
    public Queue notificationsQueue() {
        return QueueBuilder.durable(notificationsQueueName)
                .withArgument("x-dead-letter-exchange", directExchangeName)
                .withArgument("x-dead-letter-routing-key", dlqNotificationsName)
                .build();
    }

    @Bean
    public Queue reportsQueue() {
        return QueueBuilder.durable(reportsQueueName)
                .withArgument("x-dead-letter-exchange", directExchangeName)
                .withArgument("x-dead-letter-routing-key", dlqReportsName)
                .build();
    }

    @Bean
    public Queue auditQueue() {
        return QueueBuilder.durable(auditQueueName)
                .withArgument("x-dead-letter-exchange", directExchangeName)
                .withArgument("x-dead-letter-routing-key", dlqAuditName)
                .build();
    }

    // Bindings DLQ (Always Direct)
    @Bean
    public Binding bindingDlqNotifications() {
        return BindingBuilder.bind(dlqNotifications()).to(directExchange()).with(dlqNotificationsName);
    }

    @Bean
    public Binding bindingDlqReports() {
        return BindingBuilder.bind(dlqReports()).to(directExchange()).with(dlqReportsName);
    }

    @Bean
    public Binding bindingDlqAudit() {
        return BindingBuilder.bind(dlqAudit()).to(directExchange()).with(dlqAuditName);
    }

    // Bindings Normal
    @Bean
    public Binding bindingNotifications() {
        return BindingBuilder.bind(notificationsQueue()).to(directExchange()).with(routingNotifications);
    }

    @Bean
    public Binding bindingReports() {
        return BindingBuilder.bind(reportsQueue()).to(topicExchange()).with(routingReports);
    }

    @Bean
    public Binding bindingAudit() {
        return BindingBuilder.bind(auditQueue()).to(topicExchange()).with(routingAudit);
    }

    // Listener Factory config (MANUAL ACK)
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        return factory;
    }
}
