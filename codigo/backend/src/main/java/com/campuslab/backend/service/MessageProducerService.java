package com.campuslab.backend.service;

import com.campuslab.backend.dto.AuditEventDto;
import com.campuslab.backend.dto.NotificationRequestDto;
import com.campuslab.backend.dto.ReportRequestDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class MessageProducerService {

    private static final Logger log = LoggerFactory.getLogger(MessageProducerService.class);

    private final RabbitTemplate rabbitTemplate;

    @Value("${campuslab.rabbitmq.exchanges.direct:campuslab.direct}")
    private String directExchange;

    @Value("${campuslab.rabbitmq.exchanges.topic:campuslab.topic}")
    private String topicExchange;

    @Value("${campuslab.rabbitmq.routing-keys.notifications:routing.notifications}")
    private String routingNotifications;

    public MessageProducerService(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public void sendBookingNotification(NotificationRequestDto dto) {
        log.info("Publicando notificación a exchange '{}' con key '{}': {}", directExchange, routingNotifications, dto.getMessage());
        rabbitTemplate.convertAndSend(directExchange, routingNotifications, dto.getMessage());
    }

    public void sendReportRequest(ReportRequestDto dto) {
        String routingKey = "routing.reports.generate";
        log.info("Publicando solicitud de reporte a exchange '{}' con key '{}': {}", topicExchange, routingKey, dto.getReportName());
        rabbitTemplate.convertAndSend(topicExchange, routingKey, dto.getReportName() + " (" + dto.getFormat() + ")");
    }

    public void sendAuditEvent(AuditEventDto dto) {
        String routingKey = "routing.audit.log";
        log.info("Publicando evento de auditoría a exchange '{}' con key '{}': {}", topicExchange, routingKey, dto.getAction());
        rabbitTemplate.convertAndSend(topicExchange, routingKey, dto.getAction() + " - " + dto.getDetails());
    }

    public void sendSimulatedErrorNotification() {
        String errorMessage = "error: simulación de fallo para activar NACK y DLQ";
        log.warn("Publicando mensaje con 'error' intencional a exchange '{}' con key '{}'", directExchange, routingNotifications);
        rabbitTemplate.convertAndSend(directExchange, routingNotifications, errorMessage);
    }
}
