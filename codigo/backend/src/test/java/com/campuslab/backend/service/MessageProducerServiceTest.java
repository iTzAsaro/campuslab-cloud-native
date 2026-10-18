package com.campuslab.backend.service;

import com.campuslab.backend.dto.AuditEventDto;
import com.campuslab.backend.dto.NotificationRequestDto;
import com.campuslab.backend.dto.ReportRequestDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class MessageProducerServiceTest {

    @Mock
    private RabbitTemplate rabbitTemplate;

    private MessageProducerService messageProducerService;

    @BeforeEach
    void setUp() {
        messageProducerService = new MessageProducerService(rabbitTemplate);
        ReflectionTestUtils.setField(messageProducerService, "directExchange", "campuslab.direct");
        ReflectionTestUtils.setField(messageProducerService, "topicExchange", "campuslab.topic");
        ReflectionTestUtils.setField(messageProducerService, "routingNotifications", "routing.notifications");
    }

    @Test
    void testSendBookingNotification() {
        NotificationRequestDto dto = new NotificationRequestDto("test@campuslab.cl", "Mensaje de prueba", "CONFIRMATION");

        messageProducerService.sendBookingNotification(dto);

        verify(rabbitTemplate).convertAndSend(eq("campuslab.direct"), eq("routing.notifications"), eq("Mensaje de prueba"));
    }

    @Test
    void testSendReportRequest() {
        ReportRequestDto dto = new ReportRequestDto("ReporteMensual", "PDF", "admin");

        messageProducerService.sendReportRequest(dto);

        verify(rabbitTemplate).convertAndSend(eq("campuslab.topic"), eq("routing.reports.generate"), eq("ReporteMensual (PDF)"));
    }

    @Test
    void testSendAuditEvent() {
        AuditEventDto dto = new AuditEventDto("LOGIN", "Inicio de sesión exitoso", "test@campuslab.cl");

        messageProducerService.sendAuditEvent(dto);

        verify(rabbitTemplate).convertAndSend(eq("campuslab.topic"), eq("routing.audit.log"), eq("LOGIN - Inicio de sesión exitoso"));
    }

    @Test
    void testSendSimulatedErrorNotification() {
        messageProducerService.sendSimulatedErrorNotification();

        verify(rabbitTemplate).convertAndSend(eq("campuslab.direct"), eq("routing.notifications"), eq("error: simulación de fallo para activar NACK y DLQ"));
    }
}
