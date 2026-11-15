package com.campuslab.backend.listener;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.service.AnalyticsProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka para el tópico de eventos de reservas (campuslab.bookings.stream).
 * Consumer Group: campuslab-analytics-group (Balanceo de carga y aislamiento de dominio).
 */
@Component
public class AnalyticsStreamListener {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsStreamListener.class);

    private final AnalyticsProcessingService analyticsProcessingService;

    public AnalyticsStreamListener(AnalyticsProcessingService analyticsProcessingService) {
        this.analyticsProcessingService = analyticsProcessingService;
    }

    @KafkaListener(
            topics = "${campuslab.kafka.topics.bookings-stream:campuslab.bookings.stream}",
            groupId = "${campuslab.kafka.consumer-groups.analytics:campuslab-analytics-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onBookingStreamEvent(@Payload BookingStreamEventDto event,
                                     @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                     @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Kafka Listener [ANALYTICS CONSUMER] | Group: campuslab-analytics-group | Tópico: bookings.stream | Partición: {} | Offset: {} | Evento: {}",
                partition, offset, event.getEventId());

        // Manejo y validación de payload - Simulación deliberada de error irrecuperable para prueba de DLT
        if (event.getStudentEmail() != null && event.getStudentEmail().contains("trigger-dlt-error")) {
            log.warn("Kafka Listener [ERROR SIMULADO]: Detectado payload con 'trigger-dlt-error'. Lanzando excepción para activar DefaultErrorHandler y DLT...");
            throw new IllegalArgumentException("Fallo deliberado en procesamiento de reserva: " + event.getEventId());
        }

        // Delegar procesamiento a la capa de dominio
        analyticsProcessingService.processBookingEvent(event, partition, offset);
    }
}
