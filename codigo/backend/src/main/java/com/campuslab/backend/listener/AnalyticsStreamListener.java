package com.campuslab.backend.listener;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.service.AnalyticsProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Consumidor para eventos de reservas desde el tópico campuslab.bookings.stream.
@Component
public class AnalyticsStreamListener {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsStreamListener.class);

    private final AnalyticsProcessingService analyticsProcessingService;

    public AnalyticsStreamListener(AnalyticsProcessingService analyticsProcessingService) {
        this.analyticsProcessingService = analyticsProcessingService;
    }

    // Escucha eventos de reservas; si contiene "trigger-dlt-error" simula un fallo para activar el DLT
    @KafkaListener(
            topics = "${campuslab.kafka.topics.bookings-stream:campuslab.bookings.stream}",
            groupId = "${campuslab.kafka.consumer-groups.analytics:campuslab-analytics-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onBookingStreamEvent(@Payload BookingStreamEventDto event,
                                     @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                     @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Kafka Listener: Recibido evento [{}] en partición [{}] offset [{}]",
                event.getEventId(), partition, offset);

        if (event.getStudentEmail() != null && event.getStudentEmail().contains("trigger-dlt-error")) {
            log.warn("Kafka Listener: Simulación de fallo para activar DLT en evento [{}]", event.getEventId());
            throw new IllegalArgumentException("Fallo deliberado para probar DLT: " + event.getEventId());
        }

        analyticsProcessingService.processBookingEvent(event, partition, offset);
    }
}
