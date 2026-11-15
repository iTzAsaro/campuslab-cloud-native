package com.campuslab.backend.service;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

// Servicio para publicar eventos y métricas en tópicos de Apache Kafka.
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${campuslab.kafka.topics.bookings-stream:campuslab.bookings.stream}")
    private String bookingsStreamTopic;

    @Value("${campuslab.kafka.topics.telemetry-metrics:campuslab.telemetry.metrics}")
    private String telemetryMetricsTopic;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    // Publica un evento de reserva usando labId como clave para ordenar por partición
    public CompletableFuture<SendResult<String, Object>> publishBookingStreamEvent(BookingStreamEventDto event) {
        String partitionKey = event.getLabId() != null ? event.getLabId() : "DEFAULT_LAB";
        log.info("Kafka Producer: Publicando evento [{}] para lab [{}] en [{}]",
                event.getEventId(), partitionKey, bookingsStreamTopic);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(bookingsStreamTopic, partitionKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Kafka Producer: Evento [{}] confirmado en partición [{}] offset [{}]",
                        event.getEventId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Kafka Producer: Error enviando evento [{}]: {}",
                        event.getEventId(), ex.getMessage());
            }
        });

        return future;
    }

    // Publica métricas de telemetría IoT usando labCode como clave de particionado
    public CompletableFuture<SendResult<String, Object>> publishTelemetryMetric(LabTelemetryDto metric) {
        String partitionKey = metric.getLabCode() != null ? metric.getLabCode() : "DEFAULT_LAB";
        log.info("Kafka Producer: Enviando telemetría [{}] sensor [{}] en [{}]",
                metric.getMetricId(), metric.getSensorId(), telemetryMetricsTopic);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(telemetryMetricsTopic, partitionKey, metric);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Kafka Producer: Telemetría [{}] confirmada en partición [{}] offset [{}]",
                        metric.getMetricId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Kafka Producer: Error enviando telemetría [{}]: {}",
                        metric.getMetricId(), ex.getMessage());
            }
        });

        return future;
    }

    // Emite una ráfaga concurrente de eventos para probar balanceo entre particiones
    public void publishBurstEvents(int count) {
        log.info("Kafka Producer: Enviando ráfaga de {} eventos...", count);
        String[] labs = {"LAB-CLOUD-01", "LAB-CLOUD-02", "LAB-REDES-01"};
        String[] actions = {"RESERVATION_CREATED", "CHECK_IN", "SESSION_COMPLETED"};

        for (int i = 1; i <= count; i++) {
            String labId = labs[i % labs.length];
            String action = actions[i % actions.length];
            BookingStreamEventDto burstEvent = new BookingStreamEventDto(
                    "BURST-" + Long.toHexString(Double.doubleToLongBits(Math.random())).toUpperCase().substring(0, 8),
                    2000L + i,
                    labId,
                    "student." + i + "@campuslab.cl",
                    action,
                    60 + (i * 10),
                    (i % 3) + 1
            );
            publishBookingStreamEvent(burstEvent);
        }
    }
}
