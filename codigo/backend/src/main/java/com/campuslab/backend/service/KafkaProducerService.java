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

/**
 * Servicio Productor de Apache Kafka para la plataforma CampusLab.
 * Publica eventos tipados estructurados utilizando Partition Keys para asegurar
 * el orden secuencial por laboratorio y maneja confirmaciones asíncronas con logging contextual.
 */
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

    /**
     * Publica un evento de streaming de reserva en Kafka.
     * Utiliza labId como llave de particionado (Partition Key).
     */
    public CompletableFuture<SendResult<String, Object>> publishBookingStreamEvent(BookingStreamEventDto event) {
        String partitionKey = event.getLabId() != null ? event.getLabId() : "DEFAULT_LAB";
        log.info("Kafka Producer [BOOKINGS]: Enviando evento [{}] tipo [{}] para lab [{}] hacia tópico [{}]",
                event.getEventId(), event.getAction(), partitionKey, bookingsStreamTopic);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(bookingsStreamTopic, partitionKey, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Kafka Producer [BOOKINGS SUCCESS]: Evento [{}] entregado a tópico [{}] partición [{}] con offset [{}]",
                        event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Kafka Producer [BOOKINGS ERROR]: Fallo al publicar evento [{}] hacia Kafka: {}",
                        event.getEventId(), ex.getMessage(), ex);
            }
        });

        return future;
    }

    /**
     * Publica una métrica de telemetría IoT de laboratorio en Kafka.
     * Utiliza labCode como Partition Key.
     */
    public CompletableFuture<SendResult<String, Object>> publishTelemetryMetric(LabTelemetryDto metric) {
        String partitionKey = metric.getLabCode() != null ? metric.getLabCode() : "DEFAULT_LAB";
        log.info("Kafka Producer [TELEMETRY]: Enviando telemetría [{}] sensor [{}] hacia tópico [{}]",
                metric.getMetricId(), metric.getSensorId(), telemetryMetricsTopic);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(telemetryMetricsTopic, partitionKey, metric);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Kafka Producer [TELEMETRY SUCCESS]: Métrica [{}] entregada a partición [{}] offset [{}]",
                        metric.getMetricId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Kafka Producer [TELEMETRY ERROR]: Fallo al publicar telemetría [{}] hacia Kafka: {}",
                        metric.getMetricId(), ex.getMessage(), ex);
            }
        });

        return future;
    }

    /**
     * Publica un payload genérico en cualquier tópico especificado.
     */
    public CompletableFuture<SendResult<String, Object>> publishGeneric(String topic, String key, Object payload) {
        log.info("Kafka Producer [GENERIC]: Enviando a tópico [{}] con key [{}]", topic, key);
        return kafkaTemplate.send(topic, key, payload);
    }
}
