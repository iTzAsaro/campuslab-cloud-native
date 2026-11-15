package com.campuslab.backend.controller;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import com.campuslab.backend.service.AnalyticsProcessingService;
import com.campuslab.backend.service.KafkaProducerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * =========================================================================================
 * CONTROLADOR REST: INGESTA DE EVENTOS Y MOTOR ANALÍTICO (KAFKA STREAM CONTROLLER)
 * =========================================================================================
 * 
 * Expone los endpoints de ingesta de eventos de streaming, métricas de telemetría IoT,
 * simulación de ráfagas balanceadas, pruebas deliberadas de resiliencia DLT y consulta de analítica.
 * 
 * Endpoints:
 * - `POST /api/stream/kafka/bookings`           -> Publica un evento de ciclo de vida de reserva.
 * - `POST /api/stream/kafka/telemetry`          -> Transmite una métrica de telemetría de sensor.
 * - `POST /api/stream/kafka/simulate-load`      -> Dispara una ráfaga de 10 eventos distribuidos por partición.
 * - `POST /api/stream/kafka/trigger-dlt-error`  -> Inyecta un mensaje corrupto para forzar reintentos y DLT.
 * - `GET  /api/stream/kafka/analytics`          -> Retorna las métricas agregadas en tiempo real.
 */
@RestController
@RequestMapping("/api/stream/kafka")
@CrossOrigin(origins = "*")
public class KafkaStreamController {

    private final KafkaProducerService kafkaProducerService;
    private final AnalyticsProcessingService analyticsProcessingService;

    public KafkaStreamController(KafkaProducerService kafkaProducerService,
                                 AnalyticsProcessingService analyticsProcessingService) {
        this.kafkaProducerService = kafkaProducerService;
        this.analyticsProcessingService = analyticsProcessingService;
    }

    /**
     * Publica un evento de streaming de reserva en `campuslab.bookings.stream` particionado por labId.
     */
    @PostMapping("/bookings")
    public ResponseEntity<Map<String, Object>> publishBookingEvent(@Valid @RequestBody BookingStreamEventDto event) {
        if (event.getEventId() == null) {
            event.setEventId("EVT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        }
        if (event.getTimestamp() == null) {
            event.setTimestamp(Instant.now());
        }

        kafkaProducerService.publishBookingStreamEvent(event);

        return ResponseEntity.ok(Map.of(
                "status", "QUEUED_TO_KAFKA",
                "topic", "campuslab.bookings.stream",
                "partitionKey", event.getLabId(),
                "event", event
        ));
    }

    /**
     * Transmite una lectura de telemetría IoT de sensores hacia `campuslab.telemetry.metrics`.
     */
    @PostMapping("/telemetry")
    public ResponseEntity<Map<String, Object>> publishTelemetry(@Valid @RequestBody LabTelemetryDto telemetry) {
        if (telemetry.getMetricId() == null) {
            telemetry.setMetricId("TEL-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        }
        if (telemetry.getTimestamp() == null) {
            telemetry.setTimestamp(Instant.now());
        }

        kafkaProducerService.publishTelemetryMetric(telemetry);

        return ResponseEntity.ok(Map.of(
                "status", "QUEUED_TO_KAFKA",
                "topic", "campuslab.telemetry.metrics",
                "partitionKey", telemetry.getLabCode(),
                "metric", telemetry
        ));
    }

    /**
     * Dispara una ráfaga concurrente de 10 eventos distribuidos uniformemente
     * entre las 3 particiones para demostrar balanceo y particionado en Kafka-UI.
     */
    @PostMapping("/simulate-load")
    public ResponseEntity<Map<String, Object>> simulateLoad() {
        String[] labs = {"LAB-CLOUD-01", "LAB-CLOUD-02", "LAB-REDES-01"};
        String[] actions = {"RESERVATION_CREATED", "CHECK_IN", "SESSION_COMPLETED"};
        java.util.List<BookingStreamEventDto> published = new java.util.ArrayList<>();

        for (int i = 1; i <= 10; i++) {
            String lab = labs[i % labs.length];
            String action = actions[i % actions.length];
            BookingStreamEventDto event = new BookingStreamEventDto(
                    "BURST-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                    2000L + i,
                    lab,
                    "student." + i + "@campuslab.cl",
                    action,
                    60 + (i * 10),
                    1 + (i % 3)
            );
            kafkaProducerService.publishBookingStreamEvent(event);
            published.add(event);
        }

        return ResponseEntity.ok(Map.of(
                "status", "BURST_PUBLISHED",
                "totalEvents", published.size(),
                "events", published,
                "note", "Eventos distribuidos entre particiones de Kafka utilizando labId como clave"
        ));
    }

    /**
     * Inyecta intencionalmente un payload con 'trigger-dlt-error' para activar la prueba de resiliencia:
     * El listener fallará, ejecutará 2 reintentos via FixedBackOff(1000L, 2) y derivará el mensaje
     * al tópico Dead Letter Topic (`campuslab.bookings.stream.DLT`).
     */
    @PostMapping("/trigger-dlt-error")
    public ResponseEntity<Map<String, Object>> triggerDltError() {
        BookingStreamEventDto poisonPill = new BookingStreamEventDto(
                "POISON-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                9999L,
                "LAB-ERROR-TEST",
                "trigger-dlt-error@campuslab.cl",
                "CORRUPT_TRANSACTION",
                0,
                0
        );

        kafkaProducerService.publishBookingStreamEvent(poisonPill);

        return ResponseEntity.ok(Map.of(
                "status", "POISON_PILL_PUBLISHED",
                "event", poisonPill,
                "expectedBehavior", "El listener fallará, ejecutará 2 reintentos via FixedBackOff y lo desviará a campuslab.bookings.stream.DLT"
        ));
    }

    /**
     * Retorna el snapshot consolidado de métricas en tiempo real agregadas por el motor analítico.
     */
    @GetMapping("/analytics")
    public ResponseEntity<Map<String, Object>> getAnalytics() {
        return ResponseEntity.ok(analyticsProcessingService.getAnalyticsSummary());
    }
}
