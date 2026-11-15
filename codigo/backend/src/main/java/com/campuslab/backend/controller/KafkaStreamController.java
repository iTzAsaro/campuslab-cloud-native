package com.campuslab.backend.controller;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import com.campuslab.backend.service.AnalyticsProcessingService;
import com.campuslab.backend.service.KafkaProducerService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Controlador REST para operaciones de Streaming y Event-Driven Analytics con Kafka.
 * Permite disparar eventos de reservas, métricas de telemetría IoT, simulación masiva y pruebas de DLT.
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
     * Publica un evento de streaming de reserva en Kafka.
     */
    @PostMapping("/bookings")
    public ResponseEntity<Map<String, Object>> publishBookingEvent(@RequestBody(required = false) BookingStreamEventDto request) {
        BookingStreamEventDto event = request;
        if (event == null || event.getEventId() == null) {
            event = new BookingStreamEventDto(
                    "EVT-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                    1001L,
                    "LAB-CLOUD-01",
                    "estudiante@campuslab.cl",
                    "RESERVATION_CREATED",
                    120,
                    1
            );
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
     * Publica una métrica de telemetría IoT de laboratorio.
     */
    @PostMapping("/telemetry")
    public ResponseEntity<Map<String, Object>> publishTelemetry(@RequestBody(required = false) LabTelemetryDto request) {
        LabTelemetryDto metric = request;
        if (metric == null || metric.getMetricId() == null) {
            metric = new LabTelemetryDto(
                    "TEL-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                    "LAB-CLOUD-01",
                    "TEMP-POWER-SENSOR-01",
                    12,
                    23.5,
                    45.0,
                    1850.0
            );
        }

        kafkaProducerService.publishTelemetryMetric(metric);

        return ResponseEntity.ok(Map.of(
                "status", "QUEUED_TO_KAFKA",
                "topic", "campuslab.telemetry.metrics",
                "partitionKey", metric.getLabCode(),
                "metric", metric
        ));
    }

    /**
     * Simula una ráfaga de 10 eventos distribuidos entre 3 laboratorios para probar particionado y balanceo.
     */
    @PostMapping("/simulate-load")
    public ResponseEntity<Map<String, Object>> simulateLoad() {
        String[] labs = {"LAB-CLOUD-01", "LAB-CLOUD-02", "LAB-REDES-01"};
        String[] actions = {"RESERVATION_CREATED", "CHECK_IN", "SESSION_COMPLETED"};

        List<BookingStreamEventDto> published = new ArrayList<>();

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
     * Dispara un evento diseñado intencionalmente para fallar en el listener y ser derivado a Dead Letter Topic.
     */
    @PostMapping("/trigger-dlt-error")
    public ResponseEntity<Map<String, Object>> triggerDltError() {
        BookingStreamEventDto poisonPill = new BookingStreamEventDto(
                "POISON-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                9999L,
                "LAB-ERROR-TEST",
                "trigger-dlt-error@campuslab.cl", // Activa excepción en listener
                "CORRUPT_TRANSACTION",
                0,
                0
        );

        kafkaProducerService.publishBookingStreamEvent(poisonPill);

        return ResponseEntity.ok(Map.of(
                "status", "POISON_PILL_PUBLISHED",
                "expectedBehavior", "El listener fallará, ejecutará 2 reintentos via FixedBackOff y lo desviará a campuslab.bookings.stream.DLT",
                "event", poisonPill
        ));
    }

    /**
     * Consulta el estado y métricas de analítica agregadas en streaming por los consumidores Kafka.
     */
    @GetMapping("/analytics")
    public ResponseEntity<Map<String, Object>> getAnalytics() {
        return ResponseEntity.ok(analyticsProcessingService.getAnalyticsSummary());
    }
}
