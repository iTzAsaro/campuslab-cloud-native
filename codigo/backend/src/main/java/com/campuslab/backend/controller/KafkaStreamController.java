package com.campuslab.backend.controller;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import com.campuslab.backend.service.AnalyticsProcessingService;
import com.campuslab.backend.service.KafkaProducerService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

// Controlador REST para emitir eventos de streaming a Kafka y consultar métricas en vivo.
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

    // Publica un evento de reserva en el tópico de streaming
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

    // Publica una métrica de telemetría IoT de sensores
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

    // Dispara una ráfaga de 10 eventos para probar distribución entre particiones
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

    // Inyecta un error para validar reintentos y desvío al Dead Letter Topic (DLT)
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

    // Retorna el resumen de métricas agregadas en tiempo real
    @GetMapping("/analytics")
    public ResponseEntity<Map<String, Object>> getAnalytics() {
        return ResponseEntity.ok(analyticsProcessingService.getAnalyticsSummary());
    }
}
