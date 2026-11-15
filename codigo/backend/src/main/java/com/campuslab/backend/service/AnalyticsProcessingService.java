package com.campuslab.backend.service;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Servicio de Dominio para el procesamiento analítico y cálculo de métricas en tiempo real.
 * Mantiene la lógica de negocio desacoplada de la infraestructura de mensajería (Kafka/RabbitMQ).
 */
@Service
public class AnalyticsProcessingService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsProcessingService.class);

    private final AtomicLong totalBookingsProcessed = new AtomicLong(0);
    private final AtomicLong totalTelemetryPointsProcessed = new AtomicLong(0);
    private final AtomicLong totalDltAlerts = new AtomicLong(0);

    private final Map<String, Integer> activeWorkstationsByLab = new ConcurrentHashMap<>();
    private final Map<String, Double> currentTemperatureByLab = new ConcurrentHashMap<>();
    private final Map<String, Double> currentPowerByLab = new ConcurrentHashMap<>();
    private final List<Map<String, Object>> recentAlerts = Collections.synchronizedList(new LinkedList<>());
    private final List<BookingStreamEventDto> recentBookingEvents = Collections.synchronizedList(new LinkedList<>());

    /**
     * Procesa un evento de reserva recibido desde el tópico Kafka.
     */
    public void processBookingEvent(BookingStreamEventDto event, int partition, long offset) {
        totalBookingsProcessed.incrementAndGet();
        log.info("Analytics Engine: Analizando evento de reserva [{}] acción [{}] para lab [{}] (Partición: {}, Offset: {})",
                event.getEventId(), event.getAction(), event.getLabId(), partition, offset);

        // Actualizar histórico reciente (máximo 20 eventos en memoria)
        if (recentBookingEvents.size() >= 20) {
            recentBookingEvents.remove(0);
        }
        recentBookingEvents.add(event);

        if ("RESERVATION_CREATED".equalsIgnoreCase(event.getAction()) || "CHECK_IN".equalsIgnoreCase(event.getAction())) {
            activeWorkstationsByLab.merge(event.getLabId(), event.getWorkstationCount() != null ? event.getWorkstationCount() : 1, Integer::sum);
        } else if ("RESERVATION_CANCELLED".equalsIgnoreCase(event.getAction()) || "SESSION_COMPLETED".equalsIgnoreCase(event.getAction())) {
            activeWorkstationsByLab.computeIfPresent(event.getLabId(), (k, v) -> Math.max(0, v - (event.getWorkstationCount() != null ? event.getWorkstationCount() : 1)));
        }
    }

    /**
     * Procesa métricas de telemetría de laboratorio.
     */
    public void processTelemetryMetric(LabTelemetryDto metric, int partition, long offset) {
        totalTelemetryPointsProcessed.incrementAndGet();
        log.info("Analytics Engine: Procesando telemetría [{}] sensor [{}] temp [{}°C] potencia [{}W] (Partición: {}, Offset: {})",
                metric.getMetricId(), metric.getSensorId(), metric.getTemperatureCelsius(), metric.getPowerConsumptionWatts(), partition, offset);

        if (metric.getLabCode() != null) {
            currentTemperatureByLab.put(metric.getLabCode(), metric.getTemperatureCelsius());
            currentPowerByLab.put(metric.getLabCode(), metric.getPowerConsumptionWatts());
            if (metric.getActiveWorkstations() != null) {
                activeWorkstationsByLab.put(metric.getLabCode(), metric.getActiveWorkstations());
            }

            // Detección de anomalías en streaming (Alerta por sobretemperatura)
            if (metric.getTemperatureCelsius() != null && metric.getTemperatureCelsius() > 28.0) {
                Map<String, Object> alert = new HashMap<>();
                alert.put("id", UUID.randomUUID().toString());
                alert.put("type", "CRITICAL_TEMPERATURE_ALERT");
                alert.put("labCode", metric.getLabCode());
                alert.put("temperature", metric.getTemperatureCelsius());
                alert.put("timestamp", Instant.now().toString());
                alert.put("details", "Alerta automática generada por motor de streaming Kafka al superar umbral térmico (28°C)");

                if (recentAlerts.size() >= 10) {
                    recentAlerts.remove(0);
                }
                recentAlerts.add(alert);
                log.warn("Analytics Engine [ALERTA DE STREAMING]: Sobretemperatura detectada en {} : {} °C",
                        metric.getLabCode(), metric.getTemperatureCelsius());
            }
        }
    }

    /**
     * Registra un mensaje capturado en el Dead Letter Topic (DLT).
     */
    public void recordDltFailure(String topic, int partition, long offset, String payload) {
        totalDltAlerts.incrementAndGet();
        Map<String, Object> dltAlert = new HashMap<>();
        dltAlert.put("id", UUID.randomUUID().toString());
        dltAlert.put("type", "DEAD_LETTER_TOPIC_RECOVERY");
        dltAlert.put("originalTopic", topic);
        dltAlert.put("partition", partition);
        dltAlert.put("offset", offset);
        dltAlert.put("payload", payload);
        dltAlert.put("timestamp", Instant.now().toString());

        if (recentAlerts.size() >= 10) {
            recentAlerts.remove(0);
        }
        recentAlerts.add(dltAlert);
        log.error("Analytics Engine [DLT CAPTURADO]: Evento irrecuperable en partición {} offset {} - Guardado en DLT", partition, offset);
    }

    /**
     * Retorna el resumen consolidado de analítica en tiempo real para la API y Dashboard.
     */
    public Map<String, Object> getAnalyticsSummary() {
        Map<String, Object> summary = new HashMap<>();
        summary.put("totalBookingsProcessed", totalBookingsProcessed.get());
        summary.put("totalTelemetryPointsProcessed", totalTelemetryPointsProcessed.get());
        summary.put("totalDltAlerts", totalDltAlerts.get());
        summary.put("activeWorkstationsByLab", activeWorkstationsByLab);
        summary.put("currentTemperatureByLab", currentTemperatureByLab);
        summary.put("currentPowerByLab", currentPowerByLab);
        summary.put("recentAlerts", recentAlerts);
        summary.put("recentBookingEvents", recentBookingEvents);
        summary.put("timestamp", Instant.now().toString());
        return summary;
    }
}
