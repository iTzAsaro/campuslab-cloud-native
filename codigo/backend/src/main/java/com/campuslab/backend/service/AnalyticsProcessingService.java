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

// Servicio de analítica en memoria: procesa eventos de reservas, telemetría y alertas DLT en tiempo real.
@Service
public class AnalyticsProcessingService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsProcessingService.class);

    // Contadores para métricas globales
    private final AtomicLong totalBookingsProcessed = new AtomicLong(0);
    private final AtomicLong totalTelemetryPointsProcessed = new AtomicLong(0);
    private final AtomicLong totalDltAlerts = new AtomicLong(0);

    // Estado en memoria de los laboratorios (ocupación, temperatura y potencia)
    private final Map<String, Integer> activeWorkstationsByLab = new ConcurrentHashMap<>();
    private final Map<String, Double> currentTemperatureByLab = new ConcurrentHashMap<>();
    private final Map<String, Double> currentPowerByLab = new ConcurrentHashMap<>();

    // Historial de alertas y eventos recientes (máximo 20)
    private final List<Map<String, Object>> recentAlerts = Collections.synchronizedList(new LinkedList<>());
    private final List<BookingStreamEventDto> recentBookingEvents = Collections.synchronizedList(new LinkedList<>());

    // Procesa eventos de reserva y actualiza la ocupación de los laboratorios
    public void processBookingEvent(BookingStreamEventDto event, int partition, long offset) {
        totalBookingsProcessed.incrementAndGet();
        log.info("Analytics: Evento de reserva [{}] acción [{}] lab [{}] (p: {}, o: {})",
                event.getEventId(), event.getAction(), event.getLabId(), partition, offset);

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

    // Procesa métricas IoT y genera alerta si la temperatura supera 28°C
    public void processTelemetryMetric(LabTelemetryDto metric, int partition, long offset) {
        totalTelemetryPointsProcessed.incrementAndGet();
        log.info("Analytics: Telemetría sensor [{}] temp [{}°C] potencia [{}W]",
                metric.getSensorId(), metric.getTemperatureCelsius(), metric.getPowerConsumptionWatts());

        if (metric.getLabCode() != null) {
            currentTemperatureByLab.put(metric.getLabCode(), metric.getTemperatureCelsius());
            currentPowerByLab.put(metric.getLabCode(), metric.getPowerConsumptionWatts());
            if (metric.getActiveWorkstations() != null) {
                activeWorkstationsByLab.put(metric.getLabCode(), metric.getActiveWorkstations());
            }

            // Alerta si la temperatura supera los 28°C
            if (metric.getTemperatureCelsius() != null && metric.getTemperatureCelsius() > 28.0) {
                Map<String, Object> alert = new HashMap<>();
                alert.put("id", UUID.randomUUID().toString());
                alert.put("type", "CRITICAL_TEMPERATURE_ALERT");
                alert.put("labId", metric.getLabCode());
                alert.put("sensorId", metric.getSensorId());
                alert.put("value", metric.getTemperatureCelsius() + "°C");
                alert.put("threshold", "28.0°C");
                alert.put("timestamp", Instant.now().toString());

                if (recentAlerts.size() >= 20) {
                    recentAlerts.remove(0);
                }
                recentAlerts.add(alert);
                log.warn("Analytics: Sobrecalentamiento en [{}]! Temp: {}°C", metric.getLabCode(), metric.getTemperatureCelsius());
            }
        }
    }

    // Registra un mensaje derivado a la Dead Letter Topic (DLT)
    public void recordDltFailure(String topic, int partition, long offset, String payload) {
        totalDltAlerts.incrementAndGet();
        Map<String, Object> dltRecord = new HashMap<>();
        dltRecord.put("id", UUID.randomUUID().toString());
        dltRecord.put("type", "DEAD_LETTER_TOPIC_RECOVERY");
        dltRecord.put("originalTopic", topic);
        dltRecord.put("partition", partition);
        dltRecord.put("offset", offset);
        dltRecord.put("payload", payload);
        dltRecord.put("timestamp", Instant.now().toString());

        if (recentAlerts.size() >= 20) {
            recentAlerts.remove(0);
        }
        recentAlerts.add(dltRecord);
        log.warn("Analytics: Mensaje derivado a DLT de [{}] partición [{}] offset [{}]", topic, partition, offset);
    }

    // Retorna el resumen de métricas para el dashboard y la API
    public Map<String, Object> getAnalyticsSnapshot() {
        return getAnalyticsSummary();
    }

    // Retorna el resumen actual de métricas
    public Map<String, Object> getAnalyticsSummary() {
        Map<String, Object> snapshot = new HashMap<>();
        snapshot.put("totalBookingsProcessed", totalBookingsProcessed.get());
        snapshot.put("totalTelemetryPointsProcessed", totalTelemetryPointsProcessed.get());
        snapshot.put("totalDltAlerts", totalDltAlerts.get());
        snapshot.put("activeWorkstationsByLab", new HashMap<>(activeWorkstationsByLab));
        snapshot.put("currentTemperatureByLab", new HashMap<>(currentTemperatureByLab));
        snapshot.put("currentPowerByLab", new HashMap<>(currentPowerByLab));
        snapshot.put("recentBookingEvents", new ArrayList<>(recentBookingEvents));
        snapshot.put("recentAlerts", new ArrayList<>(recentAlerts));
        snapshot.put("timestamp", Instant.now().toString());
        return snapshot;
    }
}
