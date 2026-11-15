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
 * =========================================================================================
 * MOTOR ANALÍTICO Y CAPA DE DOMINIO EN TIEMPO REAL (ANALYTICS PROCESSING SERVICE)
 * =========================================================================================
 * 
 * Implementa la lógica de procesamiento analítico en memoria sobre los flujos de streaming.
 * Mantiene la lógica de negocio desacoplada de la infraestructura física de Kafka y RabbitMQ.
 * 
 * Responsabilidades:
 * 1. Agregación de Métricas: Calcula en tiempo real reservas procesadas, telemetría y alertas DLT.
 * 2. Cálculo de Ocupación por Laboratorio: Suma puestos de trabajo activos en CHECK_IN / CREATED
 *    y los libera en SESSION_COMPLETED / CANCELLED.
 * 3. Detección Temprana de Anomalías: Dispara alertas térmicas si un sensor supera 28.0°C.
 * 4. Auditoría Forense DLT: Almacena los últimos mensajes derivados a la Dead Letter Topic.
 */
@Service
public class AnalyticsProcessingService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsProcessingService.class);

    // Contadores atómicos thread-safe para analítica de alta concurrencia
    private final AtomicLong totalBookingsProcessed = new AtomicLong(0);
    private final AtomicLong totalTelemetryPointsProcessed = new AtomicLong(0);
    private final AtomicLong totalDltAlerts = new AtomicLong(0);

    // Mapas concurrentes para estado de laboratorios en vivo
    private final Map<String, Integer> activeWorkstationsByLab = new ConcurrentHashMap<>();
    private final Map<String, Double> currentTemperatureByLab = new ConcurrentHashMap<>();
    private final Map<String, Double> currentPowerByLab = new ConcurrentHashMap<>();
    
    // Listas sincronizadas para feed en vivo de alertas y eventos recientes (máx 20 elementos)
    private final List<Map<String, Object>> recentAlerts = Collections.synchronizedList(new LinkedList<>());
    private final List<BookingStreamEventDto> recentBookingEvents = Collections.synchronizedList(new LinkedList<>());

    /**
     * Procesa un evento de ciclo de vida de reserva proveniente de Kafka.
     * Actualiza el contador global, la lista de eventos recientes y el balance de puestos ocupados.
     */
    public void processBookingEvent(BookingStreamEventDto event, int partition, long offset) {
        totalBookingsProcessed.incrementAndGet();
        log.info("Analytics Engine: Analizando evento de reserva [{}] acción [{}] para lab [{}] (Partición: {}, Offset: {})",
                event.getEventId(), event.getAction(), event.getLabId(), partition, offset);

        // Mantener histórico reciente en ventana deslizante (máximo 20 eventos)
        if (recentBookingEvents.size() >= 20) {
            recentBookingEvents.remove(0);
        }
        recentBookingEvents.add(event);

        // Lógica de cálculo de ocupación en tiempo real
        if ("RESERVATION_CREATED".equalsIgnoreCase(event.getAction()) || "CHECK_IN".equalsIgnoreCase(event.getAction())) {
            activeWorkstationsByLab.merge(event.getLabId(), event.getWorkstationCount() != null ? event.getWorkstationCount() : 1, Integer::sum);
        } else if ("RESERVATION_CANCELLED".equalsIgnoreCase(event.getAction()) || "SESSION_COMPLETED".equalsIgnoreCase(event.getAction())) {
            activeWorkstationsByLab.computeIfPresent(event.getLabId(), (k, v) -> Math.max(0, v - (event.getWorkstationCount() != null ? event.getWorkstationCount() : 1)));
        }
    }

    /**
     * Procesa métricas de sensores IoT continuas.
     * Almacena temperatura actual, potencia en Watts y verifica límites térmicos.
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

            // Detección de anomalías en streaming (Alerta por sobretemperatura > 28°C)
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
                log.warn("Analytics Engine [ALERTA TEMPERATURA]: ¡Sobrecalentamiento en lab [{}]! Temp: {}°C",
                        metric.getLabCode(), metric.getTemperatureCelsius());
            }
        }
    }

    /**
     * Registra un mensaje fallido derivado a la Dead Letter Topic para auditoría forense.
     */
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
        log.warn("Analytics Engine [ALERTA DLT]: Registrado fallo en DLT para tópico [{}] partición [{}] offset [{}]",
                topic, partition, offset);
    }

    /**
     * Entrega un snapshot consolidado de todas las métricas en tiempo real
     * consumido por el frontend Angular cada 5 segundos.
     */
    public Map<String, Object> getAnalyticsSnapshot() {
        return getAnalyticsSummary();
    }

    /**
     * Resumen de analítica consumido por los controladores y pruebas automatizadas.
     */
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
