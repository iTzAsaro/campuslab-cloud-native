package com.campuslab.backend.listener;

import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import com.campuslab.backend.service.AnalyticsProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * =========================================================================================
 * CONSUMIDOR KAFKA 2: TELEMETRÍA IOT Y SENSORES DE LABORATORIO (STREAMING)
 * =========================================================================================
 * 
 * Tópico Suscrito: `campuslab.telemetry.metrics`
 * Consumer Group: `campuslab-telemetry-group`
 * 
 * Principios y Arquitectura:
 * 1. Consumer Group Especializado: Opera bajo `campuslab-telemetry-group` para procesar
 *    ráfagas de alta frecuencia de sensores sin degradar el consumo de reservas.
 * 2. Balanceo de Particiones: Distribuye las lecturas de los distintos sensores en las
 *    3 particiones disponibles.
 * 3. Procesamiento en Tiempo Real: Delega el cálculo de temperatura promedio, potencia
 *    eléctrica en Watts y detección de sobrecalentamiento (> 28°C) al servicio de analítica.
 */
@Component
public class LabTelemetryListener {

    private static final Logger log = LoggerFactory.getLogger(LabTelemetryListener.class);

    private final AnalyticsProcessingService analyticsProcessingService;

    public LabTelemetryListener(AnalyticsProcessingService analyticsProcessingService) {
        this.analyticsProcessingService = analyticsProcessingService;
    }

    /**
     * Procesa lecturas continuas emitidas por sensores de laboratorios universitarios.
     * 
     * @param metric DTO con temperatura, consumo en watts y workstations detectadas.
     * @param partition Partición física desde donde se extrajo el registro.
     * @param offset Offset secuencial del mensaje.
     */
    @KafkaListener(
            topics = "${campuslab.kafka.topics.telemetry-metrics:campuslab.telemetry.metrics}",
            groupId = "${campuslab.kafka.consumer-groups.telemetry:campuslab-telemetry-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onTelemetryMetric(@Payload LabTelemetryDto metric,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                  @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Kafka Listener [TELEMETRY CONSUMER] | Group: campuslab-telemetry-group | Tópico: telemetry.metrics | Partición: {} | Offset: {} | Métrica: {} de sensor: {}",
                partition, offset, metric.getMetricId(), metric.getSensorId());

        analyticsProcessingService.processTelemetryMetric(metric, partition, offset);
    }
}
