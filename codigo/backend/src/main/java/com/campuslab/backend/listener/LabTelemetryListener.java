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

// Consumidor para métricas IoT de sensores desde el tópico campuslab.telemetry.metrics.
@Component
public class LabTelemetryListener {

    private static final Logger log = LoggerFactory.getLogger(LabTelemetryListener.class);

    private final AnalyticsProcessingService analyticsProcessingService;

    public LabTelemetryListener(AnalyticsProcessingService analyticsProcessingService) {
        this.analyticsProcessingService = analyticsProcessingService;
    }

    // Escucha métricas de sensores (temperatura, potencia y ocupación) y las envía al motor de analítica
    @KafkaListener(
            topics = "${campuslab.kafka.topics.telemetry-metrics:campuslab.telemetry.metrics}",
            groupId = "${campuslab.kafka.consumer-groups.telemetry:campuslab-telemetry-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onTelemetryMetric(@Payload LabTelemetryDto metric,
                                  @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                  @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Kafka Listener: Recibida telemetría de sensor [{}] en partición [{}] offset [{}]",
                metric.getSensorId(), partition, offset);

        analyticsProcessingService.processTelemetryMetric(metric, partition, offset);
    }
}
