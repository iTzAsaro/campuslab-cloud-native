package com.campuslab.backend.listener;

import com.campuslab.backend.service.AnalyticsProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

// Consumidor para mensajes derivados a la Dead Letter Topic (DLT) tras agotar reintentos.
@Component
public class KafkaDltListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaDltListener.class);

    private final AnalyticsProcessingService analyticsProcessingService;

    public KafkaDltListener(AnalyticsProcessingService analyticsProcessingService) {
        this.analyticsProcessingService = analyticsProcessingService;
    }

    // Registra en el log y en el motor de analítica el mensaje que falló definitivamente
    @KafkaListener(
            topics = "${campuslab.kafka.topics.bookings-dlt:campuslab.bookings.stream.DLT}",
            groupId = "${campuslab.kafka.consumer-groups.dlt-audit:campuslab-dlt-audit-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onDltMessage(@Payload Object payload,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                             @Header(KafkaHeaders.OFFSET) long offset,
                             @Header(value = KafkaHeaders.RECEIVED_TOPIC, defaultValue = "campuslab.bookings.stream.DLT") String topic) {
        log.warn("Kafka DLT Listener: Mensaje en DLT! Tópico: [{}] Partición: [{}] Offset: [{}]",
                topic, partition, offset);

        analyticsProcessingService.recordDltFailure(topic, partition, offset, String.valueOf(payload));
    }
}
