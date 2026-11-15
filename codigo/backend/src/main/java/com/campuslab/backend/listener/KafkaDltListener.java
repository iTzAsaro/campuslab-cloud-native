package com.campuslab.backend.listener;

import com.campuslab.backend.service.AnalyticsProcessingService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumidor Kafka para el Dead Letter Topic (DLT) (campuslab.bookings.stream.DLT).
 * Consumer Group: campuslab-dlt-audit-group.
 * Captura mensajes venenosos que no pudieron ser procesados tras los reintentos para auditoría forense.
 */
@Component
public class KafkaDltListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaDltListener.class);

    private final AnalyticsProcessingService analyticsProcessingService;

    public KafkaDltListener(AnalyticsProcessingService analyticsProcessingService) {
        this.analyticsProcessingService = analyticsProcessingService;
    }

    @KafkaListener(
            topics = "${campuslab.kafka.topics.bookings-dlt:campuslab.bookings.stream.DLT}",
            groupId = "${campuslab.kafka.consumer-groups.dlt-audit:campuslab-dlt-audit-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onDltMessage(@Payload Object payload,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                             @Header(KafkaHeaders.OFFSET) long offset,
                             @Header(value = KafkaHeaders.RECEIVED_TOPIC, defaultValue = "campuslab.bookings.stream.DLT") String topic) {
        log.warn("Kafka Listener [DLT AUDIT CONSUMER]: Mensaje recibido en Dead Letter Topic! Tópico: {} | Partición: {} | Offset: {} | Payload: {}",
                topic, partition, offset, payload);

        analyticsProcessingService.recordDltFailure(topic, partition, offset, String.valueOf(payload));
    }
}
