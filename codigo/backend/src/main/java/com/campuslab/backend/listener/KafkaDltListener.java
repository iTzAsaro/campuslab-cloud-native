package com.campuslab.backend.listener;

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
 * CONSUMIDOR KAFKA 3: AUDITORÍA DE DEAD LETTER TOPIC (DLT AUDIT CONSUMER)
 * =========================================================================================
 * 
 * Tópico Suscrito: `campuslab.bookings.stream.DLT`
 * Consumer Group: `campuslab-dlt-audit-group`
 * 
 * Principios y Arquitectura:
 * 1. Resiliencia Desacoplada: Captura de forma asíncrona todos aquellos mensajes que fueron
 *    descartados del flujo principal tras agotarse los 2 reintentos de `FixedBackOff`.
 * 2. Trazabilidad Forense: Registra en el motor analítico el payload rechazado, partición,
 *    offset y timestamp para permitir reenvío posterior o corrección de bugs en producción.
 * 3. Demostración en Vivo: Incrementa el contador de alertas DLT en el frontend para
 *    evidenciar visualmente la tolerancia a fallos ante la comisión.
 */
@Component
public class KafkaDltListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaDltListener.class);

    private final AnalyticsProcessingService analyticsProcessingService;

    public KafkaDltListener(AnalyticsProcessingService analyticsProcessingService) {
        this.analyticsProcessingService = analyticsProcessingService;
    }

    /**
     * Procesa los mensajes derivados a la Dead Letter Topic.
     * 
     * @param payload Objeto o representación en texto del mensaje defectuoso.
     * @param partition Partición física del DLT.
     * @param offset Offset en la DLT.
     * @param topic Nombre del tópico DLT recibido en las cabeceras de Kafka.
     */
    @KafkaListener(
            topics = "${campuslab.kafka.topics.bookings-dlt:campuslab.bookings.stream.DLT}",
            groupId = "${campuslab.kafka.consumer-groups.dlt-audit:campuslab-dlt-audit-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onDltMessage(@Payload Object payload,
                             @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                             @Header(KafkaHeaders.OFFSET) long offset,
                             @Header(value = KafkaHeaders.RECEIVED_TOPIC, defaultValue = "campuslab.bookings.stream.DLT") String topic) {
        log.warn("Kafka Listener [DLT AUDIT CONSUMER]: Mensaje derivado al Dead Letter Topic! Tópico: {} | Partición: {} | Offset: {} | Payload: {}",
                topic, partition, offset, payload);

        // Registrar el evento fallido en la memoria del motor analítico
        analyticsProcessingService.recordDltFailure(topic, partition, offset, String.valueOf(payload));
    }
}
