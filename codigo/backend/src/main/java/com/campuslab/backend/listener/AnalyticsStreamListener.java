package com.campuslab.backend.listener;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
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
 * CONSUMIDOR KAFKA 1: ANALÍTICA Y CICLO DE VIDA DE RESERVAS (STREAMING)
 * =========================================================================================
 * 
 * Tópico Suscrito: `campuslab.bookings.stream`
 * Consumer Group: `campuslab-analytics-group`
 * 
 * Principios y Arquitectura:
 * 1. Aislamiento de Dominio: Pertenece a un grupo exclusivo (`campuslab-analytics-group`), lo
 *    que garantiza que la velocidad de consumo de este flujo analítico sea totalmente
 *    independiente del procesamiento de telemetría IoT o auditoría DLT.
 * 2. Concurrencia Equilibrada: El contenedor asigna un hilo de escucha a cada partición.
 * 3. Inyección de Fallo Controlado: Si detecta un correo con `trigger-dlt-error`, lanza una
 *    excepción deliberada para demostrar a la comisión los reintentos automáticos
 *    `FixedBackOff` y la posterior derivación al tópico `.DLT`.
 * 4. Desacoplamiento: Delega el cálculo de analítica a `AnalyticsProcessingService`.
 */
@Component
public class AnalyticsStreamListener {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsStreamListener.class);

    private final AnalyticsProcessingService analyticsProcessingService;

    public AnalyticsStreamListener(AnalyticsProcessingService analyticsProcessingService) {
        this.analyticsProcessingService = analyticsProcessingService;
    }

    /**
     * Punto de entrada para los eventos de reserva provenientes del tópico de streaming.
     * 
     * @param event DTO deserializado de forma segura con los datos del evento.
     * @param partition Partición física (0, 1 o 2) asignada al consumidor.
     * @param offset Número secuencial único del mensaje dentro de la partición.
     */
    @KafkaListener(
            topics = "${campuslab.kafka.topics.bookings-stream:campuslab.bookings.stream}",
            groupId = "${campuslab.kafka.consumer-groups.analytics:campuslab-analytics-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onBookingStreamEvent(@Payload BookingStreamEventDto event,
                                     @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                     @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("Kafka Listener [ANALYTICS CONSUMER] | Group: campuslab-analytics-group | Tópico: bookings.stream | Partición: {} | Offset: {} | Evento: {}",
                partition, offset, event.getEventId());

        // Manejo y validación de payload - Simulación deliberada de error irrecuperable para prueba de DLT
        if (event.getStudentEmail() != null && event.getStudentEmail().contains("trigger-dlt-error")) {
            log.warn("Kafka Listener [ERROR SIMULADO]: Detectado payload con 'trigger-dlt-error'. Lanzando excepción para activar DefaultErrorHandler y DLT...");
            throw new IllegalArgumentException("Fallo deliberado en procesamiento de reserva: " + event.getEventId());
        }

        // Delegar procesamiento a la capa de dominio en memoria
        analyticsProcessingService.processBookingEvent(event, partition, offset);
    }
}
