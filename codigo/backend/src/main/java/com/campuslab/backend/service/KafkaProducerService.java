package com.campuslab.backend.service;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * =========================================================================================
 * SERVICIO PRODUCTOR DE EVENTOS APACHE KAFKA (KAFKA PRODUCER SERVICE)
 * =========================================================================================
 * 
 * Implementa la publicación estructurada de eventos y métricas hacia el clúster Kafka.
 * 
 * Principios de Diseño Implementados:
 * 1. Uso de Partition Keys: Garantiza orden cronológico estricto por laboratorio (mensajes
 *    con la misma clave siempre viajan a la misma partición).
 * 2. Procesamiento Asíncrono no Bloqueante: Retorna `CompletableFuture<SendResult>` para
 *    máximo rendimiento y tolerancia a latencia de red.
 * 3. Callbacks de Notificación: Registra en logs el tópico, partición y offset asignado
 *    tras la confirmación del broker, o la traza de error en caso de fallo.
 */
@Service
public class KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducerService.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${campuslab.kafka.topics.bookings-stream:campuslab.bookings.stream}")
    private String bookingsStreamTopic;

    @Value("${campuslab.kafka.topics.telemetry-metrics:campuslab.telemetry.metrics}")
    private String telemetryMetricsTopic;

    public KafkaProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publica un evento de ciclo de vida de reserva en `campuslab.bookings.stream`.
     * 
     * @param event DTO estructurado con identificador de evento, acción y datos de la reserva.
     * @return CompletableFuture con los metadatos de envío (partición, offset y timestamp).
     * 
     * Clave de Particionado (Partition Key):
     * Se utiliza `event.getLabId()` (ej: "LAB-CLOUD-01"). Kafka calcula `murmur2(key) % 3`
     * para asegurar que todos los eventos de un mismo laboratorio lleguen a la misma partición
     * y sean consumidos en estricto orden secuencial.
     */
    public CompletableFuture<SendResult<String, Object>> publishBookingStreamEvent(BookingStreamEventDto event) {
        String partitionKey = event.getLabId() != null ? event.getLabId() : "DEFAULT_LAB";
        log.info("Kafka Producer [BOOKINGS]: Publicando evento [{}] acción [{}] para lab [{}] en tópico [{}]",
                event.getEventId(), event.getAction(), partitionKey, bookingsStreamTopic);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(bookingsStreamTopic, partitionKey, event);

        // Callback asíncrono para confirmación de entrega
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Kafka Producer [BOOKINGS CONFIRMADO]: Evento [{}] almacenado en tópico [{}] partición [{}] con offset [{}]",
                        event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Kafka Producer [BOOKINGS FALLÓ]: No se pudo entregar evento [{}] a Kafka: {}",
                        event.getEventId(), ex.getMessage(), ex);
            }
        });

        return future;
    }

    /**
     * Publica una lectura de telemetría IoT de sensores en `campuslab.telemetry.metrics`.
     * 
     * @param metric DTO con temperatura, consumo en Watts y puestos activos detectados.
     * @return CompletableFuture con los metadatos del registro.
     * 
     * Clave de Particionado (Partition Key):
     * Se utiliza `metric.getLabCode()` (ej: "LAB-REDES-01") para balancear las métricas de
     * cada laboratorio en una partición asignada.
     */
    public CompletableFuture<SendResult<String, Object>> publishTelemetryMetric(LabTelemetryDto metric) {
        String partitionKey = metric.getLabCode() != null ? metric.getLabCode() : "DEFAULT_LAB";
        log.info("Kafka Producer [TELEMETRÍA]: Transmitiendo lectura [{}] sensor [{}] para lab [{}] en tópico [{}]",
                metric.getMetricId(), metric.getSensorId(), partitionKey, telemetryMetricsTopic);

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(telemetryMetricsTopic, partitionKey, metric);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Kafka Producer [TELEMETRÍA CONFIRMADA]: Métrica [{}] entregada a tópico [{}] partición [{}] con offset [{}]",
                        metric.getMetricId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Kafka Producer [TELEMETRÍA FALLÓ]: Error transmitiendo telemetría [{}] a Kafka: {}",
                        metric.getMetricId(), ex.getMessage(), ex);
            }
        });

        return future;
    }

    /**
     * Emite una ráfaga controlada de eventos de prueba para evidenciar el balanceo
     * entre las 3 particiones del clúster ante la comisión evaluadora.
     */
    public void publishBurstEvents(int count) {
        log.info("Kafka Producer [RÁFAGA]: Iniciando emisión de {} eventos concurrentes...", count);
        String[] labs = {"LAB-CLOUD-01", "LAB-CLOUD-02", "LAB-REDES-01"};
        String[] actions = {"RESERVATION_CREATED", "CHECK_IN", "SESSION_COMPLETED"};

        for (int i = 1; i <= count; i++) {
            String labId = labs[i % labs.length];
            String action = actions[i % actions.length];
            BookingStreamEventDto burstEvent = new BookingStreamEventDto(
                    "BURST-" + Long.toHexString(Double.doubleToLongBits(Math.random())).toUpperCase().substring(0, 8),
                    2000L + i,
                    labId,
                    "student." + i + "@campuslab.cl",
                    action,
                    60 + (i * 10),
                    (i % 3) + 1
            );
            publishBookingStreamEvent(burstEvent);
        }
    }
}
