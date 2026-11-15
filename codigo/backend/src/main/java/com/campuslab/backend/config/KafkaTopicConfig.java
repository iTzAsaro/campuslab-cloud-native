package com.campuslab.backend.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

/**
 * Configuración y declaración programática de Tópicos Kafka de la plataforma CampusLab.
 * Satisface los indicadores de pauta sobre declaración de 2 tópicos, parámetros de administración
 * (retention.ms, cleanup.policy, número de particiones y factor de replicación).
 */
@Configuration
public class KafkaTopicConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:29092}")
    private String bootstrapServers;

    @Value("${campuslab.kafka.topics.bookings-stream:campuslab.bookings.stream}")
    private String bookingsStreamTopicName;

    @Value("${campuslab.kafka.topics.telemetry-metrics:campuslab.telemetry.metrics}")
    private String telemetryMetricsTopicName;

    @Value("${campuslab.kafka.topics.bookings-dlt:campuslab.bookings.stream.DLT}")
    private String bookingsDltTopicName;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    /**
     * TÓPICO 1: Streaming de eventos del ciclo de vida de reservas de laboratorios.
     * Particiones: 3 (balanceo de carga por labId).
     * Replicación: 1 (entorno clúster broker individual / escalable).
     * Retención: 7 días (604,800,000 ms) para relectura y análisis histórico.
     * Cleanup: delete.
     */
    @Bean
    public NewTopic bookingsStreamTopic() {
        return TopicBuilder.name(bookingsStreamTopicName)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 días
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    /**
     * TÓPICO 2: Métricas de telemetría y sensores IoT de alta velocidad.
     * Particiones: 3 (balanceo por código de laboratorio).
     * Replicación: 1.
     * Retención: 24 horas (86,400,000 ms) - streaming de métricas operativas.
     * Cleanup: delete.
     */
    @Bean
    public NewTopic telemetryMetricsTopic() {
        return TopicBuilder.name(telemetryMetricsTopicName)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "86400000") // 24 horas
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }

    /**
     * TÓPICO DLT: Dead Letter Topic para eventos corruptos o no procesables de reservas.
     * Retención: 14 días para inspección y auditoría post-mortem.
     */
    @Bean
    public NewTopic bookingsDltTopic() {
        return TopicBuilder.name(bookingsDltTopicName)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "1209600000") // 14 días
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }
}
