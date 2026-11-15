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
 * =========================================================================================
 * DECLARACIÓN PROGRAMÁTICA Y TOPOLOGÍA DE TÓPICOS KAFKA (KAFKA TOPIC CONFIG)
 * =========================================================================================
 * 
 * Declara e inicializa automáticamente los tópicos especializados del sistema CampusLab
 * durante el arranque de Spring Boot mediante Spring KafkaAdmin.
 * 
 * Cumple con los requerimientos de la Pauta Oficial EP5:
 * 1. Tópico 1 (Reservas): `campuslab.bookings.stream` con 3 particiones y retención de 7 días.
 * 2. Tópico 2 (Telemetría IoT): `campuslab.telemetry.metrics` con 3 particiones y retención de 24 horas.
 * 3. Tópico DLT (Dead Letter Topic): `campuslab.bookings.stream.DLT` con 3 particiones y retención de 14 días.
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

    /**
     * Administrador de infraestructura Spring Kafka que aplica las definiciones de NewTopic
     * contra el clúster al inicializar el contexto de la aplicación.
     */
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    /**
     * TÓPICO 1: Streaming de eventos del ciclo de vida de reservas de laboratorios.
     * 
     * Especificaciones de Diseño:
     * - Particiones: 3 (Permite paralelismo y balanceo horizontal por labId).
     * - Replicación: 1 (Configurado para broker individual en Docker / adaptable a clúster multi-broker).
     * - Retención (retention.ms): 7 días (604,800,000 ms) para posibilitar relectura histórica de eventos.
     * - Política de Limpieza (cleanup.policy): delete (Elimina segmentos tras expirar la ventana de retención).
     */
    @Bean
    public NewTopic bookingsStreamTopic() {
        return TopicBuilder.name(bookingsStreamTopicName)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000") // 7 días en milisegundos
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    /**
     * TÓPICO 2: Métricas continuas de sensores IoT de laboratorios (temperatura, potencia, ocupación).
     * 
     * Especificaciones de Diseño:
     * - Particiones: 3 (Particionado por código de laboratorio labCode).
     * - Replicación: 1.
     * - Retención (retention.ms): 24 horas (86,400,000 ms), optimizado para datos volátiles de alta frecuencia.
     * - Política de Limpieza (cleanup.policy): delete.
     */
    @Bean
    public NewTopic telemetryMetricsTopic() {
        return TopicBuilder.name(telemetryMetricsTopicName)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "86400000") // 24 horas en milisegundos
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    /**
     * TÓPICO 3: Dead Letter Topic (DLT) para auditoría de eventos de reservas no recuperables.
     * 
     * Especificaciones de Diseño:
     * - Particiones: 3.
     * - Replicación: 1.
     * - Retención (retention.ms): 14 días (1,209,600,000 ms) para garantizar amplio margen de análisis forense.
     * - Política de Limpieza (cleanup.policy): delete.
     */
    @Bean
    public NewTopic bookingsDltTopic() {
        return TopicBuilder.name(bookingsDltTopicName)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "1209600000") // 14 días en milisegundos
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }
}
