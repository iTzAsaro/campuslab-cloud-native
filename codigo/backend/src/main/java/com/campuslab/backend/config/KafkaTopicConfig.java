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

// Declaración y creación automática de tópicos de Kafka al iniciar la aplicación.
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

    // Administrador para registrar tópicos automáticamente en el clúster
    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return new KafkaAdmin(configs);
    }

    // Tópico para eventos de reservas (3 particiones, retención 7 días)
    @Bean
    public NewTopic bookingsStreamTopic() {
        return TopicBuilder.name(bookingsStreamTopicName)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "604800000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    // Tópico para telemetría de sensores IoT (3 particiones, retención 24 horas)
    @Bean
    public NewTopic telemetryMetricsTopic() {
        return TopicBuilder.name(telemetryMetricsTopicName)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "86400000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1")
                .build();
    }

    // Tópico Dead Letter Topic (DLT) para mensajes con error (3 particiones, retención 14 días)
    @Bean
    public NewTopic bookingsDltTopic() {
        return TopicBuilder.name(bookingsDltTopicName)
                .partitions(3)
                .replicas(1)
                .config(TopicConfig.RETENTION_MS_CONFIG, "1209600000")
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE)
                .build();
    }
}
