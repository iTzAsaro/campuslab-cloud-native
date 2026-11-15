package com.campuslab.backend.dto.kafka;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * DTO para la creación dinámica de tópicos en el microservicio administrador de Kafka.
 */
public class TopicCreateRequestDto implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotBlank(message = "El nombre del tópico es obligatorio")
    private String name;

    @Positive(message = "El número de particiones debe ser mayor a 0")
    private int partitions = 3;

    @Positive(message = "El factor de replicación debe ser mayor a 0")
    private short replicationFactor = 1;

    private Long retentionMs = 604800000L; // 7 días default

    private String cleanupPolicy = "delete"; // "delete" o "compact"

    private Map<String, String> additionalConfigs = new HashMap<>();

    public TopicCreateRequestDto() {
    }

    public TopicCreateRequestDto(String name, int partitions, short replicationFactor, Long retentionMs, String cleanupPolicy) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.retentionMs = retentionMs;
        this.cleanupPolicy = cleanupPolicy;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public short getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(short replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public Long getRetentionMs() {
        return retentionMs;
    }

    public void setRetentionMs(Long retentionMs) {
        this.retentionMs = retentionMs;
    }

    public String getCleanupPolicy() {
        return cleanupPolicy;
    }

    public void setCleanupPolicy(String cleanupPolicy) {
        this.cleanupPolicy = cleanupPolicy;
    }

    public Map<String, String> getAdditionalConfigs() {
        return additionalConfigs;
    }

    public void setAdditionalConfigs(Map<String, String> additionalConfigs) {
        this.additionalConfigs = additionalConfigs;
    }
}
