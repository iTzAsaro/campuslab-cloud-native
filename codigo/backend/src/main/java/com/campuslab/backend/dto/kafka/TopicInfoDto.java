package com.campuslab.backend.dto.kafka;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * DTO con información detallada de un tópico para el microservicio administrador de Kafka.
 */
public class TopicInfoDto implements Serializable {

    private static final long serialVersionUID = 1L;

    private String name;
    private int partitions;
    private int replicationFactor;
    private boolean internal;
    private List<Integer> partitionIds;
    private Map<String, String> configs;

    public TopicInfoDto() {
    }

    public TopicInfoDto(String name, int partitions, int replicationFactor, boolean internal,
                        List<Integer> partitionIds, Map<String, String> configs) {
        this.name = name;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
        this.internal = internal;
        this.partitionIds = partitionIds;
        this.configs = configs;
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

    public int getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public boolean isInternal() {
        return internal;
    }

    public void setInternal(boolean internal) {
        this.internal = internal;
    }

    public List<Integer> getPartitionIds() {
        return partitionIds;
    }

    public void setPartitionIds(List<Integer> partitionIds) {
        this.partitionIds = partitionIds;
    }

    public Map<String, String> getConfigs() {
        return configs;
    }

    public void setConfigs(Map<String, String> configs) {
        this.configs = configs;
    }
}
