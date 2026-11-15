package com.campuslab.backend.service;

import com.campuslab.backend.dto.kafka.ConsumerGroupInfoDto;
import com.campuslab.backend.dto.kafka.TopicCreateRequestDto;
import com.campuslab.backend.dto.kafka.TopicInfoDto;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Microservicio Administrador de Apache Kafka.
 * Implementa operaciones programáticas para crear, listar, inspeccionar y eliminar tópicos
 * y consultar el estado de Consumer Groups usando AdminClient.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private final AdminClient adminClient;

    public KafkaAdminService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    /**
     * Crea un tópico dinámicamente con particiones, factor de replicación y políticas de configuración.
     */
    public TopicInfoDto createTopic(TopicCreateRequestDto request) {
        log.info("Kafka Admin: Creando tópico [{}] con {} particiones y replicación {}",
                request.getName(), request.getPartitions(), request.getReplicationFactor());

        NewTopic newTopic = new NewTopic(request.getName(), request.getPartitions(), request.getReplicationFactor());

        Map<String, String> configs = new HashMap<>();
        if (request.getRetentionMs() != null) {
            configs.put(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(request.getRetentionMs()));
        }
        if (request.getCleanupPolicy() != null) {
            configs.put(TopicConfig.CLEANUP_POLICY_CONFIG, request.getCleanupPolicy());
        }
        if (request.getAdditionalConfigs() != null) {
            configs.putAll(request.getAdditionalConfigs());
        }
        newTopic.configs(configs);

        try {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get(10, TimeUnit.SECONDS);
            log.info("Kafka Admin: Tópico [{}] creado exitosamente", request.getName());
            return describeTopic(request.getName());
        } catch (Exception e) {
            log.error("Kafka Admin Error al crear tópico [{}]: {}", request.getName(), e.getMessage());
            throw new RuntimeException("Error al crear tópico Kafka: " + e.getMessage(), e);
        }
    }

    /**
     * Lista todos los tópicos existentes con su cantidad de particiones y metadatos básicos.
     */
    public List<TopicInfoDto> listTopics() {
        log.info("Kafka Admin: Listando tópicos...");
        try {
            ListTopicsResult listTopicsResult = adminClient.listTopics(new ListTopicsOptions().listInternal(false));
            Map<String, TopicListing> topicListingMap = listTopicsResult.namesToListings().get(10, TimeUnit.SECONDS);

            if (topicListingMap.isEmpty()) {
                return Collections.emptyList();
            }

            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(topicListingMap.keySet());
            Map<String, TopicDescription> descriptions = describeTopicsResult.allTopicNames().get(10, TimeUnit.SECONDS);

            List<TopicInfoDto> result = new ArrayList<>();
            for (Map.Entry<String, TopicDescription> entry : descriptions.entrySet()) {
                TopicDescription desc = entry.getValue();
                List<Integer> partitionIds = desc.partitions().stream()
                        .map(TopicPartitionInfo::partition)
                        .collect(Collectors.toList());

                int replicationFactor = desc.partitions().isEmpty() ? 1 : desc.partitions().get(0).replicas().size();

                result.add(new TopicInfoDto(
                        desc.name(),
                        desc.partitions().size(),
                        replicationFactor,
                        desc.isInternal(),
                        partitionIds,
                        Collections.emptyMap()
                ));
            }

            result.sort(Comparator.comparing(TopicInfoDto::getName));
            return result;
        } catch (Exception e) {
            log.error("Kafka Admin Error al listar tópicos: {}", e.getMessage());
            throw new RuntimeException("Error al listar tópicos Kafka: " + e.getMessage(), e);
        }
    }

    /**
     * Describe en detalle un tópico específico, incluyendo sus particiones y configuración de retención/cleanup.
     */
    public TopicInfoDto describeTopic(String topicName) {
        log.info("Kafka Admin: Describiendo tópico [{}]", topicName);
        try {
            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
            TopicDescription desc = describeTopicsResult.allTopicNames().get(10, TimeUnit.SECONDS).get(topicName);

            if (desc == null) {
                throw new NoSuchElementException("El tópico no existe: " + topicName);
            }

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            DescribeConfigsResult configsResult = adminClient.describeConfigs(Collections.singletonList(resource));
            Config config = configsResult.all().get(10, TimeUnit.SECONDS).get(resource);

            Map<String, String> configMap = new HashMap<>();
            if (config != null) {
                config.entries().forEach(entry -> {
                    if (TopicConfig.RETENTION_MS_CONFIG.equals(entry.name()) ||
                        TopicConfig.CLEANUP_POLICY_CONFIG.equals(entry.name()) ||
                        TopicConfig.MAX_MESSAGE_BYTES_CONFIG.equals(entry.name())) {
                        configMap.put(entry.name(), entry.value());
                    }
                });
            }

            List<Integer> partitionIds = desc.partitions().stream()
                    .map(TopicPartitionInfo::partition)
                    .collect(Collectors.toList());

            int replicationFactor = desc.partitions().isEmpty() ? 1 : desc.partitions().get(0).replicas().size();

            return new TopicInfoDto(
                    desc.name(),
                    desc.partitions().size(),
                    replicationFactor,
                    desc.isInternal(),
                    partitionIds,
                    configMap
            );
        } catch (Exception e) {
            log.error("Kafka Admin Error al describir tópico [{}]: {}", topicName, e.getMessage());
            throw new RuntimeException("Error al obtener detalle del tópico Kafka: " + e.getMessage(), e);
        }
    }

    /**
     * Elimina un tópico de Kafka.
     */
    public void deleteTopic(String topicName) {
        log.info("Kafka Admin: Eliminando tópico [{}]", topicName);
        try {
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get(10, TimeUnit.SECONDS);
            log.info("Kafka Admin: Tópico [{}] eliminado con éxito", topicName);
        } catch (Exception e) {
            log.error("Kafka Admin Error al eliminar tópico [{}]: {}", topicName, e.getMessage());
            throw new RuntimeException("Error al eliminar tópico Kafka: " + e.getMessage(), e);
        }
    }

    /**
     * Lista los Consumer Groups registrados en el clúster.
     */
    public List<ConsumerGroupInfoDto> listConsumerGroups() {
        log.info("Kafka Admin: Consultando Consumer Groups...");
        try {
            Collection<ConsumerGroupListing> listings = adminClient.listConsumerGroups().all().get(10, TimeUnit.SECONDS);
            List<String> groupIds = listings.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toList());

            if (groupIds.isEmpty()) {
                return Collections.emptyList();
            }

            Map<String, ConsumerGroupDescription> descriptions = adminClient.describeConsumerGroups(groupIds).all().get(10, TimeUnit.SECONDS);

            return descriptions.values().stream()
                    .map(desc -> new ConsumerGroupInfoDto(
                            desc.groupId(),
                            desc.state() != null ? desc.state().toString() : "UNKNOWN",
                            desc.members().size(),
                            desc.isSimpleConsumerGroup()
                    ))
                    .sorted(Comparator.comparing(ConsumerGroupInfoDto::getGroupId))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Kafka Admin Error al listar Consumer Groups: {}", e.getMessage());
            throw new RuntimeException("Error al listar consumer groups Kafka: " + e.getMessage(), e);
        }
    }
}
