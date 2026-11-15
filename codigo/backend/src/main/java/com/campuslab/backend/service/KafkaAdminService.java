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

// Servicio para administrar tópicos y grupos de consumo usando el AdminClient de Kafka.
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private final AdminClient adminClient;

    public KafkaAdminService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    // Crea un nuevo tópico con particiones, réplicas y retención configurables
    public TopicInfoDto createTopic(TopicCreateRequestDto request) {
        log.info("Kafka Admin: Creando tópico [{}] con {} particiones", request.getName(), request.getPartitions());

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
            log.info("Kafka Admin: Tópico [{}] creado con éxito", request.getName());
            return describeTopic(request.getName());
        } catch (Exception e) {
            log.error("Kafka Admin: Error al crear tópico [{}]: {}", request.getName(), e.getMessage());
            throw new RuntimeException("Error al crear tópico Kafka: " + e.getMessage(), e);
        }
    }

    // Lista los tópicos del clúster (excluyendo tópicos internos del sistema)
    public List<TopicInfoDto> listTopics() {
        try {
            ListTopicsResult listTopicsResult = adminClient.listTopics(new ListTopicsOptions().listInternal(false));
            Map<String, TopicListing> topicListingMap = listTopicsResult.namesToListings().get(10, TimeUnit.SECONDS);

            if (topicListingMap.isEmpty()) {
                return Collections.emptyList();
            }

            DescribeTopicsResult describeResult = adminClient.describeTopics(topicListingMap.keySet());
            Map<String, TopicDescription> descriptionMap = describeResult.allTopicNames().get(10, TimeUnit.SECONDS);

            List<TopicInfoDto> result = new ArrayList<>();
            for (Map.Entry<String, TopicDescription> entry : descriptionMap.entrySet()) {
                String topicName = entry.getKey();
                TopicDescription desc = entry.getValue();

                List<Integer> partitionIds = desc.partitions().stream()
                        .map(TopicPartitionInfo::partition)
                        .sorted()
                        .collect(Collectors.toList());

                int replicationFactor = desc.partitions().isEmpty() ? 1 : desc.partitions().get(0).replicas().size();

                result.add(new TopicInfoDto(
                        topicName,
                        desc.partitions().size(),
                        (short) replicationFactor,
                        desc.isInternal(),
                        partitionIds,
                        Collections.emptyMap()
                ));
            }

            result.sort(Comparator.comparing(TopicInfoDto::getName));
            return result;
        } catch (Exception e) {
            log.error("Kafka Admin: Error al listar tópicos: {}", e.getMessage());
            throw new RuntimeException("Error al listar tópicos de Kafka: " + e.getMessage(), e);
        }
    }

    // Obtiene el detalle de un tópico (particiones, réplicas y configuración)
    public TopicInfoDto describeTopic(String topicName) {
        try {
            DescribeTopicsResult describeResult = adminClient.describeTopics(Collections.singletonList(topicName));
            TopicDescription desc = describeResult.allTopicNames().get(10, TimeUnit.SECONDS).get(topicName);

            if (desc == null) {
                throw new NoSuchElementException("Tópico no encontrado: " + topicName);
            }

            List<Integer> partitionIds = desc.partitions().stream()
                    .map(TopicPartitionInfo::partition)
                    .sorted()
                    .collect(Collectors.toList());

            int replicationFactor = desc.partitions().isEmpty() ? 1 : desc.partitions().get(0).replicas().size();

            ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            DescribeConfigsResult configsResult = adminClient.describeConfigs(Collections.singletonList(resource));
            Config config = configsResult.all().get(10, TimeUnit.SECONDS).get(resource);

            Map<String, String> configMap = new HashMap<>();
            if (config != null) {
                config.entries().forEach(entry -> {
                    if (!entry.isDefault()) {
                        configMap.put(entry.name(), entry.value());
                    }
                });
            }

            return new TopicInfoDto(
                    topicName,
                    desc.partitions().size(),
                    (short) replicationFactor,
                    desc.isInternal(),
                    partitionIds,
                    configMap
            );
        } catch (Exception e) {
            log.error("Kafka Admin: Error al describir tópico [{}]: {}", topicName, e.getMessage());
            throw new RuntimeException("Error al describir tópico Kafka: " + e.getMessage(), e);
        }
    }

    // Elimina un tópico del clúster
    public void deleteTopic(String topicName) {
        log.info("Kafka Admin: Eliminando tópico [{}]", topicName);
        try {
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get(10, TimeUnit.SECONDS);
            log.info("Kafka Admin: Tópico [{}] eliminado", topicName);
        } catch (Exception e) {
            log.error("Kafka Admin: Error al eliminar tópico [{}]: {}", topicName, e.getMessage());
            throw new RuntimeException("Error al eliminar tópico Kafka: " + e.getMessage(), e);
        }
    }

    // Lista los grupos de consumidores registrados en el clúster y su estado
    public List<ConsumerGroupInfoDto> listConsumerGroups() {
        try {
            ListConsumerGroupsResult listResult = adminClient.listConsumerGroups();
            Collection<ConsumerGroupListing> listings = listResult.all().get(10, TimeUnit.SECONDS);

            List<String> groupIds = listings.stream()
                    .map(ConsumerGroupListing::groupId)
                    .collect(Collectors.toList());

            if (groupIds.isEmpty()) {
                return Collections.emptyList();
            }

            DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(groupIds);
            Map<String, ConsumerGroupDescription> descriptions = describeResult.all().get(10, TimeUnit.SECONDS);

            List<ConsumerGroupInfoDto> result = new ArrayList<>();
            for (Map.Entry<String, ConsumerGroupDescription> entry : descriptions.entrySet()) {
                ConsumerGroupDescription desc = entry.getValue();
                result.add(new ConsumerGroupInfoDto(
                        desc.groupId(),
                        desc.state() != null ? desc.state().toString() : "UNKNOWN",
                        desc.members().size(),
                        desc.isSimpleConsumerGroup()
                ));
            }

            result.sort(Comparator.comparing(ConsumerGroupInfoDto::getGroupId));
            return result;
        } catch (Exception e) {
            log.error("Kafka Admin: Error al consultar grupos de consumidores: {}", e.getMessage());
            throw new RuntimeException("Error al consultar consumer groups de Kafka: " + e.getMessage(), e);
        }
    }
}
