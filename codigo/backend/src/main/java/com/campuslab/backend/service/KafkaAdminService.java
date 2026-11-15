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
 * =========================================================================================
 * MICROSERVICIO ADMINISTRADOR DE APACHE KAFKA (KAFKA ADMIN SERVICE)
 * =========================================================================================
 * 
 * Implementa la gestión dinámica y el ciclo de vida del clúster Apache Kafka utilizando
 * el cliente programático nativo `AdminClient`.
 * 
 * Capacidades Principales:
 * 1. Creación Dinámica de Tópicos: Permite definir nombre, particiones, réplicas y políticas
 *    de retención personalizadas desde la API REST o la interfaz web.
 * 2. Listado e Inspección de Metadatos: Obtiene en tiempo real las particiones activas,
 *    líderes de réplica, políticas de limpieza y configuraciones efectivas.
 * 3. Eliminación Segura de Tópicos: Borra tópicos del catálogo del broker y ZooKeeper.
 * 4. Monitoreo de Consumer Groups: Inspecciona grupos de consumidores activos, su estado
 *    de rebalanceo (Stable, CompletingRebalance, Dead) y la cantidad de miembros conectados.
 */
@Service
public class KafkaAdminService {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminService.class);

    private final AdminClient adminClient;

    public KafkaAdminService(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    /**
     * Crea un nuevo tópico Kafka en caliente sin necesidad de reiniciar la infraestructura.
     * 
     * @param request DTO con especificaciones de nombre, número de particiones y retención.
     * @return TopicInfoDto con el estado del tópico recién materializado en los brokers.
     */
    public TopicInfoDto createTopic(TopicCreateRequestDto request) {
        log.info("Kafka Admin [CREAR]: Solicitando creación de tópico [{}] con {} particiones y replicación {}",
                request.getName(), request.getPartitions(), request.getReplicationFactor());

        NewTopic newTopic = new NewTopic(request.getName(), request.getPartitions(), request.getReplicationFactor());

        // Aplicar configuraciones de retención y limpieza
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
            // Invoca la creación y espera confirmación síncrona del clúster (timeout 10s)
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get(10, TimeUnit.SECONDS);
            log.info("Kafka Admin [CREAR ÉXITO]: Tópico [{}] creado satisfactoriamente", request.getName());
            return describeTopic(request.getName());
        } catch (Exception e) {
            log.error("Kafka Admin [ERROR CREACIÓN]: No se pudo crear el tópico [{}]: {}", request.getName(), e.getMessage());
            throw new RuntimeException("Error al crear tópico Kafka: " + e.getMessage(), e);
        }
    }

    /**
     * Lista todos los tópicos registrados en el clúster (excluyendo tópicos internos del sistema como __consumer_offsets).
     */
    public List<TopicInfoDto> listTopics() {
        log.info("Kafka Admin [LISTAR]: Consultando catálogo de tópicos del clúster...");
        try {
            ListTopicsResult listTopicsResult = adminClient.listTopics(new ListTopicsOptions().listInternal(false));
            Map<String, TopicListing> topicListingMap = listTopicsResult.namesToListings().get(10, TimeUnit.SECONDS);

            if (topicListingMap.isEmpty()) {
                return Collections.emptyList();
            }

            // Describir tópicos para obtener el desglose detallado de particiones
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
            log.error("Kafka Admin [ERROR LISTADO]: Error al consultar tópicos del clúster: {}", e.getMessage());
            throw new RuntimeException("Error al listar tópicos de Kafka: " + e.getMessage(), e);
        }
    }

    /**
     * Obtiene la descripción detallada de un tópico específico, incluyendo particiones y configuraciones efectivas.
     */
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

            // Consultar configuraciones del tópico (retention, cleanup.policy, etc.)
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
            log.error("Kafka Admin [ERROR DESCRIBIR]: Error al describir tópico [{}]: {}", topicName, e.getMessage());
            throw new RuntimeException("Error al describir tópico Kafka: " + e.getMessage(), e);
        }
    }

    /**
     * Elimina un tópico del clúster Kafka.
     * 
     * @param topicName Nombre del tópico a suprimir.
     */
    public void deleteTopic(String topicName) {
        log.info("Kafka Admin [ELIMINAR]: Solicitando borrado del tópico [{}]", topicName);
        try {
            adminClient.deleteTopics(Collections.singletonList(topicName)).all().get(10, TimeUnit.SECONDS);
            log.info("Kafka Admin [ELIMINAR ÉXITO]: Tópico [{}] eliminado del clúster", topicName);
        } catch (Exception e) {
            log.error("Kafka Admin [ERROR ELIMINACIÓN]: Falló la eliminación del tópico [{}]: {}", topicName, e.getMessage());
            throw new RuntimeException("Error al eliminar tópico Kafka: " + e.getMessage(), e);
        }
    }

    /**
     * Consulta el estado de los Consumer Groups activos en el clúster.
     * Permite verificar la sincronización y membresía de `campuslab-analytics-group`,
     * `campuslab-telemetry-group` y `campuslab-dlt-audit-group`.
     */
    public List<ConsumerGroupInfoDto> listConsumerGroups() {
        log.info("Kafka Admin [CONSUMER GROUPS]: Consultando grupos registrados en el coordinador...");
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
            log.error("Kafka Admin [ERROR GRUPOS]: No se pudo consultar Consumer Groups: {}", e.getMessage());
            throw new RuntimeException("Error al consultar consumer groups de Kafka: " + e.getMessage(), e);
        }
    }
}
