package com.campuslab.backend.controller;

import com.campuslab.backend.dto.kafka.ConsumerGroupInfoDto;
import com.campuslab.backend.dto.kafka.TopicCreateRequestDto;
import com.campuslab.backend.dto.kafka.TopicInfoDto;
import com.campuslab.backend.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * =========================================================================================
 * CONTROLADOR REST: MICROSERVICIO ADMINISTRADOR DE KAFKA (KAFKA ADMIN CONTROLLER)
 * =========================================================================================
 * 
 * Expone la API REST pública para la gestión dinámica del clúster Apache Kafka.
 * 
 * Endpoints Disponibles:
 * - `GET    /api/admin/kafka/topics`                 -> Lista todos los tópicos y sus particiones.
 * - `GET    /api/admin/kafka/topics/{topicName}`     -> Obtiene la configuración detallada de un tópico.
 * - `POST   /api/admin/kafka/topics`                 -> Crea un nuevo tópico en caliente.
 * - `DELETE /api/admin/kafka/topics/{topicName}`     -> Elimina un tópico del clúster.
 * - `GET    /api/admin/kafka/consumer-groups`        -> Consulta estado y miembros de los grupos de consumidores.
 */
@RestController
@RequestMapping("/api/admin/kafka")
@CrossOrigin(origins = "*")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    /**
     * Lista todos los tópicos activos en el clúster Kafka.
     */
    @GetMapping("/topics")
    public ResponseEntity<List<TopicInfoDto>> listTopics() {
        return ResponseEntity.ok(kafkaAdminService.listTopics());
    }

    /**
     * Inspecciona los metadatos de un tópico específico.
     */
    @GetMapping("/topics/{topicName}")
    public ResponseEntity<TopicInfoDto> getTopicDetails(@PathVariable String topicName) {
        return ResponseEntity.ok(kafkaAdminService.describeTopic(topicName));
    }

    /**
     * Crea un tópico Kafka programáticamente con particiones y retención personalizadas.
     */
    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> createTopic(@Valid @RequestBody TopicCreateRequestDto request) {
        TopicInfoDto createdTopic = kafkaAdminService.createTopic(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "status", "SUCCESS",
                "message", "Tópico Kafka creado exitosamente",
                "topic", createdTopic
        ));
    }

    /**
     * Elimina un tópico del clúster.
     */
    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<Map<String, String>> deleteTopic(@PathVariable String topicName) {
        kafkaAdminService.deleteTopic(topicName);
        return ResponseEntity.ok(Map.of(
                "status", "DELETED",
                "message", "Tópico Kafka eliminado exitosamente: " + topicName
        ));
    }

    /**
     * Consulta el estado operativo y conteo de miembros de los Consumer Groups de Kafka.
     */
    @GetMapping("/consumer-groups")
    public ResponseEntity<List<ConsumerGroupInfoDto>> listConsumerGroups() {
        return ResponseEntity.ok(kafkaAdminService.listConsumerGroups());
    }
}
