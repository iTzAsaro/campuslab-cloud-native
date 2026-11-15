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

// Controlador REST para administrar tópicos y grupos de consumo en Apache Kafka.
@RestController
@RequestMapping("/api/admin/kafka")
@CrossOrigin(origins = "*")
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    public KafkaAdminController(KafkaAdminService kafkaAdminService) {
        this.kafkaAdminService = kafkaAdminService;
    }

    // Listar todos los tópicos del clúster
    @GetMapping("/topics")
    public ResponseEntity<List<TopicInfoDto>> listTopics() {
        return ResponseEntity.ok(kafkaAdminService.listTopics());
    }

    // Obtener detalles de un tópico específico
    @GetMapping("/topics/{topicName}")
    public ResponseEntity<TopicInfoDto> getTopicDetails(@PathVariable String topicName) {
        return ResponseEntity.ok(kafkaAdminService.describeTopic(topicName));
    }

    // Crear un nuevo tópico dinámicamente
    @PostMapping("/topics")
    public ResponseEntity<Map<String, Object>> createTopic(@Valid @RequestBody TopicCreateRequestDto request) {
        TopicInfoDto createdTopic = kafkaAdminService.createTopic(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "status", "SUCCESS",
                "message", "Tópico Kafka creado exitosamente",
                "topic", createdTopic
        ));
    }

    // Eliminar un tópico del clúster
    @DeleteMapping("/topics/{topicName}")
    public ResponseEntity<Map<String, String>> deleteTopic(@PathVariable String topicName) {
        kafkaAdminService.deleteTopic(topicName);
        return ResponseEntity.ok(Map.of(
                "status", "DELETED",
                "message", "Tópico Kafka eliminado exitosamente: " + topicName
        ));
    }

    // Listar grupos de consumidores y su estado
    @GetMapping("/consumer-groups")
    public ResponseEntity<List<ConsumerGroupInfoDto>> listConsumerGroups() {
        return ResponseEntity.ok(kafkaAdminService.listConsumerGroups());
    }
}
