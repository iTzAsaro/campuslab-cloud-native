package com.campuslab.backend.controller;

import com.campuslab.backend.dto.BindingDto;
import com.campuslab.backend.dto.ExchangeDto;
import com.campuslab.backend.dto.QueueDto;
import com.campuslab.backend.service.RabbitAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/admin")
public class RabbitAdminController {

    private final RabbitAdminService rabbitAdminService;

    public RabbitAdminController(RabbitAdminService rabbitAdminService) {
        this.rabbitAdminService = rabbitAdminService;
    }

    // --- Colas ---
    @PostMapping("/queues")
    public ResponseEntity<Map<String, Object>> createQueue(@Valid @RequestBody QueueDto queueDto) {
        rabbitAdminService.createQueue(queueDto.getName(), queueDto.isDurable());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "status", "success",
                "message", "Cola declarada exitosamente",
                "queue", queueDto.getName()
        ));
    }

    @DeleteMapping("/queues/{name}")
    public ResponseEntity<Map<String, Object>> deleteQueue(@PathVariable String name) {
        boolean deleted = rabbitAdminService.deleteQueue(name);
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Cola eliminada exitosamente",
                "queue", name,
                "deleted", deleted
        ));
    }

    // --- Exchanges ---
    @PostMapping("/exchanges")
    public ResponseEntity<Map<String, Object>> createExchange(@Valid @RequestBody ExchangeDto exchangeDto) {
        rabbitAdminService.createExchange(exchangeDto.getName(), exchangeDto.getType(), exchangeDto.isDurable());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "status", "success",
                "message", "Exchange declarado exitosamente",
                "exchange", exchangeDto.getName(),
                "type", exchangeDto.getType()
        ));
    }

    @DeleteMapping("/exchanges/{name}")
    public ResponseEntity<Map<String, Object>> deleteExchange(@PathVariable String name) {
        boolean deleted = rabbitAdminService.deleteExchange(name);
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Exchange eliminado exitosamente",
                "exchange", name,
                "deleted", deleted
        ));
    }

    // --- Bindings ---
    @PostMapping("/bindings")
    public ResponseEntity<Map<String, Object>> createBinding(@Valid @RequestBody BindingDto bindingDto) {
        rabbitAdminService.createBinding(bindingDto.getQueue(), bindingDto.getExchange(), bindingDto.getRoutingKey());
        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "status", "success",
                "message", "Binding creado exitosamente",
                "binding", bindingDto
        ));
    }

    @DeleteMapping("/bindings")
    public ResponseEntity<Map<String, Object>> removeBinding(@Valid @RequestBody BindingDto bindingDto) {
        rabbitAdminService.removeBinding(bindingDto.getQueue(), bindingDto.getExchange(), bindingDto.getRoutingKey());
        return ResponseEntity.ok(Map.of(
                "status", "success",
                "message", "Binding eliminado exitosamente",
                "binding", bindingDto
        ));
    }
}
