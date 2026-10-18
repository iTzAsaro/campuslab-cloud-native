package com.campuslab.backend.controller;

import com.campuslab.backend.dto.AuditEventDto;
import com.campuslab.backend.dto.NotificationRequestDto;
import com.campuslab.backend.dto.ReportRequestDto;
import com.campuslab.backend.model.AuditLog;
import com.campuslab.backend.model.Booking;
import com.campuslab.backend.repository.AuditLogRepository;
import com.campuslab.backend.repository.BookingRepository;
import com.campuslab.backend.service.MessageProducerService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class EventController {

    private final MessageProducerService messageProducerService;
    private final BookingRepository bookingRepository;
    private final AuditLogRepository auditLogRepository;

    public EventController(MessageProducerService messageProducerService,
                           BookingRepository bookingRepository,
                           AuditLogRepository auditLogRepository) {
        this.messageProducerService = messageProducerService;
        this.bookingRepository = bookingRepository;
        this.auditLogRepository = auditLogRepository;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "campuslab-backend",
                "timestamp", LocalDateTime.now().toString()
        ));
    }

    @PostMapping("/events/notification")
    public ResponseEntity<Map<String, Object>> sendNotification(@Valid @RequestBody NotificationRequestDto dto) {
        messageProducerService.sendBookingNotification(dto);
        return ResponseEntity.ok(Map.of(
                "status", "PUBLISHED",
                "exchange", "campuslab.direct",
                "routingKey", "routing.notifications",
                "message", dto.getMessage()
        ));
    }

    @PostMapping("/events/report")
    public ResponseEntity<Map<String, Object>> sendReportRequest(@Valid @RequestBody ReportRequestDto dto) {
        messageProducerService.sendReportRequest(dto);
        return ResponseEntity.ok(Map.of(
                "status", "QUEUED",
                "exchange", "campuslab.topic",
                "routingKey", "routing.reports.generate",
                "report", dto.getReportName()
        ));
    }

    @PostMapping("/events/audit")
    public ResponseEntity<Map<String, Object>> sendAuditEvent(@Valid @RequestBody AuditEventDto dto) {
        messageProducerService.sendAuditEvent(dto);
        return ResponseEntity.ok(Map.of(
                "status", "LOGGED",
                "exchange", "campuslab.topic",
                "routingKey", "routing.audit.log",
                "action", dto.getAction()
        ));
    }

    @PostMapping("/events/notification/error-test")
    public ResponseEntity<Map<String, Object>> triggerErrorNotification() {
        messageProducerService.sendSimulatedErrorNotification();
        return ResponseEntity.ok(Map.of(
                "status", "SIMULATED_ERROR_PUBLISHED",
                "description", "Mensaje con 'error' enviado a RabbitMQ. El consumidor emitirá basicNack(requeue=false) enviándolo a la DLQ.",
                "targetDlq", "campuslab.booking.notifications.dlq"
        ));
    }

    @PostMapping("/events/bookings")
    public ResponseEntity<Map<String, Object>> createBooking(@RequestBody Map<String, String> payload) {
        String email = payload.getOrDefault("userEmail", "estudiante@campuslab.cl");
        String lab = payload.getOrDefault("labName", "Laboratorio Cloud 101");
        Booking booking = new Booking(email, lab, LocalDateTime.now().plusDays(1));
        Booking saved = bookingRepository.save(booking);

        // Disparar mensaje a RabbitMQ
        NotificationRequestDto notif = new NotificationRequestDto(
                saved.getUserEmail(),
                "Reserva confirmada ID #" + saved.getId() + " para " + saved.getLabName(),
                "BOOKING_CREATED"
        );
        messageProducerService.sendBookingNotification(notif);

        return ResponseEntity.status(HttpStatus.CREATED).body(Map.of(
                "booking", saved,
                "message", "Reserva persistida en BD Cloud y evento publicado en RabbitMQ"
        ));
    }

    @GetMapping("/events/audits")
    public ResponseEntity<List<AuditLog>> getAuditLogs() {
        return ResponseEntity.ok(auditLogRepository.findTop50ByOrderByTimestampDesc());
    }
}
