package com.campuslab.backend.service;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private KafkaProducerService producerService;

    @BeforeEach
    void setUp() {
        producerService = new KafkaProducerService(kafkaTemplate);
        ReflectionTestUtils.setField(producerService, "bookingsStreamTopic", "campuslab.bookings.stream");
        ReflectionTestUtils.setField(producerService, "telemetryMetricsTopic", "campuslab.telemetry.metrics");
    }

    @Test
    void testPublishBookingStreamEvent() {
        BookingStreamEventDto event = new BookingStreamEventDto(
                "EVT-100", 101L, "LAB-CLOUD-01", "student@campuslab.cl",
                "RESERVATION_CREATED", 120, 2
        );

        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(eq("campuslab.bookings.stream"), eq("LAB-CLOUD-01"), eq(event))).thenReturn(future);

        producerService.publishBookingStreamEvent(event);

        verify(kafkaTemplate).send("campuslab.bookings.stream", "LAB-CLOUD-01", event);
    }

    @Test
    void testPublishTelemetryMetric() {
        LabTelemetryDto metric = new LabTelemetryDto(
                "TEL-500", "LAB-REDES-02", "SENSOR-TEMP", 15, 24.5, 60.0, 2100.0
        );

        CompletableFuture<SendResult<String, Object>> future = new CompletableFuture<>();
        when(kafkaTemplate.send(eq("campuslab.telemetry.metrics"), eq("LAB-REDES-02"), eq(metric))).thenReturn(future);

        producerService.publishTelemetryMetric(metric);

        verify(kafkaTemplate).send("campuslab.telemetry.metrics", "LAB-REDES-02", metric);
    }
}
