package com.campuslab.backend.controller;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import com.campuslab.backend.service.AnalyticsProcessingService;
import com.campuslab.backend.service.KafkaProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaStreamController.class)
@AutoConfigureMockMvc(addFilters = false)
class KafkaStreamControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private KafkaProducerService kafkaProducerService;

    @MockBean
    private AnalyticsProcessingService analyticsProcessingService;

    @Test
    void testPublishBookingEvent() throws Exception {
        BookingStreamEventDto dto = new BookingStreamEventDto(
                "EVT-TEST-1", 101L, "LAB-CLOUD-01", "student@campuslab.cl",
                "RESERVATION_CREATED", 120, 2
        );

        when(kafkaProducerService.publishBookingStreamEvent(any(BookingStreamEventDto.class)))
                .thenReturn(new CompletableFuture<>());

        mockMvc.perform(post("/api/stream/kafka/bookings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("QUEUED_TO_KAFKA"))
                .andExpect(jsonPath("$.topic").value("campuslab.bookings.stream"))
                .andExpect(jsonPath("$.partitionKey").value("LAB-CLOUD-01"));

        verify(kafkaProducerService).publishBookingStreamEvent(any(BookingStreamEventDto.class));
    }

    @Test
    void testPublishTelemetry() throws Exception {
        LabTelemetryDto metric = new LabTelemetryDto(
                "TEL-TEST-1", "LAB-CLOUD-01", "SENSOR-1", 15, 23.5, 50.0, 1600.0
        );

        when(kafkaProducerService.publishTelemetryMetric(any(LabTelemetryDto.class)))
                .thenReturn(new CompletableFuture<>());

        mockMvc.perform(post("/api/stream/kafka/telemetry")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(metric)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("QUEUED_TO_KAFKA"))
                .andExpect(jsonPath("$.topic").value("campuslab.telemetry.metrics"))
                .andExpect(jsonPath("$.partitionKey").value("LAB-CLOUD-01"));

        verify(kafkaProducerService).publishTelemetryMetric(any(LabTelemetryDto.class));
    }

    @Test
    void testSimulateLoad() throws Exception {
        when(kafkaProducerService.publishBookingStreamEvent(any(BookingStreamEventDto.class)))
                .thenReturn(new CompletableFuture<>());

        mockMvc.perform(post("/api/stream/kafka/simulate-load"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("BURST_PUBLISHED"))
                .andExpect(jsonPath("$.totalEvents").value(10));

        verify(kafkaProducerService, times(10)).publishBookingStreamEvent(any(BookingStreamEventDto.class));
    }

    @Test
    void testTriggerDltError() throws Exception {
        when(kafkaProducerService.publishBookingStreamEvent(any(BookingStreamEventDto.class)))
                .thenReturn(new CompletableFuture<>());

        mockMvc.perform(post("/api/stream/kafka/trigger-dlt-error"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("POISON_PILL_PUBLISHED"));

        verify(kafkaProducerService).publishBookingStreamEvent(any(BookingStreamEventDto.class));
    }

    @Test
    void testGetAnalytics() throws Exception {
        when(analyticsProcessingService.getAnalyticsSummary()).thenReturn(Map.of(
                "totalBookingsProcessed", 42L,
                "totalTelemetryPointsProcessed", 100L
        ));

        mockMvc.perform(get("/api/stream/kafka/analytics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalBookingsProcessed").value(42))
                .andExpect(jsonPath("$.totalTelemetryPointsProcessed").value(100));

        verify(analyticsProcessingService).getAnalyticsSummary();
    }
}
