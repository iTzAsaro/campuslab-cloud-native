package com.campuslab.backend.service;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.dto.kafka.LabTelemetryDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AnalyticsProcessingServiceTest {

    private AnalyticsProcessingService service;

    @BeforeEach
    void setUp() {
        service = new AnalyticsProcessingService();
    }

    @Test
    void testProcessBookingEventsAndAggregates() {
        BookingStreamEventDto event1 = new BookingStreamEventDto(
                "EVT-01", 1L, "LAB-01", "user1@campuslab.cl", "RESERVATION_CREATED", 60, 5
        );
        service.processBookingEvent(event1, 0, 100L);

        Map<String, Object> summary = service.getAnalyticsSummary();
        assertEquals(1L, summary.get("totalBookingsProcessed"));

        @SuppressWarnings("unchecked")
        Map<String, Integer> workstations = (Map<String, Integer>) summary.get("activeWorkstationsByLab");
        assertEquals(5, workstations.get("LAB-01"));

        // Evento de cancelación
        BookingStreamEventDto event2 = new BookingStreamEventDto(
                "EVT-02", 1L, "LAB-01", "user1@campuslab.cl", "RESERVATION_CANCELLED", 60, 2
        );
        service.processBookingEvent(event2, 0, 101L);

        Map<String, Object> updatedSummary = service.getAnalyticsSummary();
        assertEquals(2L, updatedSummary.get("totalBookingsProcessed"));

        @SuppressWarnings("unchecked")
        Map<String, Integer> updatedWorkstations = (Map<String, Integer>) updatedSummary.get("activeWorkstationsByLab");
        assertEquals(3, updatedWorkstations.get("LAB-01"));
    }

    @Test
    void testProcessTelemetryAndOverheatAlert() {
        // Métrica normal
        LabTelemetryDto normalMetric = new LabTelemetryDto(
                "TEL-1", "LAB-01", "S1", 10, 22.0, 30.0, 1500.0
        );
        service.processTelemetryMetric(normalMetric, 1, 200L);

        Map<String, Object> summary = service.getAnalyticsSummary();
        assertEquals(1L, summary.get("totalTelemetryPointsProcessed"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> alerts = (List<Map<String, Object>>) summary.get("recentAlerts");
        assertTrue(alerts.isEmpty(), "No debería haber alertas con 22.0 °C");

        // Métrica crítica (>28 °C)
        LabTelemetryDto hotMetric = new LabTelemetryDto(
                "TEL-2", "LAB-01", "S1", 10, 29.5, 95.0, 2800.0
        );
        service.processTelemetryMetric(hotMetric, 1, 201L);

        Map<String, Object> updatedSummary = service.getAnalyticsSummary();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> updatedAlerts = (List<Map<String, Object>>) updatedSummary.get("recentAlerts");
        assertEquals(1, updatedAlerts.size());
        assertEquals("CRITICAL_TEMPERATURE_ALERT", updatedAlerts.get(0).get("type"));
    }

    @Test
    void testRecordDltFailure() {
        service.recordDltFailure("campuslab.bookings.stream", 2, 505L, "Payload corrupto");

        Map<String, Object> summary = service.getAnalyticsSummary();
        assertEquals(1L, summary.get("totalDltAlerts"));

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> alerts = (List<Map<String, Object>>) summary.get("recentAlerts");
        assertEquals(1, alerts.size());
        assertEquals("DEAD_LETTER_TOPIC_RECOVERY", alerts.get(0).get("type"));
    }
}
