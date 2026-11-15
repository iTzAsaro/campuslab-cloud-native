package com.campuslab.backend.listener;

import com.campuslab.backend.dto.kafka.BookingStreamEventDto;
import com.campuslab.backend.service.AnalyticsProcessingService;
import com.campuslab.backend.service.KafkaProducerService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@EmbeddedKafka(
        partitions = 3,
        topics = {"campuslab.bookings.stream", "campuslab.telemetry.metrics", "campuslab.bookings.stream.DLT"}
)
@ActiveProfiles("test")
@DirtiesContext
class KafkaConsumerIntegrationTest {

    @Autowired
    private KafkaProducerService producerService;

    @Autowired
    private AnalyticsProcessingService analyticsProcessingService;

    @Test
    void testEndToEndBookingStreaming() {
        BookingStreamEventDto event = new BookingStreamEventDto(
                "INTEG-EVT-01", 5001L, "LAB-CLOUD-01", "student.integ@campuslab.cl",
                "RESERVATION_CREATED", 90, 4
        );

        producerService.publishBookingStreamEvent(event);

        // Esperar a que el consumidor procese el evento asíncrono
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            Map<String, Object> summary = analyticsProcessingService.getAnalyticsSummary();
            long total = (long) summary.get("totalBookingsProcessed");
            assertEquals(true, total >= 1, "Debe haber procesado al menos un evento de reserva");
        });
    }
}
