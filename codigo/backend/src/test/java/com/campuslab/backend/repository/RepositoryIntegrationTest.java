package com.campuslab.backend.repository;

import com.campuslab.backend.model.AuditLog;
import com.campuslab.backend.model.Booking;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@DataJpaTest
@ActiveProfiles("test")
class RepositoryIntegrationTest {

    @Autowired
    private BookingRepository bookingRepository;

    @Autowired
    private AuditLogRepository auditLogRepository;

    @Test
    void testSaveAndFindBooking() {
        Booking booking = new Booking("estudiante@campuslab.cl", "Lab Redes 202", LocalDateTime.now());
        Booking saved = bookingRepository.save(booking);

        assertNotNull(saved.getId());
        List<Booking> userBookings = bookingRepository.findByUserEmail("estudiante@campuslab.cl");
        assertFalse(userBookings.isEmpty());
        assertEquals("Lab Redes 202", userBookings.get(0).getLabName());
    }

    @Test
    void testSaveAndFindAuditLog() {
        AuditLog audit = new AuditLog("LOGIN_EVENT", "Usuario alex autenticado", "campuslab.audit.events");
        AuditLog saved = auditLogRepository.save(audit);

        assertNotNull(saved.getId());
        List<AuditLog> logs = auditLogRepository.findTop50ByOrderByTimestampDesc();
        assertFalse(logs.isEmpty());
        assertEquals("LOGIN_EVENT", logs.get(0).getAction());
    }
}
