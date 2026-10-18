package com.campuslab.backend.controller;

import com.campuslab.backend.dto.NotificationRequestDto;
import com.campuslab.backend.repository.AuditLogRepository;
import com.campuslab.backend.repository.BookingRepository;
import com.campuslab.backend.service.MessageProducerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventController.class)
@AutoConfigureMockMvc(addFilters = false)
class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private MessageProducerService messageProducerService;

    @MockBean
    private BookingRepository bookingRepository;

    @MockBean
    private AuditLogRepository auditLogRepository;

    @Test
    void testHealthEndpoint() throws Exception {
        mockMvc.perform(get("/api/health"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("UP"))
                .andExpect(jsonPath("$.service").value("campuslab-backend"));
    }

    @Test
    void testSendNotification() throws Exception {
        NotificationRequestDto dto = new NotificationRequestDto("estudiante@campuslab.cl", "Tu reserva está lista", "CONFIRM");

        mockMvc.perform(post("/api/events/notification")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PUBLISHED"))
                .andExpect(jsonPath("$.exchange").value("campuslab.direct"));

        verify(messageProducerService).sendBookingNotification(any(NotificationRequestDto.class));
    }

    @Test
    void testTriggerErrorNotification() throws Exception {
        mockMvc.perform(post("/api/events/notification/error-test"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("SIMULATED_ERROR_PUBLISHED"))
                .andExpect(jsonPath("$.targetDlq").value("campuslab.booking.notifications.dlq"));

        verify(messageProducerService).sendSimulatedErrorNotification();
    }
}
