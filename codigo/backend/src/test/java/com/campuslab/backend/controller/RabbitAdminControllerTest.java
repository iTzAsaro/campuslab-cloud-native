package com.campuslab.backend.controller;

import com.campuslab.backend.dto.BindingDto;
import com.campuslab.backend.dto.ExchangeDto;
import com.campuslab.backend.dto.QueueDto;
import com.campuslab.backend.service.RabbitAdminService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.*;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(RabbitAdminController.class)
@AutoConfigureMockMvc(addFilters = false) // Deshabilitar filtros de seguridad para pruebas unitarias de MVC
class RabbitAdminControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private RabbitAdminService rabbitAdminService;

    @Test
    void testCreateQueueValid() throws Exception {
        QueueDto dto = new QueueDto("my.custom.queue");

        mockMvc.perform(post("/api/admin/queues")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.queue").value("my.custom.queue"));

        verify(rabbitAdminService).createQueue("my.custom.queue", true);
    }

    @Test
    void testCreateQueueInvalidBlankName() throws Exception {
        QueueDto dto = new QueueDto("");

        mockMvc.perform(post("/api/admin/queues")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isBadRequest());

        verify(rabbitAdminService, never()).createQueue(anyString(), anyBoolean());
    }

    @Test
    void testDeleteQueue() throws Exception {
        when(rabbitAdminService.deleteQueue("my.custom.queue")).thenReturn(true);

        mockMvc.perform(delete("/api/admin/queues/my.custom.queue"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.deleted").value(true));

        verify(rabbitAdminService).deleteQueue("my.custom.queue");
    }

    @Test
    void testCreateExchangeValid() throws Exception {
        ExchangeDto dto = new ExchangeDto("orders.exchange", "topic");

        mockMvc.perform(post("/api/admin/exchanges")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.exchange").value("orders.exchange"))
                .andExpect(jsonPath("$.type").value("topic"));

        verify(rabbitAdminService).createExchange("orders.exchange", "topic", true);
    }

    @Test
    void testCreateExchangeInvalidType() throws Exception {
        ExchangeDto dto = new ExchangeDto("orders.exchange", "invalido");

        mockMvc.perform(post("/api/admin/exchanges")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isBadRequest());

        verify(rabbitAdminService, never()).createExchange(anyString(), anyString(), anyBoolean());
    }

    @Test
    void testCreateBindingValid() throws Exception {
        BindingDto dto = new BindingDto("my.queue", "my.exchange", "routing.key");

        mockMvc.perform(post("/api/admin/bindings")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(dto)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("success"));

        verify(rabbitAdminService).createBinding("my.queue", "my.exchange", "routing.key");
    }
}
