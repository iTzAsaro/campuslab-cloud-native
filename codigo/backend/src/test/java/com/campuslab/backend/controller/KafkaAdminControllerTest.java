package com.campuslab.backend.controller;

import com.campuslab.backend.dto.kafka.ConsumerGroupInfoDto;
import com.campuslab.backend.dto.kafka.TopicCreateRequestDto;
import com.campuslab.backend.dto.kafka.TopicInfoDto;
import com.campuslab.backend.service.KafkaAdminService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaAdminController.class)
@AutoConfigureMockMvc(addFilters = false)
class KafkaAdminControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @MockBean
    private KafkaAdminService kafkaAdminService;

    @Test
    void testCreateTopicValid() throws Exception {
        TopicCreateRequestDto request = new TopicCreateRequestDto("campuslab.custom.stream", 3, (short) 1, 86400000L, "delete");
        TopicInfoDto topicInfo = new TopicInfoDto("campuslab.custom.stream", 3, 1, false, List.of(0, 1, 2), Map.of("retention.ms", "86400000"));

        when(kafkaAdminService.createTopic(any(TopicCreateRequestDto.class))).thenReturn(topicInfo);

        mockMvc.perform(post("/api/admin/kafka/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.status").value("SUCCESS"))
                .andExpect(jsonPath("$.topic.name").value("campuslab.custom.stream"))
                .andExpect(jsonPath("$.topic.partitions").value(3));

        verify(kafkaAdminService).createTopic(any(TopicCreateRequestDto.class));
    }

    @Test
    void testCreateTopicInvalidBlankName() throws Exception {
        TopicCreateRequestDto request = new TopicCreateRequestDto("", 3, (short) 1, 86400000L, "delete");

        mockMvc.perform(post("/api/admin/kafka/topics")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());

        verify(kafkaAdminService, never()).createTopic(any());
    }

    @Test
    void testListTopics() throws Exception {
        TopicInfoDto info = new TopicInfoDto("campuslab.bookings.stream", 3, 1, false, List.of(0, 1, 2), Collections.emptyMap());
        when(kafkaAdminService.listTopics()).thenReturn(List.of(info));

        mockMvc.perform(get("/api/admin/kafka/topics"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("campuslab.bookings.stream"))
                .andExpect(jsonPath("$[0].partitions").value(3));

        verify(kafkaAdminService).listTopics();
    }

    @Test
    void testDescribeTopic() throws Exception {
        TopicInfoDto info = new TopicInfoDto("campuslab.bookings.stream", 3, 1, false, List.of(0, 1, 2), Map.of("cleanup.policy", "delete"));
        when(kafkaAdminService.describeTopic("campuslab.bookings.stream")).thenReturn(info);

        mockMvc.perform(get("/api/admin/kafka/topics/campuslab.bookings.stream"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("campuslab.bookings.stream"))
                .andExpect(jsonPath("$.configs.['cleanup.policy']").value("delete"));

        verify(kafkaAdminService).describeTopic("campuslab.bookings.stream");
    }

    @Test
    void testDeleteTopic() throws Exception {
        doNothing().when(kafkaAdminService).deleteTopic("campuslab.to.delete");

        mockMvc.perform(delete("/api/admin/kafka/topics/campuslab.to.delete"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DELETED"));

        verify(kafkaAdminService).deleteTopic("campuslab.to.delete");
    }

    @Test
    void testListConsumerGroups() throws Exception {
        ConsumerGroupInfoDto group = new ConsumerGroupInfoDto("campuslab-analytics-group", "STABLE", 3, false);
        when(kafkaAdminService.listConsumerGroups()).thenReturn(List.of(group));

        mockMvc.perform(get("/api/admin/kafka/consumer-groups"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].groupId").value("campuslab-analytics-group"))
                .andExpect(jsonPath("$[0].state").value("STABLE"));

        verify(kafkaAdminService).listConsumerGroups();
    }
}
