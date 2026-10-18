package com.campuslab.backend.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitAdmin;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class RabbitAdminServiceTest {

    @Mock
    private RabbitAdmin rabbitAdmin;

    private RabbitAdminService rabbitAdminService;

    @BeforeEach
    void setUp() {
        rabbitAdminService = new RabbitAdminService(rabbitAdmin);
    }

    @Test
    void testCreateQueue() {
        rabbitAdminService.createQueue("test.queue", true);

        ArgumentCaptor<Queue> captor = ArgumentCaptor.forClass(Queue.class);
        verify(rabbitAdmin).declareQueue(captor.capture());

        Queue declaredQueue = captor.getValue();
        assertEquals("test.queue", declaredQueue.getName());
        assertTrue(declaredQueue.isDurable());
    }

    @Test
    void testDeleteQueue() {
        when(rabbitAdmin.deleteQueue("test.queue")).thenReturn(true);

        boolean result = rabbitAdminService.deleteQueue("test.queue");

        assertTrue(result);
        verify(rabbitAdmin).deleteQueue("test.queue");
    }

    @Test
    void testCreateDirectExchange() {
        rabbitAdminService.createExchange("test.direct", "direct", true);

        ArgumentCaptor<Exchange> captor = ArgumentCaptor.forClass(Exchange.class);
        verify(rabbitAdmin).declareExchange(captor.capture());

        Exchange exchange = captor.getValue();
        assertEquals("test.direct", exchange.getName());
        assertEquals("direct", exchange.getType());
        assertTrue(exchange.isDurable());
    }

    @Test
    void testCreateTopicExchange() {
        rabbitAdminService.createExchange("test.topic", "topic", true);

        ArgumentCaptor<Exchange> captor = ArgumentCaptor.forClass(Exchange.class);
        verify(rabbitAdmin).declareExchange(captor.capture());

        Exchange exchange = captor.getValue();
        assertEquals("test.topic", exchange.getName());
        assertEquals("topic", exchange.getType());
    }

    @Test
    void testDeleteExchange() {
        when(rabbitAdmin.deleteExchange("test.direct")).thenReturn(true);

        boolean result = rabbitAdminService.deleteExchange("test.direct");

        assertTrue(result);
        verify(rabbitAdmin).deleteExchange("test.direct");
    }

    @Test
    void testCreateBinding() {
        rabbitAdminService.createBinding("test.queue", "test.direct", "test.key");

        ArgumentCaptor<Binding> captor = ArgumentCaptor.forClass(Binding.class);
        verify(rabbitAdmin).declareBinding(captor.capture());

        Binding binding = captor.getValue();
        assertEquals("test.queue", binding.getDestination());
        assertEquals("test.direct", binding.getExchange());
        assertEquals("test.key", binding.getRoutingKey());
    }

    @Test
    void testRemoveBinding() {
        rabbitAdminService.removeBinding("test.queue", "test.direct", "test.key");

        ArgumentCaptor<Binding> captor = ArgumentCaptor.forClass(Binding.class);
        verify(rabbitAdmin).removeBinding(captor.capture());

        Binding binding = captor.getValue();
        assertEquals("test.queue", binding.getDestination());
        assertEquals("test.direct", binding.getExchange());
        assertEquals("test.key", binding.getRoutingKey());
    }
}
