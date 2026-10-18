package com.campuslab.backend.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public class BindingDto {

    @NotBlank(message = "El nombre de la cola es obligatorio")
    private String queue;

    @NotBlank(message = "El nombre del exchange es obligatorio")
    private String exchange;

    @NotBlank(message = "La routing key es obligatoria")
    private String routingKey;

    public BindingDto() {}

    public BindingDto(String queue, String exchange, String routingKey) {
        this.queue = queue;
        this.exchange = exchange;
        this.routingKey = routingKey;
    }

    public String getQueue() { return queue; }
    public void setQueue(String queue) { this.queue = queue; }

    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }

    public String getRoutingKey() { return routingKey; }
    public void setRoutingKey(String routingKey) { this.routingKey = routingKey; }
}
