package com.campuslab.backend.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public class ExchangeDto {

    @NotBlank(message = "El nombre del exchange es obligatorio")
    @Size(min = 3, max = 255, message = "El nombre del exchange debe tener entre 3 y 255 caracteres")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre del exchange solo puede contener letras, números, puntos, guiones y guiones bajos")
    private String name;

    @NotBlank(message = "El tipo de exchange es obligatorio (direct, topic, fanout)")
    @Pattern(regexp = "^(direct|topic|fanout|headers)$", flags = Pattern.Flag.CASE_INSENSITIVE, message = "El tipo de exchange debe ser 'direct', 'topic', 'fanout' o 'headers'")
    private String type;

    private boolean durable = true;

    public ExchangeDto() {}

    public ExchangeDto(String name, String type) {
        this.name = name;
        this.type = type;
        this.durable = true;
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public boolean isDurable() { return durable; }
    public void setDurable(boolean durable) { this.durable = durable; }
}
