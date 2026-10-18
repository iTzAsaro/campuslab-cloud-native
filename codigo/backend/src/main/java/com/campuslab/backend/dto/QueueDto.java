package com.campuslab.backend.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

public class QueueDto {

    @NotBlank(message = "El nombre de la cola es obligatorio")
    @Size(min = 3, max = 255, message = "El nombre de la cola debe tener entre 3 y 255 caracteres")
    @Pattern(regexp = "^[a-zA-Z0-9._-]+$", message = "El nombre de la cola solo puede contener letras, números, puntos, guiones y guiones bajos")
    private String name;

    private boolean durable = true;

    public QueueDto() {}

    public QueueDto(String name) {
        this.name = name;
        this.durable = true;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }
}
