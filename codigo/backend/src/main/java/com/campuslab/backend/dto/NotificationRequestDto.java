package com.campuslab.backend.dto;

import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;

public class NotificationRequestDto {

    @NotBlank(message = "El email del usuario es obligatorio")
    @Email(message = "Debe ser un email válido")
    private String userEmail;

    @NotBlank(message = "El mensaje o título es obligatorio")
    private String message;

    private String type = "BOOKING_CONFIRMATION";

    public NotificationRequestDto() {}

    public NotificationRequestDto(String userEmail, String message, String type) {
        this.userEmail = userEmail;
        this.message = message;
        this.type = type;
    }

    public String getUserEmail() { return userEmail; }
    public void setUserEmail(String userEmail) { this.userEmail = userEmail; }

    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
}
