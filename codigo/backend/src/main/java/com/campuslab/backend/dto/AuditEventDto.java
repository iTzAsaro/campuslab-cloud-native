package com.campuslab.backend.dto;

import jakarta.validation.constraints.NotBlank;

public class AuditEventDto {

    @NotBlank(message = "La acción es obligatoria")
    private String action;

    @NotBlank(message = "El detalle es obligatorio")
    private String details;

    private String userEmail;

    public AuditEventDto() {}

    public AuditEventDto(String action, String details, String userEmail) {
        this.action = action;
        this.details = details;
        this.userEmail = userEmail;
    }

    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }

    public String getDetails() { return details; }
    public void setDetails(String details) { this.details = details; }

    public String getUserEmail() { return userEmail; }
    public void setUserEmail(String userEmail) { this.userEmail = userEmail; }
}
