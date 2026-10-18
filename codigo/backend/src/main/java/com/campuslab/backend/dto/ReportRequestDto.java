package com.campuslab.backend.dto;

import jakarta.validation.constraints.NotBlank;

public class ReportRequestDto {

    @NotBlank(message = "El nombre del reporte es obligatorio")
    private String reportName;

    @NotBlank(message = "El formato es obligatorio (PDF, CSV, XLSX)")
    private String format;

    private String requestedBy;

    public ReportRequestDto() {}

    public ReportRequestDto(String reportName, String format, String requestedBy) {
        this.reportName = reportName;
        this.format = format;
        this.requestedBy = requestedBy;
    }

    public String getReportName() { return reportName; }
    public void setReportName(String reportName) { this.reportName = reportName; }

    public String getFormat() { return format; }
    public void setFormat(String format) { this.format = format; }

    public String getRequestedBy() { return requestedBy; }
    public void setRequestedBy(String requestedBy) { this.requestedBy = requestedBy; }
}
