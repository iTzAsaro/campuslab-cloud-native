package com.campuslab.backend.dto.kafka;

import java.io.Serializable;
import java.time.Instant;

/**
 * DTO para el streaming de telemetría y sensores IoT de laboratorios.
 * Tópico Kafka: campuslab.telemetry.metrics
 * Partition Key sugerida: labCode
 */
public class LabTelemetryDto implements Serializable {

    private static final long serialVersionUID = 1L;

    private String metricId;
    private String labCode;
    private String sensorId;
    private Integer activeWorkstations;
    private Double temperatureCelsius;
    private Double cpuUtilizationPercent;
    private Double powerConsumptionWatts;
    private Instant timestamp;

    public LabTelemetryDto() {
        this.timestamp = Instant.now();
    }

    public LabTelemetryDto(String metricId, String labCode, String sensorId,
                           Integer activeWorkstations, Double temperatureCelsius,
                           Double cpuUtilizationPercent, Double powerConsumptionWatts) {
        this.metricId = metricId;
        this.labCode = labCode;
        this.sensorId = sensorId;
        this.activeWorkstations = activeWorkstations;
        this.temperatureCelsius = temperatureCelsius;
        this.cpuUtilizationPercent = cpuUtilizationPercent;
        this.powerConsumptionWatts = powerConsumptionWatts;
        this.timestamp = Instant.now();
    }

    public String getMetricId() {
        return metricId;
    }

    public void setMetricId(String metricId) {
        this.metricId = metricId;
    }

    public String getLabCode() {
        return labCode;
    }

    public void setLabCode(String labCode) {
        this.labCode = labCode;
    }

    public String getSensorId() {
        return sensorId;
    }

    public void setSensorId(String sensorId) {
        this.sensorId = sensorId;
    }

    public Integer getActiveWorkstations() {
        return activeWorkstations;
    }

    public void setActiveWorkstations(Integer activeWorkstations) {
        this.activeWorkstations = activeWorkstations;
    }

    public Double getTemperatureCelsius() {
        return temperatureCelsius;
    }

    public void setTemperatureCelsius(Double temperatureCelsius) {
        this.temperatureCelsius = temperatureCelsius;
    }

    public Double getCpuUtilizationPercent() {
        return cpuUtilizationPercent;
    }

    public void setCpuUtilizationPercent(Double cpuUtilizationPercent) {
        this.cpuUtilizationPercent = cpuUtilizationPercent;
    }

    public Double getPowerConsumptionWatts() {
        return powerConsumptionWatts;
    }

    public void setPowerConsumptionWatts(Double powerConsumptionWatts) {
        this.powerConsumptionWatts = powerConsumptionWatts;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LabTelemetryDto{" +
                "metricId='" + metricId + '\'' +
                ", labCode='" + labCode + '\'' +
                ", sensorId='" + sensorId + '\'' +
                ", activeWorkstations=" + activeWorkstations +
                ", temperatureCelsius=" + temperatureCelsius +
                ", cpuUtilizationPercent=" + cpuUtilizationPercent +
                ", powerConsumptionWatts=" + powerConsumptionWatts +
                ", timestamp=" + timestamp +
                '}';
    }
}
