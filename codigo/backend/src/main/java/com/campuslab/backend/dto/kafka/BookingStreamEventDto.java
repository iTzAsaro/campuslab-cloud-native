package com.campuslab.backend.dto.kafka;

import java.io.Serializable;
import java.time.Instant;

/**
 * DTO para el streaming de eventos del ciclo de vida de reservas de laboratorios.
 * Tópico Kafka: campuslab.bookings.stream
 * Partition Key sugerida: labId (garantiza orden estricto de eventos por laboratorio).
 */
public class BookingStreamEventDto implements Serializable {

    private static final long serialVersionUID = 1L;

    private String eventId;
    private Long bookingId;
    private String labId;
    private String studentEmail;
    private String action; // RESERVATION_CREATED, CHECK_IN, SESSION_COMPLETED, RESERVATION_CANCELLED
    private Integer durationMinutes;
    private Integer workstationCount;
    private Instant timestamp;

    public BookingStreamEventDto() {
        this.timestamp = Instant.now();
    }

    public BookingStreamEventDto(String eventId, Long bookingId, String labId, String studentEmail,
                                 String action, Integer durationMinutes, Integer workstationCount) {
        this.eventId = eventId;
        this.bookingId = bookingId;
        this.labId = labId;
        this.studentEmail = studentEmail;
        this.action = action;
        this.durationMinutes = durationMinutes;
        this.workstationCount = workstationCount;
        this.timestamp = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getBookingId() {
        return bookingId;
    }

    public void setBookingId(Long bookingId) {
        this.bookingId = bookingId;
    }

    public String getLabId() {
        return labId;
    }

    public void setLabId(String labId) {
        this.labId = labId;
    }

    public String getStudentEmail() {
        return studentEmail;
    }

    public void setStudentEmail(String studentEmail) {
        this.studentEmail = studentEmail;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Integer getDurationMinutes() {
        return durationMinutes;
    }

    public void setDurationMinutes(Integer durationMinutes) {
        this.durationMinutes = durationMinutes;
    }

    public Integer getWorkstationCount() {
        return workstationCount;
    }

    public void setWorkstationCount(Integer workstationCount) {
        this.workstationCount = workstationCount;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "BookingStreamEventDto{" +
                "eventId='" + eventId + '\'' +
                ", bookingId=" + bookingId +
                ", labId='" + labId + '\'' +
                ", studentEmail='" + studentEmail + '\'' +
                ", action='" + action + '\'' +
                ", durationMinutes=" + durationMinutes +
                ", workstationCount=" + workstationCount +
                ", timestamp=" + timestamp +
                '}';
    }
}
