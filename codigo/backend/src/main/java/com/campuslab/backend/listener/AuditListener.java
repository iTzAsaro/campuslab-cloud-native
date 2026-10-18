package com.campuslab.backend.listener;

import com.campuslab.backend.model.AuditLog;
import com.campuslab.backend.repository.AuditLogRepository;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class AuditListener {

    private static final Logger log = LoggerFactory.getLogger(AuditListener.class);
    private final AuditLogRepository auditLogRepository;

    public AuditListener(AuditLogRepository auditLogRepository) {
        this.auditLogRepository = auditLogRepository;
    }

    @RabbitListener(queues = "${campuslab.rabbitmq.queues.audit:campuslab.audit.events}")
    public void processAudit(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) {
        try {
            log.info("[DOMINIO AUDITORÍA] Registrando evento: {}", message);

            // Persistir en Base de Datos Cloud (PostgreSQL)
            AuditLog auditLog = new AuditLog("AUDIT_EVENT", message, "campuslab.audit.events");
            auditLogRepository.save(auditLog);

            channel.basicAck(tag, false);
            log.info("[DOMINIO AUDITORÍA] Evento persistido y ACK enviado para tag={}", tag);
        } catch (Exception e) {
            log.error("[DOMINIO AUDITORÍA] Fallo registrando auditoría: {}. Enviando a DLQ.", e.getMessage());
            try {
                channel.basicNack(tag, false, false);
            } catch (Exception nackEx) {
                log.error("[DOMINIO AUDITORÍA] Error al ejecutar basicNack", nackEx);
            }
        }
    }
}
