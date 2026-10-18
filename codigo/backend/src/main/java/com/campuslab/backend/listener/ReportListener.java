package com.campuslab.backend.listener;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class ReportListener {

    private static final Logger log = LoggerFactory.getLogger(ReportListener.class);

    @RabbitListener(queues = "${campuslab.rabbitmq.queues.reports:campuslab.reports.generation}")
    public void processReport(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) {
        try {
            log.info("[DOMINIO REPORTES] Iniciando generación de reporte: {}", message);
            // Procesamiento asíncrono
            channel.basicAck(tag, false);
            log.info("[DOMINIO REPORTES] Reporte generado exitosamente. ACK enviado para tag={}", tag);
        } catch (Exception e) {
            log.error("[DOMINIO REPORTES] Fallo en generación de reporte: {}. Enviando a DLQ.", e.getMessage());
            try {
                channel.basicNack(tag, false, false);
            } catch (Exception nackEx) {
                log.error("[DOMINIO REPORTES] Error al ejecutar basicNack", nackEx);
            }
        }
    }
}
