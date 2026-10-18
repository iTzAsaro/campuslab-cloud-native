package com.campuslab.backend.listener;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class NotificationListener {

    private static final Logger log = LoggerFactory.getLogger(NotificationListener.class);

    @RabbitListener(queues = "${campuslab.rabbitmq.queues.notifications:campuslab.booking.notifications}")
    public void processNotification(String message, Channel channel, @Header(AmqpHeaders.DELIVERY_TAG) long tag) {
        try {
            log.info("[DOMINIO NOTIFICACIONES] Recibiendo mensaje: {}", message);

            // Simulación de error controlado para demostración de Dead Letter Queue (DLQ)
            if (message != null && message.toLowerCase().contains("error")) {
                throw new RuntimeException("Error intencional en procesamiento de notificación");
            }

            // Confirmación explícita (ACK)
            channel.basicAck(tag, false);
            log.info("[DOMINIO NOTIFICACIONES] Notificación procesada con éxito. ACK enviado para tag={}", tag);
        } catch (Exception e) {
            log.error("[DOMINIO NOTIFICACIONES] Error procesando notificación: {}. Ejecutando NACK -> Redirigiendo a DLQ.", e.getMessage());
            try {
                // requeue = false para forzar que el broker lo transfiera a la DLQ
                channel.basicNack(tag, false, false);
            } catch (Exception nackEx) {
                log.error("[DOMINIO NOTIFICACIONES] Error crítico al ejecutar basicNack", nackEx);
            }
        }
    }
}
