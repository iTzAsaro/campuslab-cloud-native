# Guía de Defensa Técnica y Demostración en Vivo - EP5 y EP6
## Plataforma CampusLab: Streaming y Analítica en Tiempo Real con Apache Kafka

**Asignatura:** Desarrollo Cloud Native I (`DSY1107`)  
**Evaluación:** EP5 (Encargo) & EP6 (Defensa Técnica y Presentación)  
**Duración Sugerida de Exposición:** 5 a 10 minutos  

---

## 🎯 1. Estructura de la Presentación (Guión Minuto a Minuto)

| Minuto | Tema Clave | Qué Decir y Enfatizar | Apoyo Visual |
|:---:|---|---|---|
| **0:00 - 1:30** | **Continuidad Arquitectónica y Problema de Negocio** | Explicar cómo el sistema **CampusLab** evoluciona desde la EP1/EP2 (Spring Boot, Angular, Entra ID) y EP3/EP4 (RabbitMQ para tareas y notificaciones) hacia un motor de **Streaming y Analítica Continua con Apache Kafka**. | Diagrama de arquitectura general en el `README.md`. |
| **1:30 - 3:30** | **Justificación de Coexistencia: RabbitMQ vs. Kafka** | Demostrar por qué NO se reemplazó RabbitMQ: RabbitMQ maneja la mensajería transaccional punto a punto (notificaciones y reportes con ACK manual), mientras que Kafka maneja flujos inmutables de alta concurrencia (telemetría de sensores y eventos de reserva para analítica de ocupación). | Matriz comparativa y diagrama de brokers. |
| **3:30 - 6:30** | **Demostración en Vivo (Live Demo)** | Ejecutar el envío de eventos de streaming, mostrar la distribución en particiones en **Kafka-UI** (`http://localhost:8085`), demostrar la resiliencia con el Dead Letter Topic (`.DLT`), y mostrar el microservicio de administración dinámica de tópicos. | Dashboard Angular y Kafka-UI en vivo. |
| **6:30 - 8:00** | **Resiliencia, Particionado y Consumer Groups** | Explicar la política de particionado por `labId` (garantiza orden estricto por laboratorio), el uso de Consumer Groups balanceados (`campuslab-analytics-group`), y la configuración de `DefaultErrorHandler` con reintentos `FixedBackOff` hacia `.DLT`. | Código de `KafkaConfig.java` y logs de consola. |
| **8:00 - 10:00** | **Preguntas de la Comisión y Cierre** | Respuestas técnicas directas usando el banco de preguntas preparadas a continuación. | Código fuente y métricas en vivo. |

---

## 💻 2. Guión Paso a Paso para la Demostración en Vivo

> [!NOTE]
> La demostración se puede realizar tanto en **Entorno Local** como en **AWS Cloud Producción**:
> - **Frontend en Producción Cloud (con TLS/SSL):** `https://52-200-87-76.sslip.io` (o Local: `http://localhost:4200`)
> - **Kafka-UI en la Nube:** `http://52.200.87.76:8085` (o Local: `http://localhost:8085`)
> - **AWS API Gateway:** `https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api` (o Local: `http://localhost:8080/api`)
> - **RabbitMQ Console:** `http://52.200.87.76:15672` (o Local: `http://localhost:15672`)

### Paso 1: Verificación de Contenedores y Servicios
Mostrar al evaluador que el stack completo está arriba:
- En Cloud (EC2): Los servicios de streaming (`zookeeper`, `kafka1`, `kafka-ui`, `rabbitmq1`, `rabbitmq2`, `campuslab_frontend`, `campuslab_ssl`) y el microservicio backend nativo en systemd están activos.
- En Local (Docker Compose):
```bash
cd codigo
docker compose ps
```
*Señalar:* Se observan `zookeeper`, `kafka1`, `kafka-ui`, `rabbitmq1`, `rabbitmq2`, `postgres`, `backend` y `frontend`.

### Paso 2: Apertura de Kafka-UI
1. Abrir navegador en `http://52.200.87.76:8085` (Cloud) o `http://localhost:8085` (Local).
2. Mostrar los tópicos configurados automáticamente al arrancar Spring Boot:
   - `campuslab.bookings.stream` (3 particiones, factor de replicación 1, retención 7 días).
   - `campuslab.telemetry.metrics` (3 particiones, retención 24 horas).
   - `campuslab.bookings.stream.DLT` (Dead Letter Topic).

### Paso 3: Disparo de Eventos desde el Frontend Angular
1. Ingresar al Frontend en `http://localhost:4200`.
2. En la sección **"01 // STREAMING Y ANALÍTICA CONTINUA (APACHE KAFKA)"**:
   - Presionar **`> PUBLICAR EVENTO STREAMING (RESERVA)`**: Muestra en la consola de salida que el evento fue recibido y encolado en `campuslab.bookings.stream`.
   - Presionar **`> TRANSMITIR MÉTRICA SENSOR (IOT)`**: Muestra la actualización en vivo de temperatura y potencia en las tarjetas superiores.
   - Presionar **`> DISPARAR RÁFAGA (10 EVENTOS)`**: Muestra cómo los 10 eventos se reparten entre las 3 particiones del tópico gracias a la partition key `labId`.
3. Volver a **Kafka-UI** y mostrar en la pestaña *Messages* que los mensajes están distribuidos en las particiones 0, 1 y 2.

### Paso 4: Demostración de Resiliencia y Dead Letter Topic (DLT)
1. En el Frontend, presionar el botón rojo: **`> INYECTAR FALLO Y ACTIVAR DLT`**.
2. Explicar lo que ocurre internamente:
   - El productor envía un payload con correo `trigger-dlt-error@campuslab.cl`.
   - El consumidor `AnalyticsStreamListener` detecta la condición y lanza una excepción.
   - El `DefaultErrorHandler` configurado en `KafkaConfig.java` intercepta el fallo.
   - Aplica la política de reintentos: 2 intentos con intervalo de 1000 ms (`FixedBackOff`).
   - Al agotarse los reintentos, el `DeadLetterPublishingRecoverer` desvía el mensaje al tópico `campuslab.bookings.stream.DLT`.
   - El consumidor de auditoría `KafkaDltListener` captura el mensaje muerto y lo registra en el log forense, incrementando el contador *"Mensajes Derivados a DLT"*.
3. Mostrar en Kafka-UI que el mensaje corrupto se encuentra ahora en el tópico `.DLT`.

### Paso 5: Microservicio Administrador de Kafka (API Dinámica)
1. En el Frontend, sección **"02 // MICROSERVICIO ADMINISTRADOR DE KAFKA"**:
   - Crear un tópico dinámico (ej: `campuslab.nuevo.laboratorio`, 3 particiones, cleanup: `delete`).
   - Presionar **`> POST /api/admin/kafka/topics`**: Mostrar la confirmación del `AdminClient`.
   - Presionar **`> LISTAR TÓPICOS`**: Comprobar que aparece en la lista devuelta por el backend.
   - Presionar **`> VER CONSUMER GROUPS`**: Mostrar los 3 grupos activos (`campuslab-analytics-group`, `campuslab-telemetry-group`, `campuslab-dlt-audit-group`).
2. Mostrar en Kafka-UI que el nuevo tópico apareció inmediatamente en el clúster.

### Paso 6: Prueba de Regresión de RabbitMQ (Continuidad)
1. En la sección **"03 // MENSAJERÍA TRANSACCIONAL RABBITMQ"**:
   - Presionar **`> DISPARAR NOTIFICACIÓN (ACK)`** y **`> SOLICITAR REPORTE (ACK)`**.
   - Comprobar que RabbitMQ sigue respondiendo con normalidad y con confirmación manual de consumo (ACK explícito).

---

## ❓ 3. Banco de Preguntas Frecuentes de la Comisión Evaluadora

### P1: ¿Por qué no usar RabbitMQ para todo o Kafka para todo? ¿Por qué ambos?
> **Respuesta:** RabbitMQ y Apache Kafka resuelven problemas arquitectónicos fundamentalmente distintos.  
> **RabbitMQ** está optimizado para mensajería de tareas y transacciones puntuales (*smart broker / dumb consumer*), donde cada mensaje debe ser enrutado con flexibilidad compleja (bindings directos o por tópicos), confirmado con ACK manual y eliminado de inmediato de la cola para liberar recursos.  
> **Apache Kafka**, en cambio, es un log distribuido inmutable de eventos (*dumb broker / smart consumer*). Mantiene el historial en disco según la política de retención (7 días en reservas, 24h en telemetría), permitiendo que múltiples grupos de consumidores lean a su propio ritmo sin interferir entre sí y que nuevos servicios puedan reprocesar eventos pasados (*event replayability*). Usar Kafka para notificaciones simples sobrecarga la arquitectura; usar RabbitMQ para streaming masivo de sensores satura la memoria del broker.

### P2: ¿Cómo garantizan el orden de los mensajes en Kafka si tienen múltiples particiones?
> **Respuesta:** En Kafka, el orden de entrega está estrictamente garantizado **únicamente dentro de una misma partición**. Por ello, nuestro productor (`KafkaProducerService`) utiliza como **Partition Key** el identificador del recurso (el `labId` para reservas o el `labCode` para telemetría). De este modo, la función de hash garantiza que todos los eventos correspondientes a un mismo laboratorio caen siempre en la misma partición física, procesándose en orden cronológico exacto, mientras que el balanceo concurrente se mantiene entre distintos laboratorios.

### P3: ¿Cómo manejan los mensajes malformados o que producen excepciones en el consumidor?
> **Respuesta:** Implementamos un patrón de resiliencia con `DefaultErrorHandler` de Spring Kafka combinado con `DeadLetterPublishingRecoverer`. Si un mensaje produce una excepción durante su deserialización o procesamiento de dominio, se ejecuta una política de reintento (`FixedBackOff` de 2 reintentos con intervalo de 1 segundo). Si el fallo persiste, el recuperador desvía automáticamente el mensaje hacia el Dead Letter Topic (`campuslab.bookings.stream.DLT`), evitando que el hilo del consumidor quede bloqueado y permitiendo la auditoría forense posterior mediante `KafkaDltListener`.

### P4: ¿Qué parámetros de configuración de tópicos fueron definidos y por qué?
> **Respuesta:**  
> - **Particiones:** Configuramos 3 particiones para permitir procesamiento en paralelo y distribución de carga entre instancias de consumidores.  
> - **Factor de replicación:** Definido en 1 para el entorno local (y configurable a 3 en producción con clúster multi-broker).  
> - **`retention.ms`:** 7 días (`604800000 ms`) para `campuslab.bookings.stream` (permite recalcular analítica semanal) y 24 horas (`86400000 ms`) para `campuslab.telemetry.metrics` (telemetría operacional de alta frecuencia).  
> - **`cleanup.policy`:** `delete` para descartar segmentos que superan la ventana de retención.
