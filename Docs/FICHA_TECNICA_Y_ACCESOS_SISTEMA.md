# Ficha Técnica y Matriz Maestra de Accesos - CampusLab Cloud Native
## Sistema de Streaming y Gestión de Laboratorios Universitarios (EP1 a EP6)

> **Asignatura:** Desarrollo Cloud Native I (`DSY1107`) — Duoc UC  
> **Estudiante / Desarrollador:** `@iTzAsaro`  
> **Repositorio Oficial:** [`https://github.com/iTzAsaro/campuslab-cloud-native`](https://github.com/iTzAsaro/campuslab-cloud-native)  
> **Fecha de Actualización:** Noviembre 2026  

---

## 🌐 1. Tabla Maestra de Accesos, URLs e IPs (Producción AWS vs. Entorno Local)

| Componente / Servicio | Entorno AWS Cloud (Producción) | Entorno Local (Docker / Dev) | Credenciales / Protocolo |
|---|---|---|---|
| **Frontend Web (UI)** | [https://52-200-87-76.sslip.io](https://52-200-87-76.sslip.io) *(o http://52.200.87.76:4200)* | [http://localhost:4200](http://localhost:4200) | HTTPS (Caddy SSL automático) / Angular 19 |
| **API Gateway AWS** | [https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api](https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api) | [http://localhost:8080/api](http://localhost:8080/api) | HTTP Proxy Integration a EC2 / CORS universal |
| **Backend Spring Boot** | `http://52.200.87.76:8080/api` *(vía Gateway o directo)* | `http://localhost:8080/api` | Java 21 / Spring Boot 3.3.4 (systemd en EC2) |
| **Kafka-UI (Brokers & Topics)** | [http://52.200.87.76:8085](http://52.200.87.76:8085) | [http://localhost:8085](http://localhost:8085) | Sin autenticación / Gestión visual en vivo |
| **RabbitMQ Nodo 1 (Mgmt)** | `http://52.200.87.76:15672` | `http://localhost:15672` | Usuario: `admin` \| Clave: `admin` |
| **RabbitMQ Nodo 2 (Mgmt)** | `http://52.200.87.76:15673` | `http://localhost:15673` | Usuario: `admin` \| Clave: `admin` |
| **PostgreSQL Database** | Host: `52.200.87.76:5432` *(interno en EC2)* | Host: `localhost:5433` *(Docker)* | BD: `campuslab` (EC2) / `appdb` (local) \| User: `campuslab` / `admin` |
| **Apache Kafka Broker** | `localhost:29092` *(host EC2)* \| `kafka1:9092` *(docker)* | `localhost:29092` \| `kafka1:9092` | PLAINTEXT / Sin TLS para baja latencia interna |
| **ZooKeeper** | `localhost:2181` | `localhost:2181` | Quorum Leader/Follower |

---

## ☁️ 2. Ficha de Infraestructura Cloud en AWS

### Instancia EC2
- **ID de Instancia:** `i-00605e0ac85776d8b`
- **Nombre (Tag):** `CampusLab-Backend`
- **Tipo de Instancia:** `t3.small` (2 vCPUs, 2.0 GiB RAM, 2.0 GiB Swap)
- **Zona de Disponibilidad:** `us-east-1f`
- **IP Pública Elástica / DNS:** `52.200.87.76` (`ec2-52-200-87-76.compute-1.amazonaws.com`)
- **IP Privada VPC:** `172.31.68.99`
- **Security Group:** `sg-076cfc00b2d5a1798` (`campuslab-sg`)
  - **Puerto 22 (SSH):** Autorizado para IP del desarrollador.
  - **Puerto 80 / 443 (HTTP/HTTPS):** Abierto a `0.0.0.0/0` (Caddy reverse proxy con certificados Let's Encrypt para `52-200-87-76.sslip.io`).
  - **Puerto 4200 (Frontend Web):** Abierto a `0.0.0.0/0` (Nginx container).
  - **Puerto 8080 (Spring Boot API):** Abierto a `0.0.0.0/0` (integración con AWS API Gateway).
  - **Puerto 8085 (Kafka-UI):** Abierto a `0.0.0.0/0` (dashboard visual).
  - **Puertos 5672, 15672, 5673, 15673:** RabbitMQ clúster y consola de administración.
- **Servicio Systemd del Backend:**
  - Archivo: `/etc/systemd/system/campuslab-backend.service`
  - Jar: `/opt/campuslab/backend.jar` (Java 21 con `-Xms256m -Xmx384m`)

### AWS API Gateway (HTTP API)
- **API ID:** `hygoaj1glh`
- **Nombre:** `mi_api_gateway`
- **Endpoint Base:** `https://hygoaj1glh.execute-api.us-east-1.amazonaws.com`
- **Etapa:** `$default` (con `AutoDeploy: true`)
- **Configuración CORS:**
  - `AllowOrigins`: `["*"]`
  - `AllowMethods`: `["GET", "POST", "PUT", "DELETE", "OPTIONS"]`
  - `AllowHeaders`: `["authorization", "content-type", "x-amz-date", "x-api-key"]`
  - `MaxAge`: `86400`
- **Rutas Principales:**
  - `ANY /api/{proxy+}` ➔ Integración HTTP Proxy a `http://52.200.87.76:8080/api/{proxy}`
  - `GET /api/health` ➔ Integración HTTP Proxy a `http://52.200.87.76:8080/api/health`
  - `OPTIONS /api/{proxy+}` ➔ Preflight CORS habilitado

---

## 🔐 3. Identidad y Seguridad (Microsoft Entra ID / IDaaS)

- **Proveedor:** Microsoft Azure Entra ID
- **Tenant ID (Directorio):** `4b9ee4c5-a799-40af-88fe-81d4ef8aaba3`
- **Application / Client ID:** `618a6485-6e73-4faf-803c-8e120dd362f4`
- **Issuer URI (OIDC Discovery):**  
  `https://login.microsoftonline.com/4b9ee4c5-a799-40af-88fe-81d4ef8aaba3/v2.0`
- **Scopes Solicitados:** `openid profile email`
- **Flujo de Autorización:** Authorization Code Flow con PKCE (`S256`)
- **Redirect URIs Autorizadas:**
  - `https://52-200-87-76.sslip.io/dashboard` *(Producción Cloud)*
  - `http://localhost:4200/dashboard` *(Desarrollo Local)*
- **Filtro Backend:** `SecurityConfig.java` configurado como Resource Server OAuth2 validando el token Bearer JWT con clave pública de Microsoft.

---

## 📊 4. Apache Kafka: Tópicos, Particiones y Consumer Groups

### Tópicos Especializados
| Nombre del Tópico | Particiones | Replicación | Retención | Propósito / Evento |
|---|:---:|:---:|:---:|---|
| **`campuslab.bookings.stream`** | 3 | 1 | 7 días (`604800000 ms`) | Eventos de ciclo de vida de reservas (`RESERVATION_CREATED`, `CHECK_IN`, `SESSION_COMPLETED`). Partition Key: `labId`. |
| **`campuslab.telemetry.metrics`** | 3 | 1 | 24 horas (`86400000 ms`) | Telemetría continua IoT de sensores (temperatura, potencia en Watts, puestos activos). Partition Key: `labCode`. |
| **`campuslab.bookings.stream.DLT`** | 3 | 1 | 14 días (`1209600000 ms`) | Dead Letter Topic. Almacena mensajes que fallaron tras reintentos con cabeceras de excepción y stacktrace. |

### Consumer Groups
| Consumer Group ID | Tópico que Escucha | Concurrencia | Listener Java |
|---|---|:---:|---|
| **`campuslab-analytics-group`** | `campuslab.bookings.stream` | 3 hilos | `AnalyticsStreamListener.java` |
| **`campuslab-telemetry-group`** | `campuslab.telemetry.metrics` | 3 hilos | `LabTelemetryListener.java` |
| **`campuslab-dlt-audit-group`** | `campuslab.bookings.stream.DLT` | 3 hilos | `KafkaDltListener.java` |

### Política de Resiliencia
- **Manejador de Errores:** `DefaultErrorHandler` con `FixedBackOff(1000L, 2)`.
- **Comportamiento:** Si un mensaje lanza excepción no recuperable, ejecuta exactamente **2 reintentos con 1 segundo de espera**. Si continúa fallando, `DeadLetterPublishingRecoverer` lo publica automáticamente en `campuslab.bookings.stream.DLT`.

---

## 🐇 5. Clúster RabbitMQ: Exchanges, Colas y DLQ

| Objeto AMQP | Tipo / Nombre | Enrutamiento (Routing Key) | Política / Resiliencia |
|---|---|---|---|
| **Exchange Direct** | `campuslab.direct` | `routing.notifications` | Notificaciones transaccionales a estudiantes con ACK manual. |
| **Exchange Topic** | `campuslab.topic` | `routing.reports.generate`, `routing.audit.*` | Enrutamiento de reportes y logs de auditoría. |
| **Cola Notificaciones** | `campuslab.booking.notifications` | `routing.notifications` | `x-dead-letter-exchange: campuslab.dlx` |
| **Dead Letter Exchange (DLX)**| `campuslab.dlx` (Fanout) | Broadcast | Recibe mensajes con `basicNack(requeue=false)` |
| **Dead Letter Queue (DLQ)** | `campuslab.booking.notifications.dlq` | — | Almacena mensajes fallidos para auditoría forense |

---

## ⚡ 6. Cheatsheet de Comandos cURL para Pruebas en Vivo (Cloud AWS)

### 1. Health Check
```bash
curl -s https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/health
```

### 2. Kafka: Listar Tópicos
```bash
curl -s https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/admin/kafka/topics
```

### 3. Kafka: Listar Consumer Groups
```bash
curl -s https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/admin/kafka/consumer-groups
```

### 4. Kafka: Publicar Ráfaga de 10 Eventos Particionados
```bash
curl -s -X POST https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/stream/kafka/simulate-load
```

### 5. Kafka: Enviar Métrica de Sensor IoT
```bash
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"labId":"LAB-CLOUD-01","temperature":23.5,"powerWatts":1920.0,"activeWorkstations":16}' \
  https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/stream/kafka/telemetry
```

### 6. Kafka: Inyectar Error y Activar Dead Letter Topic (DLT)
```bash
curl -s -X POST https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/stream/kafka/trigger-dlt-error
```

### 7. Kafka: Consultar Motor Analítico en Tiempo Real
```bash
curl -s https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/stream/kafka/analytics
```

### 8. Kafka Admin: Crear y Eliminar Tópico Dinámicamente
```bash
# Crear
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"name":"campuslab.demo.topic","partitions":3,"retentionHours":24}' \
  https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/admin/kafka/topics

# Eliminar
curl -s -X DELETE https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/admin/kafka/topics/campuslab.demo.topic
```

### 9. RabbitMQ: Notificación Transaccional (Direct Exchange)
```bash
curl -s -X POST -H "Content-Type: application/json" \
  -d '{"userEmail":"estudiante@campuslab.cl","message":"Confirmacion de reserva de laboratorio","type":"BOOKING_CONFIRMATION"}' \
  https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/events/notification
```

### 10. RabbitMQ: Probar Desvío a DLQ con basicNack
```bash
curl -s -X POST https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api/events/notification/error-test
```
