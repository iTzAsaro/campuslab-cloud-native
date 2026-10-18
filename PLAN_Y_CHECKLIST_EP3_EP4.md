# Plan de Trabajo y Checklist - EP3 y EP4 (100% de la Rúbrica)

Este documento contiene el plan de implementación y el checklist detallado extraído de las rúbricas de Evaluación Parcial N°3 (Encargo) y N°4 (Presentación) de Desarrollo Cloud Native I.

## 1. Plan de Trabajo

### Fase 1: Infraestructura y Entorno Cloud
- **Docker Compose**: Crear archivo para levantar 2 nodos de RabbitMQ en clúster.
- **API Manager**: Crear instancia en la nube y configurar enrutamiento hacia el backend. Configurar validación de token JWT a nivel del API Manager.
- **IDaaS**: Configurar tenant, registrar usuarios y habilitar flujo OAuth 2.0 / OpenID Connect.
- **Base de Datos Cloud**: Proveer instancia y asegurar credenciales para el backend.

### Fase 2: Desarrollo Frontend (Angular)
- Configurar autenticación contra IDaaS (Login, obtener JWT).
- Enviar peticiones al backend pasando por el API Manager (inyectar JWT en Headers).
- Crear vistas funcionales, asegurar modularidad y eliminar errores de compilación.
- Configurar `.gitignore` adecuado.

### Fase 3: Desarrollo Backend Base (Java + Spring Boot)
- Estructurar microservicios.
- Integrar Base de Datos Cloud (Entidades, Repositorios, Propiedades).
- Implementar filtros de seguridad para validar JWT emitido por IDaaS.
- Configurar `.gitignore` adecuado.

### Fase 4: Mensajería Asíncrona (RabbitMQ) - Core EP3/EP4
- **Configuración Centralizada**: Crear `application.yml` o clase `@Configuration` con todos los nombres de colas, exchanges y bindings. No usar *hardcoding*.
- **Declaración de Beans**: Configurar Beans (`Queue`, `Exchange` (direct y topic), `Binding`) para cada caso de uso.
- **Productores y Consumidores**: 
  - Usar `@RabbitListener` agrupados por dominio. 
  - Implementar ACK explícito.
  - Implementar manejo de errores, NACK y DLQ (Dead Letter Queue) con logs.
- **Microservicio Administrador**:
  - API REST limpia (`POST`, `DELETE` de colas y exchanges).
  - Encapsular lógica en `RabbitAdminService`.
  - Validar parámetros de entrada mediante DTOs.

### Fase 5: Pruebas, Despliegue y Preparación de Presentación (EP4)
- Desplegar Backend, Frontend, API Manager y RabbitMQ en la nube.
- Probar flujo completo: IDaaS -> Frontend -> API Manager -> Backend -> Base de Datos / RabbitMQ.
- Preparar demostración en vivo (5 a 10 min).

---

## 2. Checklist para lograr 100% (Rúbricas EP3 y EP4)

### Infraestructura & Despliegue (Nube / Docker)
- [x] Dockerfile de Backend y Frontend listos para despliegue en contenedores.
- [x] `docker-compose.yml` multi-servicio configurado con clúster RabbitMQ (2 nodos), PostgreSQL, Backend y Frontend.
- [x] Archivos `.gitignore` configurados en raíz, backend y frontend.
- [x] Guía completa de presentación y defensa cloud documentada en `Docs/GUIA_DEFENSA_Y_DEMOSTRACION_EP4.md`.
- [ ] Despliegue en VM de nube (AWS / Azure) ejecutando `docker compose up -d`.
- [ ] Configuración del API Manager en la nube (AWS API Gateway / Azure APIM) apuntando a la IP pública del backend.
- [ ] Configuración del tenant IDaaS (Azure Entra ID / Auth0) y usuarios registrados.

### Seguridad y Autenticación
- [x] Flujo Login Frontend: Implementa OAuth 2.0/OpenID Connect contra IDaaS y obtiene JWT (`angular-oauth2-oidc`).
- [x] Interceptor HTTP de Angular inyecta automáticamente el token Bearer JWT en todas las peticiones salientes.
- [x] Backend incluye filtro de seguridad OAuth2 Resource Server para validar JWT emitido por IDaaS.
- [x] Configuración de CORS habilitada en backend para comunicación fluida con el frontend.

### Frontend (Angular)
- [x] Código modular, standalone y sin errores de compilación (`ng build` pasa al 100% en 3.2s).
- [x] Vistas 100% funcionales: Panel de control interactivo con disparadores para Notificaciones (Direct), Reportes (Topic), Auditoría (Topic) y prueba de fallo DLQ.
- [x] Interfaz gráfica para interactuar con el microservicio administrador (crear y eliminar colas y exchanges).
- [x] Pruebas unitarias de frontend configuradas y pasando al 100% (`ng test` OK).

### Backend - Arquitectura y DB (Spring Boot 3.3.4 + Java 21)
- [x] Compila sin errores y responde a suite completa de pruebas unitarias y de integración (`./gradlew test` pasa al 100%).
- [x] Integración con BD Cloud (PostgreSQL) configurada mediante Entidades JPA (`Booking`, `AuditLog`), Repositorios (`BookingRepository`, `AuditLogRepository`) y `application.yml`.
- [x] Wrapper de Gradle (`./gradlew`) generado e incluido para portabilidad total.

### Mensajería RabbitMQ (Microservicios Regulares)
- [x] Nombres centralizados: Todas las colas, exchanges y bindings en `application.yml` y mapeados con `@Value` en `RabbitMQConfig`. Cero valores sueltos en el código (12%).
- [x] Separación de configuración: `RabbitMQConfig` dedicada creando Beans para `DirectExchange`, `TopicExchange`, `Queue`, `Binding` y `SimpleRabbitListenerContainerFactory` (13%).
- [x] Separación de responsabilidades: El código de negocio no tiene lógica de configuración de infraestructura (10%).
- [x] Topología implementada: 3 colas de mensajes funcionando (`campuslab.booking.notifications`, `campuslab.reports.generation`, `campuslab.audit.events`).
- [x] Topología implementada: 3 colas DLQ asociadas y operativas (`.dlq`).
- [x] Topología implementada: Exchanges tipo `direct` y `topic` correctamente configurados con sus bindings.
- [x] Consumidores: Implementados con `@RabbitListener` y agrupados por dominio funcional (`NotificationListener`, `ReportListener`, `AuditListener`) (15%).
- [x] Confiabilidad: Productores (`MessageProducerService`) y consumidores operativos en el sistema.
- [x] Confiabilidad: Manejo explícito de confirmación (ACK) en consumidores mediante `channel.basicAck(tag, false)` (20%).
- [x] Confiabilidad: Manejo explícito de errores (NACK) redirigiendo a DLQ mediante `channel.basicNack(tag, false, false)` y registro detallado en logs (20%).

### Mensajería RabbitMQ (Microservicio Administrador)
- [x] Microservicio administrador creado y funcional (`RabbitAdminController`).
- [x] Endpoints REST definidos correctamente y coherentes (`POST /api/admin/queues`, `DELETE /api/admin/queues/{name}`, `POST /api/admin/exchanges`, `DELETE /api/admin/exchanges/{name}`, `POST /api/admin/bindings`, `DELETE /api/admin/bindings`) (13%).
- [x] Lógica encapsulada en una clase de servicio (`RabbitAdminService`) (10%).
- [x] Controladores llaman exclusivamente al servicio, sin interactuar directamente con librerías de RabbitMQ (10%).
- [x] Validación de DTOs: Parámetros de entrada validados con Bean Validation (`@NotBlank`, `@Pattern`, `@Size`) impidiendo nombres vacíos o configuraciones inválidas con mensajes claros (7%).

### Presentación y Defensa (EP4)
- [x] Guión estructurado de 5 a 10 minutos con marcas de tiempo redactado en `Docs/GUIA_DEFENSA_Y_DEMOSTRACION_EP4.md`.
- [x] Diagrama de arquitectura de flujo completo (Mermaid) documentado.
- [x] Matriz de respuestas a preguntas difíciles del profesor sobre ACK manual, DLQ, Topic vs Direct, y Zero Trust.
- [x] Procedimiento de demostración paso a paso en consola de RabbitMQ y UI.
