import { Component, inject, signal, OnInit } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { JsonPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AuthService } from '../../core/auth/auth.service';
import { environment } from '../../../environments/environment';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [JsonPipe, FormsModule],
  template: `
    <div class="dashboard-container">
      <header class="dashboard-header">
        <div>
          <h1 class="dashboard-title">CAMPUSLAB // PANEL DE CONTROL UNIFICADO</h1>
          <p class="dashboard-subtitle">
            Arquitectura Cloud Native Dual: RabbitMQ (Transaccional & DLQ) + Apache Kafka (Streaming & Analítica)
          </p>
        </div>
        <button class="btn-logout" (click)="onLogout()">[ CERRAR SESIÓN ]</button>
      </header>

      <!-- RESUMEN ANALÍTICO EN TIEMPO REAL (KAFKA STREAMING ENGINE) -->
      @if (analyticsSummary()) {
        <section class="section metrics-section">
          <h2 class="section-title">📊 // MOTOR ANALÍTICO KAFKA EN VIVO (MÉTRICAS AGREGADAS)</h2>
          <div class="metrics-grid">
            <div class="metric-card">
              <span class="metric-value">{{ analyticsSummary()?.totalBookingsProcessed || 0 }}</span>
              <span class="metric-label">Reservas Procesadas (Streaming)</span>
            </div>
            <div class="metric-card">
              <span class="metric-value">{{ analyticsSummary()?.totalTelemetryPointsProcessed || 0 }}</span>
              <span class="metric-label">Puntos Telemetría IoT</span>
            </div>
            <div class="metric-card alert">
              <span class="metric-value text-danger">{{ analyticsSummary()?.totalDltAlerts || 0 }}</span>
              <span class="metric-label">Mensajes Derivados a DLT</span>
            </div>
            <div class="metric-card highlight">
              <span class="metric-value text-cyan">{{ getActiveWorkstationsCount() }}</span>
              <span class="metric-label">Puestos Activos Detectados</span>
            </div>
          </div>
        </section>
      }

      <!-- SECCIÓN 1: APACHE KAFKA - STREAMING Y ANALÍTICA (EP5/EP6) -->
      <section class="section">
        <h2 class="section-title">01 // STREAMING Y ANALÍTICA CONTINUA (APACHE KAFKA)</h2>
        <div class="grid">
          <!-- Disparar Evento Reserva Kafka -->
          <div class="ryoku-card">
            <div class="card-header">KAFKA // CICLO DE VIDA RESERVAS</div>
            <div class="card-body">
              <p class="info-text">
                Publica en <code>campuslab.bookings.stream</code> con key <code>labId</code> para garantizar particionado ordenado.
              </p>
              <button class="btn-action kafka-btn" (click)="sendKafkaBooking()">
                > PUBLICAR EVENTO STREAMING (RESERVA)
              </button>
            </div>
          </div>

          <!-- Disparar Telemetría IoT Kafka -->
          <div class="ryoku-card">
            <div class="card-header">KAFKA // TELEMETRÍA IOT SENSORES</div>
            <div class="card-body">
              <p class="info-text">
                Publica telemetría en <code>campuslab.telemetry.metrics</code> (temperatura, potencia, puestos activos).
              </p>
              <button class="btn-action kafka-btn" (click)="sendKafkaTelemetry()">
                > TRANSMITIR MÉTRICA SENSOR (IOT)
              </button>
            </div>
          </div>

          <!-- Ráfaga de Carga Concurrente (Particionado) -->
          <div class="ryoku-card">
            <div class="card-header">KAFKA // PRUEBA DE PARTICIONADO</div>
            <div class="card-body">
              <p class="info-text">
                Dispara ráfaga de 10 eventos distribuidos en 3 particiones y balanceados en Consumer Groups.
              </p>
              <button class="btn-action secondary" (click)="simulateKafkaLoad()">
                > DISPARAR RÁFAGA (10 EVENTOS)
              </button>
            </div>
          </div>

          <!-- Simulador Dead Letter Topic (DLT) -->
          <div class="ryoku-card card-alert">
            <div class="card-header text-danger">KAFKA // RESILIENCIA Y DLT</div>
            <div class="card-body">
              <p class="info-text">
                Inyecta payload no procesable. Ejecuta 2 reintentos con <code>FixedBackOff</code> y redirige al tópico <code>.DLT</code>.
              </p>
              <button class="btn-action danger" (click)="simulateKafkaDlt()">
                > INYECTAR FALLO Y ACTIVAR DLT
              </button>
            </div>
          </div>
        </div>
      </section>

      <!-- SECCIÓN 2: MICROSERVICIO ADMINISTRADOR DE KAFKA (EP5/EP6) -->
      <section class="section">
        <h2 class="section-title">02 // MICROSERVICIO ADMINISTRADOR DE KAFKA (ADMINCLIENT REST API)</h2>
        <div class="grid admin-grid">
          <!-- Crear Tópico Kafka -->
          <div class="ryoku-card">
            <div class="card-header">KAFKA ADMIN // DECLARAR TÓPICO</div>
            <div class="card-body">
              <input type="text" class="input-terminal" [(ngModel)]="newKafkaTopicName" placeholder="nombre.del.topico" />
              <div class="form-row mt-1">
                <input type="number" class="input-terminal" [(ngModel)]="newKafkaPartitions" placeholder="Particiones (ej: 3)" min="1" max="12" />
                <select class="input-terminal" [(ngModel)]="newKafkaCleanup">
                  <option value="delete">delete</option>
                  <option value="compact">compact</option>
                </select>
              </div>
              <button class="btn-action kafka-btn mt-2" (click)="createKafkaTopic()">
                > POST /api/admin/kafka/topics
              </button>
            </div>
          </div>

          <!-- Eliminar Tópico Kafka -->
          <div class="ryoku-card">
            <div class="card-header">KAFKA ADMIN // ELIMINAR TÓPICO</div>
            <div class="card-body">
              <input type="text" class="input-terminal" [(ngModel)]="deleteKafkaTopicName" placeholder="nombre.del.topico" />
              <button class="btn-action danger mt-2" (click)="deleteKafkaTopic()">
                > DELETE /api/admin/kafka/topics
              </button>
            </div>
          </div>

          <!-- Inspeccionar Tópicos & Consumer Groups -->
          <div class="ryoku-card">
            <div class="card-header">KAFKA ADMIN // ESTADO DE CLÚSTER</div>
            <div class="card-body">
              <p class="info-text">Consulta tópicos configurados, particiones activas y grupos de consumidores.</p>
              <div class="btn-group">
                <button class="btn-action kafka-btn" (click)="listKafkaTopics()">> LISTAR TÓPICOS</button>
                <button class="btn-action secondary" (click)="listKafkaConsumerGroups()">> VER CONSUMER GROUPS</button>
                <button class="btn-action secondary" (click)="loadKafkaAnalytics()">> ACTUALIZAR MÉTRICAS</button>
              </div>
            </div>
          </div>
        </div>
      </section>

      <!-- SECCIÓN 3: MENSAJERÍA TRANSACCIONAL RABBITMQ (EP3/EP4 CONTINUIDAD) -->
      <section class="section">
        <h2 class="section-title">03 // MENSAJERÍA TRANSACCIONAL Y TAREAS (RABBITMQ CLUSTER 2 NODOS)</h2>
        <div class="grid">
          <!-- Notificación (Direct Exchange) -->
          <div class="ryoku-card">
            <div class="card-header">RABBITMQ // DIRECT EXCHANGE</div>
            <div class="card-body">
              <p class="info-text">
                Encola hacia <code>campuslab.direct</code> con routing key <code>routing.notifications</code> (ACK manual).
              </p>
              <button class="btn-action" (click)="sendNotification()">
                > DISPARAR NOTIFICACIÓN (ACK)
              </button>
            </div>
          </div>

          <!-- Reportes (Topic Exchange) -->
          <div class="ryoku-card">
            <div class="card-header">RABBITMQ // TOPIC EXCHANGE (REPORTES)</div>
            <div class="card-body">
              <p class="info-text">
                Encola hacia <code>campuslab.topic</code> con routing key <code>routing.reports.generate</code>.
              </p>
              <button class="btn-action" (click)="sendReport()">
                > SOLICITAR REPORTE (ACK)
              </button>
            </div>
          </div>

          <!-- Auditoría + BD -->
          <div class="ryoku-card">
            <div class="card-header">RABBITMQ // AUDITORÍA + POSTGRESQL</div>
            <div class="card-body">
              <p class="info-text">
                Encola evento a <code>campuslab.topic</code> con persistencia relacional en PostgreSQL Cloud.
              </p>
              <div class="btn-group">
                <button class="btn-action" (click)="sendAudit()">> REGISTRAR AUDITORÍA</button>
                <button class="btn-action secondary" (click)="loadAudits()">> CONSULTAR BD</button>
              </div>
            </div>
          </div>

          <!-- DLQ Simulator RabbitMQ -->
          <div class="ryoku-card card-alert">
            <div class="card-header text-danger">RABBITMQ // SIMULACIÓN DE FALLO DLQ</div>
            <div class="card-body">
              <p class="info-text">
                Envía carga que activa <code>basicNack(requeue=false)</code> redirigiendo hacia la DLQ de RabbitMQ.
              </p>
              <button class="btn-action danger" (click)="simulateDlqError()">
                > ACTIVAR FALLO RABBITMQ (DLQ)
              </button>
            </div>
          </div>
        </div>
      </section>

      <!-- CONSOLA DE SALIDA Y RESPUESTA JSON -->
      <section class="section">
        <div class="ryoku-card console-card">
          <div class="card-header console-header">
            <span>CONSOLA DEL SISTEMA // RESPUESTA JSON Y TELEMETRÍA EN VIVO</span>
            <button class="btn-clear" (click)="apiData.set(null)">[ LIMPIAR ]</button>
          </div>
          <div class="card-body">
            @if (apiData()) {
              <pre class="terminal-output">{{ apiData() | json }}</pre>
            } @else {
              <p class="empty-console">> Selecciona una acción superior para interactuar con Kafka, RabbitMQ o el clúster...</p>
            }
          </div>
        </div>
      </section>
    </div>
  `,
  styles: [`
    .dashboard-container {
      padding: 30px;
      background-color: #0d0d0d;
      color: #cdc4ba;
      font-family: 'JetBrains Mono', 'Fira Code', monospace;
      min-height: 100vh;
    }
    .dashboard-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      border-bottom: 1px solid #33302d;
      padding-bottom: 20px;
      margin-bottom: 25px;
    }
    .dashboard-title {
      font-size: 1.4rem;
      letter-spacing: 2px;
      color: #fff;
      margin: 0;
    }
    .dashboard-subtitle {
      font-size: 0.8rem;
      color: #8c857e;
      margin-top: 5px;
    }
    .btn-logout {
      background: transparent;
      border: 1px solid #e06c75;
      color: #e06c75;
      padding: 8px 16px;
      font-family: inherit;
      font-size: 0.8rem;
      cursor: pointer;
      transition: all 0.2s;
    }
    .btn-logout:hover {
      background: rgba(224, 108, 117, 0.1);
    }
    .section {
      margin-bottom: 30px;
    }
    .section-title {
      font-size: 0.9rem;
      color: #e5a463;
      margin-bottom: 15px;
      letter-spacing: 1px;
    }
    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 15px;
      margin-bottom: 20px;
    }
    .metric-card {
      background: #141414;
      border: 1px solid #2a2826;
      padding: 15px;
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .metric-value {
      font-size: 1.8rem;
      font-weight: bold;
      color: #50fa7b;
    }
    .metric-label {
      font-size: 0.75rem;
      color: #8c857e;
      text-transform: uppercase;
      letter-spacing: 1px;
    }
    .text-danger { color: #e06c75 !important; }
    .text-cyan { color: #8be9fd !important; }

    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
      gap: 20px;
    }
    .admin-grid {
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
    }
    .ryoku-card {
      background: #141414;
      border: 1px solid #2a2826;
      border-radius: 2px;
      display: flex;
      flex-direction: column;
    }
    .card-header {
      background: #1c1a19;
      padding: 10px 14px;
      font-size: 0.8rem;
      font-weight: bold;
      letter-spacing: 1px;
      border-bottom: 1px solid #2a2826;
      color: #d19a66;
    }
    .card-body {
      padding: 15px;
      flex: 1;
      display: flex;
      flex-direction: column;
    }
    .card-alert {
      border-color: rgba(224, 108, 117, 0.4);
    }
    .info-text {
      font-size: 0.78rem;
      color: #a09990;
      line-height: 1.4;
      margin-bottom: 15px;
      flex: 1;
    }
    code {
      background: #222;
      padding: 2px 6px;
      border-radius: 3px;
      color: #e5c07b;
      font-size: 0.75rem;
    }
    .btn-action {
      background: transparent;
      border: 1px solid #50fa7b;
      color: #50fa7b;
      padding: 10px 14px;
      font-size: 0.8rem;
      font-family: inherit;
      cursor: pointer;
      transition: background 0.2s;
      width: 100%;
      text-align: left;
    }
    .btn-action:hover {
      background: rgba(80, 250, 123, 0.1);
    }
    .btn-action.kafka-btn {
      border-color: #bd93f9;
      color: #bd93f9;
    }
    .btn-action.kafka-btn:hover {
      background: rgba(189, 147, 249, 0.1);
    }
    .btn-action.secondary {
      border-color: #8be9fd;
      color: #8be9fd;
      margin-top: 8px;
    }
    .btn-action.secondary:hover {
      background: rgba(139, 233, 253, 0.1);
    }
    .btn-action.danger {
      border-color: #e06c75;
      color: #e06c75;
    }
    .btn-action.danger:hover {
      background: rgba(224, 108, 117, 0.1);
    }
    .btn-group {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .input-terminal {
      background: #121212;
      border: 1px solid #33302d;
      color: #cdc4ba;
      padding: 8px 12px;
      font-family: inherit;
      font-size: 0.8rem;
      width: 100%;
      box-sizing: border-box;
    }
    .input-terminal:focus {
      outline: none;
      border-color: #bd93f9;
    }
    select.input-terminal {
      height: 35px;
    }
    .form-row {
      display: flex;
      gap: 10px;
    }
    .mt-1 { margin-top: 8px; }
    .mt-2 { margin-top: 12px; }

    .console-card {
      margin-top: 10px;
      border-color: #50fa7b;
    }
    .console-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
    }
    .btn-clear {
      background: transparent;
      border: none;
      color: #8c857e;
      cursor: pointer;
      font-family: inherit;
      font-size: 0.75rem;
    }
    .btn-clear:hover { color: #fff; }
    .terminal-output {
      font-size: 0.8rem;
      color: #50fa7b;
      background: #080808;
      padding: 16px;
      margin: 0;
      overflow-x: auto;
      border: 1px solid #222;
      max-height: 350px;
    }
    .empty-console {
      color: #555;
      font-size: 0.8rem;
      margin: 10px 0;
    }
  `]
})
export class DashboardComponent implements OnInit {
  private http = inject(HttpClient);
  private authService = inject(AuthService);

  apiData = signal<any>(null);
  analyticsSummary = signal<any>(null);

  // Form inputs Kafka
  newKafkaTopicName = 'campuslab.iot.temperatures';
  newKafkaPartitions = 3;
  newKafkaCleanup = 'delete';
  deleteKafkaTopicName = 'campuslab.iot.temperatures';

  // Form inputs RabbitMQ
  newQueueName = 'campuslab.labs.telemetria';
  deleteQueueName = 'campuslab.labs.telemetria';
  newExchangeName = 'campuslab.custom.events';
  newExchangeType = 'topic';

  ngOnInit(): void {
    this.loadKafkaAnalytics();
  }

  // =========================================================================
  // 1. OPERACIONES KAFKA STREAMING & ANALYTICS (EP5/EP6)
  // =========================================================================

  sendKafkaBooking(): void {
    const payload = {
      eventId: 'EVT-' + Math.random().toString(36).substring(2, 9).toUpperCase(),
      bookingId: Math.floor(Math.random() * 9000) + 1000,
      labId: 'LAB-CLOUD-01',
      studentEmail: 'estudiante@campuslab.cl',
      action: 'RESERVATION_CREATED',
      durationMinutes: 120,
      workstationCount: 2
    };
    this.http.post(`${environment.apiUrl}/stream/kafka/bookings`, payload).subscribe({
      next: (res) => {
        this.apiData.set({ action: 'EVENTO ENVIADO A KAFKA (campuslab.bookings.stream)', response: res });
        this.loadKafkaAnalytics();
      },
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  sendKafkaTelemetry(): void {
    const payload = {
      metricId: 'TEL-' + Math.random().toString(36).substring(2, 9).toUpperCase(),
      labCode: 'LAB-CLOUD-01',
      sensorId: 'SENSOR-ROOM-TEMP-01',
      activeWorkstations: 8,
      temperatureCelsius: 24.5,
      cpuUtilizationPercent: 52.0,
      powerConsumptionWatts: 1950.0
    };
    this.http.post(`${environment.apiUrl}/stream/kafka/telemetry`, payload).subscribe({
      next: (res) => {
        this.apiData.set({ action: 'MÉTRICA TELEMETRÍA ENVIADA A KAFKA (campuslab.telemetry.metrics)', response: res });
        this.loadKafkaAnalytics();
      },
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  simulateKafkaLoad(): void {
    this.http.post(`${environment.apiUrl}/stream/kafka/simulate-load`, {}).subscribe({
      next: (res) => {
        this.apiData.set({ action: 'RÁFAGA CONCURRENTE DISPARADA (10 EVENTOS / BALANCEO DE PARTICIONES)', response: res });
        setTimeout(() => this.loadKafkaAnalytics(), 500);
      },
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  simulateKafkaDlt(): void {
    this.http.post(`${environment.apiUrl}/stream/kafka/trigger-dlt-error`, {}).subscribe({
      next: (res) => {
        this.apiData.set({ action: 'PRUEBA DEAD LETTER TOPIC (DLT) - FALLO CONTROLADO + REINTENTOS', response: res });
        setTimeout(() => this.loadKafkaAnalytics(), 2500); // Dar tiempo al FixedBackOff de 2 reintentos
      },
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  loadKafkaAnalytics(): void {
    this.http.get(`${environment.apiUrl}/stream/kafka/analytics`).subscribe({
      next: (res) => this.analyticsSummary.set(res),
      error: (err) => console.error('Error cargando analítica Kafka:', err)
    });
  }

  getActiveWorkstationsCount(): number {
    const summary = this.analyticsSummary();
    if (!summary || !summary.activeWorkstationsByLab) return 0;
    return Object.values(summary.activeWorkstationsByLab as Record<string, number>).reduce((a, b) => a + b, 0);
  }

  // =========================================================================
  // 2. OPERACIONES MICROSERVICIO ADMINISTRADOR KAFKA (EP5/EP6)
  // =========================================================================

  createKafkaTopic(): void {
    const payload = {
      name: this.newKafkaTopicName,
      partitions: this.newKafkaPartitions,
      replicationFactor: 1,
      retentionMs: 86400000,
      cleanupPolicy: this.newKafkaCleanup
    };
    this.http.post(`${environment.apiUrl}/admin/kafka/topics`, payload).subscribe({
      next: (res) => this.apiData.set({ action: 'TÓPICO KAFKA CREADO PROGRAMÁTICAMENTE (ADMINCLIENT)', response: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  deleteKafkaTopic(): void {
    this.http.delete(`${environment.apiUrl}/admin/kafka/topics/${this.deleteKafkaTopicName}`).subscribe({
      next: (res) => this.apiData.set({ action: 'TÓPICO KAFKA ELIMINADO', response: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  listKafkaTopics(): void {
    this.http.get(`${environment.apiUrl}/admin/kafka/topics`).subscribe({
      next: (res) => this.apiData.set({ action: 'TÓPICOS KAFKA REGISTRADOS EN CLÚSTER', topics: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  listKafkaConsumerGroups(): void {
    this.http.get(`${environment.apiUrl}/admin/kafka/consumer-groups`).subscribe({
      next: (res) => this.apiData.set({ action: 'CONSUMER GROUPS ACTIVOS EN KAFKA', consumerGroups: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  // =========================================================================
  // 3. OPERACIONES RABBITMQ (EP3/EP4 CONTINUIDAD)
  // =========================================================================

  sendNotification(): void {
    const payload = {
      userEmail: 'estudiante@campuslab.cl',
      message: 'Confirmación de reserva de laboratorio computación nube #101',
      type: 'BOOKING_NOTIFICATION'
    };
    this.http.post(`${environment.apiUrl}/events/notification`, payload).subscribe({
      next: (res) => this.apiData.set({ action: 'NOTIFICACIÓN PUBLICADA A RABBITMQ (campuslab.direct)', response: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  sendReport(): void {
    const payload = {
      reportName: 'ReporteUsoLaboratorios_Noviembre2026',
      format: 'PDF',
      requestedBy: 'DocenteEvaluador'
    };
    this.http.post(`${environment.apiUrl}/events/report`, payload).subscribe({
      next: (res) => this.apiData.set({ action: 'SOLICITUD DE REPORTE PUBLICADA A RABBITMQ (campuslab.topic)', response: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  sendAudit(): void {
    const payload = {
      action: 'USER_BOOKING_ACCESS',
      details: 'El estudiante accedió al módulo de reservas de infraestructura cloud',
      userEmail: 'estudiante@campuslab.cl'
    };
    this.http.post(`${environment.apiUrl}/events/audit`, payload).subscribe({
      next: (res) => this.apiData.set({ action: 'AUDITORÍA PUBLICADA A RABBITMQ + BD POSTGRESQL', response: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  loadAudits(): void {
    this.http.get(`${environment.apiUrl}/events/audits`).subscribe({
      next: (res) => this.apiData.set({ action: 'REGISTROS CONSULTADOS DESDE POSTGRESQL CLOUD', records: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  simulateDlqError(): void {
    this.http.post(`${environment.apiUrl}/events/notification/error-test`, {}).subscribe({
      next: (res) => this.apiData.set({ action: 'PRUEBA DEAD LETTER QUEUE (DLQ) RABBITMQ', result: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  onLogout(): void {
    this.authService.logout();
  }
}
