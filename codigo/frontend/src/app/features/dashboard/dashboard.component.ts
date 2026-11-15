import { Component, inject, signal, OnInit } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { JsonPipe } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { AuthService } from '../../core/auth/auth.service';
import { environment } from '../../../environments/environment';

type NavigationTab = 'overview' | 'kafka-streaming' | 'kafka-admin' | 'rabbitmq' | 'console';

@Component({
  selector: 'app-dashboard',
  standalone: true,
  imports: [JsonPipe, FormsModule],
  template: `
    <div class="app-layout" [class.sidebar-collapsed]="isSidebarCollapsed()">
      <!-- ================================================================= -->
      <!-- SIDEBAR EXPANDIBLE / COLAPSABLE                                   -->
      <!-- ================================================================= -->
      <aside class="sidebar">
        <!-- Header del Sidebar con Toggle -->
        <div class="sidebar-brand">
          @if (!isSidebarCollapsed()) {
            <div class="brand-info">
              <span class="brand-logo">CL//</span>
              <div class="brand-text">
                <span class="brand-title">CAMPUSLAB</span>
                <span class="brand-badge">CLOUD NATIVE</span>
              </div>
            </div>
          }
          <button class="btn-toggle" (click)="toggleSidebar()" [title]="isSidebarCollapsed() ? 'Expandir menú' : 'Colapsar menú'">
            {{ isSidebarCollapsed() ? '▶' : '◀' }}
          </button>
        </div>

        <!-- Menú de Navegación -->
        <nav class="sidebar-nav">
          @if (!isSidebarCollapsed()) {
            <div class="nav-section-label">// ARQUITECTURA DUAL</div>
          }

          <button class="nav-item" [class.active]="activeTab() === 'overview'" (click)="selectTab('overview')">
            <span class="nav-icon">📊</span>
            @if (!isSidebarCollapsed()) {
              <span class="nav-label">Métricas en Vivo</span>
              <span class="nav-pill">LIVE</span>
            }
          </button>

          <button class="nav-item kafka-nav" [class.active]="activeTab() === 'kafka-streaming'" (click)="selectTab('kafka-streaming')">
            <span class="nav-icon">⚡</span>
            @if (!isSidebarCollapsed()) {
              <span class="nav-label">Kafka Streaming</span>
              <span class="nav-pill purple">EP5</span>
            }
          </button>

          <button class="nav-item kafka-nav" [class.active]="activeTab() === 'kafka-admin'" (click)="selectTab('kafka-admin')">
            <span class="nav-icon">⚙️</span>
            @if (!isSidebarCollapsed()) {
              <span class="nav-label">Kafka AdminClient</span>
              <span class="nav-pill purple">API</span>
            }
          </button>

          <button class="nav-item rmq-nav" [class.active]="activeTab() === 'rabbitmq'" (click)="selectTab('rabbitmq')">
            <span class="nav-icon">🐇</span>
            @if (!isSidebarCollapsed()) {
              <span class="nav-label">RabbitMQ Clúster</span>
              <span class="nav-pill orange">EP3</span>
            }
          </button>

          <button class="nav-item" [class.active]="activeTab() === 'console'" (click)="selectTab('console')">
            <span class="nav-icon">💻</span>
            @if (!isSidebarCollapsed()) {
              <span class="nav-label">Consola del Sistema</span>
              @if (apiData()) {
                <span class="nav-pill green">ACTIVA</span>
              }
            }
          </button>
        </nav>

        <!-- Footer del Sidebar (Estado de Red y Logout) -->
        <div class="sidebar-footer">
          @if (!isSidebarCollapsed()) {
            <div class="network-status">
              <span class="status-indicator"></span>
              <div class="status-text">
                <span class="status-title">BROKERS CLOUD</span>
                <span class="status-desc">Kafka & RabbitMQ UP</span>
              </div>
            </div>
          }
          <button class="btn-logout" (click)="onLogout()" [title]="'Cerrar sesión'">
            <span class="logout-icon">⏻</span>
            @if (!isSidebarCollapsed()) {
              <span class="logout-text">CERRAR SESIÓN</span>
            }
          </button>
        </div>
      </aside>

      <!-- ================================================================= -->
      <!-- ÁREA DE CONTENIDO PRINCIPAL                                       -->
      <!-- ================================================================= -->
      <main class="main-content">
        <!-- Topbar Superior -->
        <header class="topbar">
          <div class="topbar-left">
            <span class="breadcrumb-root">CAMPUSLAB</span>
            <span class="breadcrumb-separator">/</span>
            <span class="breadcrumb-active">{{ getTabTitle() }}</span>
          </div>
          <div class="topbar-right">
            <div class="pill-badge kafka-badge">
              <span class="dot purple-dot"></span> KAFKA: 9092
            </div>
            <div class="pill-badge rmq-badge">
              <span class="dot orange-dot"></span> RMQ CLUSTER: 2 NODOS
            </div>
            <a [href]="getKafkaUiUrl()" target="_blank" class="btn-link-external">
              KAFKA-UI ↗
            </a>
          </div>
        </header>

        <!-- Contenedor con Scroll de Vistas -->
        <div class="content-scrollable">

          <!-- =============================================================== -->
          <!-- VISTA 1: OVERVIEW / MÉTRICAS EN VIVO                            -->
          <!-- =============================================================== -->
          @if (activeTab() === 'overview') {
            <section class="view-section">
              <div class="section-header-row">
                <div>
                  <h2 class="view-title">MOTOR ANALÍTICO KAFKA // MÉTRICAS EN TIEMPO REAL</h2>
                  <p class="view-subtitle">Agregación en streaming procesada por Consumer Groups desacoplados</p>
                </div>
                <button class="btn-action-sm secondary" (click)="loadKafkaAnalytics()">↻ ACTUALIZAR MÉTRICAS</button>
              </div>

              <!-- Tarjetas de Métricas -->
              <div class="metrics-grid">
                <div class="metric-card">
                  <div class="metric-icon">📦</div>
                  <div class="metric-content">
                    <span class="metric-value">{{ analyticsSummary()?.totalBookingsProcessed || 0 }}</span>
                    <span class="metric-label">Reservas Procesadas</span>
                    <span class="metric-sub">Tópico: campuslab.bookings.stream</span>
                  </div>
                </div>

                <div class="metric-card">
                  <div class="metric-icon">📡</div>
                  <div class="metric-content">
                    <span class="metric-value">{{ analyticsSummary()?.totalTelemetryPointsProcessed || 0 }}</span>
                    <span class="metric-label">Puntos Telemetría IoT</span>
                    <span class="metric-sub">Tópico: campuslab.telemetry.metrics</span>
                  </div>
                </div>

                <div class="metric-card alert">
                  <div class="metric-icon text-danger">⚠️</div>
                  <div class="metric-content">
                    <span class="metric-value text-danger">{{ analyticsSummary()?.totalDltAlerts || 0 }}</span>
                    <span class="metric-label">Derivados a DLT</span>
                    <span class="metric-sub">Dead Letter Topic Recovery</span>
                  </div>
                </div>

                <div class="metric-card highlight">
                  <div class="metric-icon text-cyan">🖥️</div>
                  <div class="metric-content">
                    <span class="metric-value text-cyan">{{ getActiveWorkstationsCount() }}</span>
                    <span class="metric-label">Puestos Activos</span>
                    <span class="metric-sub">Ocupación dinámica calculada</span>
                  </div>
                </div>
              </div>

              <!-- Feeds y Alertas en Vivo -->
              <div class="grid grid-2-col mt-4">
                <!-- Feed de Alertas / DLT -->
                <div class="ryoku-card">
                  <div class="card-header">🚨 // ALERTAS DE ANOMALÍAS Y DLT RECIENTES</div>
                  <div class="card-body">
                    @if (analyticsSummary()?.recentAlerts?.length) {
                      <div class="alerts-list">
                        @for (alert of analyticsSummary()?.recentAlerts; track alert.id) {
                          <div class="alert-item" [class.dlt-alert]="alert.type === 'DEAD_LETTER_TOPIC_RECOVERY'">
                            <div class="alert-type">{{ alert.type }}</div>
                            <div class="alert-desc">{{ alert.details || alert.originalTopic }}</div>
                            <div class="alert-time">{{ alert.timestamp }}</div>
                          </div>
                        }
                      </div>
                    } @else {
                      <p class="empty-state">> Sin alertas de sobretemperatura ni mensajes en DLT. Sistema estable.</p>
                    }
                  </div>
                </div>

                <!-- Feed de Eventos de Streaming Recientes -->
                <div class="ryoku-card">
                  <div class="card-header">⚡ // EVENTOS DE STREAMING PROCESADOS (ÚLTIMOS 20)</div>
                  <div class="card-body">
                    @if (analyticsSummary()?.recentBookingEvents?.length) {
                      <div class="events-table-wrapper">
                        <table class="terminal-table">
                          <thead>
                            <tr>
                              <th>EVENT ID</th>
                              <th>LAB ID</th>
                              <th>ACCIÓN</th>
                              <th>PUESTOS</th>
                            </tr>
                          </thead>
                          <tbody>
                            @for (evt of analyticsSummary()?.recentBookingEvents; track evt.eventId) {
                              <tr>
                                <td><code>{{ evt.eventId }}</code></td>
                                <td><span class="tag-pill">{{ evt.labId }}</span></td>
                                <td><span class="action-tag">{{ evt.action }}</span></td>
                                <td>{{ evt.workstationCount }}</td>
                              </tr>
                            }
                          </tbody>
                        </table>
                      </div>
                    } @else {
                      <p class="empty-state">> Sin eventos de reserva encolados aún. Usa la pestaña 'Kafka Streaming' para emitir.</p>
                    }
                  </div>
                </div>
              </div>
            </section>
          }

          <!-- =============================================================== -->
          <!-- VISTA 2: KAFKA STREAMING & PRODUCTORES (EP5/EP6)                -->
          <!-- =============================================================== -->
          @if (activeTab() === 'kafka-streaming') {
            <section class="view-section">
              <div class="section-header-row">
                <div>
                  <h2 class="view-title">01 // STREAMING Y ANALÍTICA CONTINUA (APACHE KAFKA)</h2>
                  <p class="view-subtitle">Publicación orientada a eventos con Partition Keys, serializers tipados y resiliencia DLT</p>
                </div>
              </div>

              <div class="grid grid-2-col">
                <!-- Card 1: Evento Reserva -->
                <div class="ryoku-card">
                  <div class="card-header kafka-header">KAFKA // CICLO DE VIDA RESERVAS</div>
                  <div class="card-body">
                    <p class="info-text">
                      Publica en <code>campuslab.bookings.stream</code> con key <code>labId</code> para garantizar particionado ordenado.
                    </p>
                    <div class="meta-row">
                      <span>Serializer: JSON</span>
                      <span>Partición: Automática (hash labId)</span>
                    </div>
                    <button class="btn-action kafka-btn mt-3" (click)="sendKafkaBooking()">
                      > PUBLICAR EVENTO STREAMING (RESERVA)
                    </button>
                  </div>
                </div>

                <!-- Card 2: Telemetría IoT -->
                <div class="ryoku-card">
                  <div class="card-header kafka-header">KAFKA // TELEMETRÍA IOT SENSORES</div>
                  <div class="card-body">
                    <p class="info-text">
                      Transmite telemetría en <code>campuslab.telemetry.metrics</code> (temperatura, potencia y puestos activos).
                    </p>
                    <div class="meta-row">
                      <span>Sensor: IoT Room 101</span>
                      <span>Frecuencia: Alta velocidad</span>
                    </div>
                    <button class="btn-action kafka-btn mt-3" (click)="sendKafkaTelemetry()">
                      > TRANSMITIR MÉTRICA SENSOR (IOT)
                    </button>
                  </div>
                </div>

                <!-- Card 3: Ráfaga Concurrente -->
                <div class="ryoku-card">
                  <div class="card-header kafka-header">KAFKA // PRUEBA DE PARTICIONADO & BALANCEO</div>
                  <div class="card-body">
                    <p class="info-text">
                      Dispara ráfaga de 10 eventos concurrentes repartidos entre las 3 particiones y balanceados en Consumer Groups.
                    </p>
                    <div class="meta-row">
                      <span>Ráfaga: 10 eventos</span>
                      <span>Particiones destino: [0, 1, 2]</span>
                    </div>
                    <button class="btn-action secondary mt-3" (click)="simulateKafkaLoad()">
                      > DISPARAR RÁFAGA (10 EVENTOS)
                    </button>
                  </div>
                </div>

                <!-- Card 4: Simulador DLT -->
                <div class="ryoku-card card-alert">
                  <div class="card-header text-danger">KAFKA // RESILIENCIA Y DEAD LETTER TOPIC (DLT)</div>
                  <div class="card-body">
                    <p class="info-text">
                      Inyecta payload no procesable. Ejecuta 2 reintentos con <code>FixedBackOff</code> y redirige al tópico <code>campuslab.bookings.stream.DLT</code>.
                    </p>
                    <div class="meta-row text-danger">
                      <span>Reintentos: 2 intentos (1s)</span>
                      <span>Destino Fallo: .DLT</span>
                    </div>
                    <button class="btn-action danger mt-3" (click)="simulateKafkaDlt()">
                      > INYECTAR FALLO Y ACTIVAR DLT
                    </button>
                  </div>
                </div>
              </div>
            </section>
          }

          <!-- =============================================================== -->
          <!-- VISTA 3: MICROSERVICIO ADMINISTRADOR DE KAFKA (EP5/EP6)         -->
          <!-- =============================================================== -->
          @if (activeTab() === 'kafka-admin') {
            <section class="view-section">
              <div class="section-header-row">
                <div>
                  <h2 class="view-title">02 // MICROSERVICIO ADMINISTRADOR DE KAFKA (ADMINCLIENT API)</h2>
                  <p class="view-subtitle">Gestión programática de tópicos, configuraciones avanzadas y monitoreo de Consumer Groups</p>
                </div>
              </div>

              <div class="grid grid-3-col">
                <!-- Crear Tópico -->
                <div class="ryoku-card">
                  <div class="card-header kafka-header">ADMIN // DECLARAR TÓPICO KAFKA</div>
                  <div class="card-body">
                    <label class="form-label">Nombre del Tópico</label>
                    <input type="text" class="input-terminal" [(ngModel)]="newKafkaTopicName" placeholder="campuslab.iot.temperatures" />
                    
                    <div class="form-row mt-2">
                      <div class="form-group">
                        <label class="form-label">Particiones</label>
                        <input type="number" class="input-terminal" [(ngModel)]="newKafkaPartitions" min="1" max="12" />
                      </div>
                      <div class="form-group">
                        <label class="form-label">Cleanup Policy</label>
                        <select class="input-terminal" [(ngModel)]="newKafkaCleanup">
                          <option value="delete">delete</option>
                          <option value="compact">compact</option>
                        </select>
                      </div>
                    </div>

                    <button class="btn-action kafka-btn mt-3" (click)="createKafkaTopic()">
                      > POST /api/admin/kafka/topics
                    </button>
                  </div>
                </div>

                <!-- Eliminar Tópico -->
                <div class="ryoku-card">
                  <div class="card-header kafka-header">ADMIN // ELIMINAR TÓPICO KAFKA</div>
                  <div class="card-body">
                    <label class="form-label">Tópico a Eliminar</label>
                    <input type="text" class="input-terminal" [(ngModel)]="deleteKafkaTopicName" placeholder="campuslab.iot.temperatures" />
                    <p class="info-text mt-2">
                      Elimina el tópico y sus particiones del clúster mediante <code>AdminClient.deleteTopics()</code>.
                    </p>
                    <button class="btn-action danger mt-3" (click)="deleteKafkaTopic()">
                      > DELETE /api/admin/kafka/topics
                    </button>
                  </div>
                </div>

                <!-- Consultar Tópicos y Grupos -->
                <div class="ryoku-card">
                  <div class="card-header kafka-header">ADMIN // INSPECCIÓN DE CLÚSTER</div>
                  <div class="card-body">
                    <p class="info-text">
                      Consulta en vivo la topología registrada en los brokers y la sincronización con ZooKeeper.
                    </p>
                    <div class="btn-group mt-2">
                      <button class="btn-action kafka-btn" (click)="listKafkaTopics()">> LISTAR TÓPICOS REGISTRADOS</button>
                      <button class="btn-action secondary" (click)="listKafkaConsumerGroups()">> VER CONSUMER GROUPS ACTIVOS</button>
                    </div>
                  </div>
                </div>
              </div>
            </section>
          }

          <!-- =============================================================== -->
          <!-- VISTA 4: RABBITMQ CLÚSTER (EP3/EP4 CONTINUIDAD)                 -->
          <!-- =============================================================== -->
          @if (activeTab() === 'rabbitmq') {
            <section class="view-section">
              <div class="section-header-row">
                <div>
                  <h2 class="view-title">03 // MENSAJERÍA TRANSACCIONAL Y TAREAS (RABBITMQ CLÚSTER)</h2>
                  <p class="view-subtitle">Continuidad arquitectónica: Exchanges Direct/Topic, ACK manual, DLQ y RabbitAdminService</p>
                </div>
              </div>

              <div class="grid grid-2-col">
                <!-- Notificaciones Direct -->
                <div class="ryoku-card">
                  <div class="card-header rmq-header">RABBITMQ // DIRECT EXCHANGE (NOTIFICACIONES)</div>
                  <div class="card-body">
                    <p class="info-text">
                      Encola hacia <code>campuslab.direct</code> con routing key <code>routing.notifications</code> (ACK manual).
                    </p>
                    <button class="btn-action mt-3" (click)="sendNotification()">
                      > DISPARAR NOTIFICACIÓN (ACK)
                    </button>
                  </div>
                </div>

                <!-- Reportes Topic -->
                <div class="ryoku-card">
                  <div class="card-header rmq-header">RABBITMQ // TOPIC EXCHANGE (REPORTES)</div>
                  <div class="card-body">
                    <p class="info-text">
                      Encola hacia <code>campuslab.topic</code> con routing key <code>routing.reports.generate</code>.
                    </p>
                    <button class="btn-action mt-3" (click)="sendReport()">
                      > SOLICITAR REPORTE (ACK)
                    </button>
                  </div>
                </div>

                <!-- Auditoría + Postgres -->
                <div class="ryoku-card">
                  <div class="card-header rmq-header">RABBITMQ // AUDITORÍA + POSTGRESQL CLOUD</div>
                  <div class="card-body">
                    <p class="info-text">
                      Encola auditoría hacia <code>campuslab.topic</code> con persistencia automática en base de datos.
                    </p>
                    <div class="btn-group mt-2">
                      <button class="btn-action" (click)="sendAudit()">> REGISTRAR AUDITORÍA</button>
                      <button class="btn-action secondary" (click)="loadAudits()">> CONSULTAR BD POSTGRESQL</button>
                    </div>
                  </div>
                </div>

                <!-- DLQ Simulator RMQ -->
                <div class="ryoku-card card-alert">
                  <div class="card-header text-danger">RABBITMQ // SIMULACIÓN DE FALLO Y DLQ</div>
                  <div class="card-body">
                    <p class="info-text">
                      Envía mensaje que fuerza <code>basicNack(requeue=false)</code>, desviándolo hacia la DLQ de RabbitMQ.
                    </p>
                    <button class="btn-action danger mt-3" (click)="simulateDlqError()">
                      > ACTIVAR FALLO RABBITMQ (DLQ)
                    </button>
                  </div>
                </div>
              </div>
            </section>
          }

          <!-- =============================================================== -->
          <!-- CONSOLA DEL SISTEMA DOCKABLE / PESTAÑA DEDICADA                -->
          <!-- =============================================================== -->
          <section class="section console-section" [class.full-view]="activeTab() === 'console'">
            <div class="ryoku-card console-card">
              <div class="card-header console-header">
                <div class="console-title-group">
                  <span class="console-dot"></span>
                  <span>CONSOLA DEL SISTEMA // RESPUESTA JSON Y TELEMETRÍA EN VIVO</span>
                </div>
                <div class="console-actions">
                  @if (apiData()) {
                    <span class="console-status">[ 200 OK ]</span>
                  }
                  <button class="btn-clear" (click)="apiData.set(null)">[ LIMPIAR ]</button>
                </div>
              </div>
              <div class="card-body console-body">
                @if (apiData()) {
                  <pre class="terminal-output">{{ apiData() | json }}</pre>
                } @else {
                  <p class="empty-console">> Listo. Ejecuta cualquier acción del menú para observar la traza JSON en tiempo real...</p>
                }
              </div>
            </div>
          </section>

        </div>
      </main>
    </div>
  `,
  styles: [`
    /* =======================================================================
       LAYOUT GENERAL & THEME RYOKU CYBERPUNK
       ======================================================================= */
    :host {
      display: block;
      height: 100vh;
      overflow: hidden;
      font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
      background-color: #0a0a0a;
      color: #cdc4ba;
    }

    .app-layout {
      display: flex;
      height: 100vh;
      width: 100vw;
      overflow: hidden;
    }

    /* =======================================================================
       SIDEBAR EXPANDIBLE / COLAPSABLE
       ======================================================================= */
    .sidebar {
      width: 260px;
      background: #11100f;
      border-right: 1px solid #262422;
      display: flex;
      flex-direction: column;
      transition: width 0.25s cubic-bezier(0.4, 0, 0.2, 1);
      z-index: 10;
      flex-shrink: 0;
    }

    .sidebar-collapsed .sidebar {
      width: 68px;
    }

    .sidebar-brand {
      height: 60px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 16px;
      border-bottom: 1px solid #262422;
    }

    .brand-info {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .brand-logo {
      font-weight: 900;
      color: #50fa7b;
      letter-spacing: -1px;
      font-size: 1.1rem;
    }

    .brand-text {
      display: flex;
      flex-direction: column;
    }

    .brand-title {
      font-size: 0.85rem;
      font-weight: 700;
      color: #fff;
      letter-spacing: 1.5px;
    }

    .brand-badge {
      font-size: 0.65rem;
      color: #e5a463;
      letter-spacing: 1px;
    }

    .btn-toggle {
      background: transparent;
      border: 1px solid #33302d;
      color: #8c857e;
      width: 28px;
      height: 28px;
      display: flex;
      align-items: center;
      justify-content: center;
      cursor: pointer;
      font-size: 0.7rem;
      border-radius: 2px;
      transition: all 0.2s;
    }

    .btn-toggle:hover {
      border-color: #50fa7b;
      color: #50fa7b;
    }

    .sidebar-nav {
      flex: 1;
      padding: 15px 8px;
      display: flex;
      flex-direction: column;
      gap: 6px;
      overflow-y: auto;
    }

    .nav-section-label {
      font-size: 0.65rem;
      color: #666059;
      padding: 8px 12px 4px;
      letter-spacing: 1px;
    }

    .nav-item {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 10px 12px;
      background: transparent;
      border: 1px solid transparent;
      color: #a09990;
      cursor: pointer;
      font-family: inherit;
      font-size: 0.8rem;
      border-radius: 2px;
      width: 100%;
      text-align: left;
      transition: all 0.2s;
    }

    .nav-item:hover {
      background: rgba(255, 255, 255, 0.03);
      color: #fff;
    }

    .nav-item.active {
      background: #181716;
      border-color: #50fa7b;
      color: #50fa7b;
    }

    .nav-item.kafka-nav.active {
      border-color: #bd93f9;
      color: #bd93f9;
    }

    .nav-item.rmq-nav.active {
      border-color: #ffb86c;
      color: #ffb86c;
    }

    .nav-icon {
      font-size: 1rem;
      min-width: 20px;
      text-align: center;
    }

    .nav-label {
      flex: 1;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .nav-pill {
      font-size: 0.6rem;
      padding: 2px 5px;
      background: #222;
      border-radius: 2px;
      color: #8c857e;
    }

    .nav-pill.purple { background: rgba(189, 147, 249, 0.15); color: #bd93f9; }
    .nav-pill.orange { background: rgba(255, 184, 108, 0.15); color: #ffb86c; }
    .nav-pill.green { background: rgba(80, 250, 123, 0.15); color: #50fa7b; }

    .sidebar-footer {
      padding: 12px;
      border-top: 1px solid #262422;
      display: flex;
      flex-direction: column;
      gap: 10px;
    }

    .network-status {
      display: flex;
      align-items: center;
      gap: 10px;
      padding: 6px 8px;
      background: #161514;
      border: 1px solid #2a2826;
      border-radius: 2px;
    }

    .status-indicator {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #50fa7b;
      box-shadow: 0 0 8px #50fa7b;
      flex-shrink: 0;
    }

    .status-text {
      display: flex;
      flex-direction: column;
    }

    .status-title {
      font-size: 0.65rem;
      font-weight: bold;
      color: #fff;
    }

    .status-desc {
      font-size: 0.6rem;
      color: #8c857e;
    }

    .btn-logout {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 8px;
      background: transparent;
      border: 1px solid rgba(224, 108, 117, 0.3);
      color: #e06c75;
      padding: 8px;
      font-family: inherit;
      font-size: 0.75rem;
      cursor: pointer;
      border-radius: 2px;
      transition: all 0.2s;
    }

    .btn-logout:hover {
      background: rgba(224, 108, 117, 0.1);
      border-color: #e06c75;
    }

    /* =======================================================================
       MAIN CONTENT AREA & TOPBAR
       ======================================================================= */
    .main-content {
      flex: 1;
      display: flex;
      flex-direction: column;
      overflow: hidden;
      background: #0d0c0b;
    }

    .topbar {
      height: 60px;
      background: #11100f;
      border-bottom: 1px solid #262422;
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 24px;
      flex-shrink: 0;
    }

    .topbar-left {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.85rem;
    }

    .breadcrumb-root { color: #8c857e; font-weight: bold; }
    .breadcrumb-separator { color: #444; }
    .breadcrumb-active { color: #fff; letter-spacing: 1px; }

    .topbar-right {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .pill-badge {
      font-size: 0.7rem;
      padding: 4px 10px;
      background: #181716;
      border: 1px solid #2a2826;
      border-radius: 2px;
      display: flex;
      align-items: center;
      gap: 6px;
    }

    .dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
    }

    .purple-dot { background: #bd93f9; box-shadow: 0 0 6px #bd93f9; }
    .orange-dot { background: #ffb86c; box-shadow: 0 0 6px #ffb86c; }

    .btn-link-external {
      background: rgba(80, 250, 123, 0.08);
      border: 1px solid #50fa7b;
      color: #50fa7b;
      font-size: 0.7rem;
      padding: 4px 10px;
      text-decoration: none;
      border-radius: 2px;
      transition: all 0.2s;
    }

    .btn-link-external:hover {
      background: #50fa7b;
      color: #000;
    }

    .content-scrollable {
      flex: 1;
      padding: 24px;
      overflow-y: auto;
      display: flex;
      flex-direction: column;
      gap: 24px;
    }

    /* =======================================================================
       CARDS, GRIDS Y SECCIONES
       ======================================================================= */
    .view-section {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }

    .section-header-row {
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      border-bottom: 1px solid #22201e;
      padding-bottom: 12px;
      margin-bottom: 8px;
    }

    .view-title {
      font-size: 1rem;
      letter-spacing: 1.5px;
      color: #fff;
      margin: 0;
    }

    .view-subtitle {
      font-size: 0.75rem;
      color: #8c857e;
      margin-top: 4px;
    }

    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
    }

    .metric-card {
      background: #141312;
      border: 1px solid #2a2826;
      padding: 16px;
      border-radius: 2px;
      display: flex;
      align-items: center;
      gap: 16px;
      transition: border-color 0.2s;
    }

    .metric-card:hover {
      border-color: #50fa7b;
    }

    .metric-card.alert:hover {
      border-color: #e06c75;
    }

    .metric-card.highlight:hover {
      border-color: #8be9fd;
    }

    .metric-icon {
      font-size: 1.8rem;
    }

    .metric-content {
      display: flex;
      flex-direction: column;
    }

    .metric-value {
      font-size: 1.8rem;
      font-weight: 800;
      color: #50fa7b;
      line-height: 1;
    }

    .metric-label {
      font-size: 0.75rem;
      text-transform: uppercase;
      letter-spacing: 1px;
      color: #fff;
      margin-top: 6px;
    }

    .metric-sub {
      font-size: 0.65rem;
      color: #777;
      margin-top: 2px;
    }

    .grid {
      display: grid;
      gap: 16px;
    }

    .grid-2-col {
      grid-template-columns: repeat(auto-fit, minmax(360px, 1fr));
    }

    .grid-3-col {
      grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
    }

    .ryoku-card {
      background: #141312;
      border: 1px solid #262422;
      border-radius: 2px;
      display: flex;
      flex-direction: column;
    }

    .card-header {
      background: #191816;
      padding: 10px 14px;
      font-size: 0.75rem;
      font-weight: bold;
      letter-spacing: 1px;
      border-bottom: 1px solid #262422;
      color: #d19a66;
    }

    .card-header.kafka-header {
      color: #bd93f9;
      border-left: 3px solid #bd93f9;
    }

    .card-header.rmq-header {
      color: #ffb86c;
      border-left: 3px solid #ffb86c;
    }

    .card-body {
      padding: 16px;
      flex: 1;
      display: flex;
      flex-direction: column;
    }

    .card-alert {
      border-color: rgba(224, 108, 117, 0.4);
    }

    .info-text {
      font-size: 0.75rem;
      color: #a09990;
      line-height: 1.4;
      flex: 1;
      margin: 0;
    }

    .meta-row {
      display: flex;
      justify-content: space-between;
      font-size: 0.7rem;
      color: #666;
      margin-top: 12px;
      padding-top: 8px;
      border-top: 1px dashed #222;
    }

    code {
      background: #201e1c;
      padding: 2px 6px;
      border-radius: 2px;
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

    .btn-action-sm {
      background: transparent;
      border: 1px solid #8be9fd;
      color: #8be9fd;
      padding: 6px 12px;
      font-size: 0.75rem;
      font-family: inherit;
      cursor: pointer;
      transition: all 0.2s;
    }

    .btn-action-sm:hover {
      background: rgba(139, 233, 253, 0.1);
    }

    .btn-group {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }

    .form-label {
      font-size: 0.7rem;
      color: #8c857e;
      margin-bottom: 4px;
      display: block;
    }

    .input-terminal {
      background: #100f0e;
      border: 1px solid #2a2826;
      color: #cdc4ba;
      padding: 8px 12px;
      font-family: inherit;
      font-size: 0.8rem;
      width: 100%;
      box-sizing: border-box;
      border-radius: 2px;
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

    .form-group {
      flex: 1;
    }

    .mt-2 { margin-top: 8px; }
    .mt-3 { margin-top: 12px; }
    .mt-4 { margin-top: 16px; }

    .text-danger { color: #e06c75 !important; }
    .text-cyan { color: #8be9fd !important; }

    /* =======================================================================
       TABLAS Y FEEDS EN VIVO
       ======================================================================= */
    .alerts-list {
      display: flex;
      flex-direction: column;
      gap: 8px;
      max-height: 220px;
      overflow-y: auto;
    }

    .alert-item {
      background: #1c1a18;
      border-left: 3px solid #ffb86c;
      padding: 8px 12px;
      font-size: 0.75rem;
    }

    .alert-item.dlt-alert {
      border-left-color: #e06c75;
      background: rgba(224, 108, 117, 0.08);
    }

    .alert-type {
      font-weight: bold;
      color: #fff;
    }

    .alert-desc {
      color: #aaa;
      margin-top: 2px;
      font-size: 0.7rem;
    }

    .alert-time {
      font-size: 0.65rem;
      color: #666;
      margin-top: 4px;
    }

    .events-table-wrapper {
      max-height: 220px;
      overflow-y: auto;
    }

    .terminal-table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.75rem;
    }

    .terminal-table th {
      text-align: left;
      padding: 6px 8px;
      color: #8c857e;
      border-bottom: 1px solid #2a2826;
      font-size: 0.7rem;
    }

    .terminal-table td {
      padding: 6px 8px;
      border-bottom: 1px solid #1a1918;
    }

    .tag-pill {
      background: #201e1c;
      padding: 2px 6px;
      border-radius: 2px;
      color: #8be9fd;
      font-size: 0.7rem;
    }

    .action-tag {
      color: #50fa7b;
      font-size: 0.7rem;
    }

    .empty-state {
      color: #555;
      font-size: 0.75rem;
      margin: 15px 0;
    }

    /* =======================================================================
       CONSOLA DEL SISTEMA DOCKABLE
       ======================================================================= */
    .console-section {
      margin-top: auto;
    }

    .console-section.full-view .console-body {
      min-height: 480px;
    }

    .console-card {
      border-color: #33302d;
    }

    .console-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      background: #141312;
    }

    .console-title-group {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 0.75rem;
      color: #50fa7b;
    }

    .console-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #50fa7b;
      box-shadow: 0 0 6px #50fa7b;
    }

    .console-actions {
      display: flex;
      align-items: center;
      gap: 12px;
    }

    .console-status {
      font-size: 0.7rem;
      color: #50fa7b;
      font-weight: bold;
    }

    .btn-clear {
      background: transparent;
      border: none;
      color: #8c857e;
      cursor: pointer;
      font-family: inherit;
      font-size: 0.7rem;
    }

    .btn-clear:hover { color: #fff; }

    .console-body {
      background: #080808;
      padding: 12px;
    }

    .terminal-output {
      font-size: 0.8rem;
      color: #50fa7b;
      margin: 0;
      overflow-x: auto;
      max-height: 280px;
      line-height: 1.4;
    }

    .empty-console {
      color: #444;
      font-size: 0.75rem;
      margin: 10px 0;
    }
  `]
})
export class DashboardComponent implements OnInit {
  private http = inject(HttpClient);
  private authService = inject(AuthService);

  isSidebarCollapsed = signal<boolean>(false);
  activeTab = signal<NavigationTab>('overview');

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

  toggleSidebar(): void {
    this.isSidebarCollapsed.set(!this.isSidebarCollapsed());
  }

  selectTab(tab: NavigationTab): void {
    this.activeTab.set(tab);
    if (tab === 'overview') {
      this.loadKafkaAnalytics();
    }
  }

  getTabTitle(): string {
    switch (this.activeTab()) {
      case 'overview': return 'MÉTRICAS EN TIEMPO REAL';
      case 'kafka-streaming': return 'KAFKA // STREAMING Y ANALÍTICA';
      case 'kafka-admin': return 'KAFKA // MICROSERVICIO ADMINISTRADOR';
      case 'rabbitmq': return 'RABBITMQ // MENSAJERÍA TRANSACCIONAL';
      case 'console': return 'CONSOLA DEL SISTEMA // JSON FEED';
      default: return 'PANEL PRINCIPAL';
    }
  }

  getKafkaUiUrl(): string {
    if (typeof window !== 'undefined') {
      const host = window.location.hostname;
      if (host === 'localhost' || host === '127.0.0.1') {
        return 'http://localhost:8085';
      }
      if (host.includes('sslip.io')) {
        const ip = host.replace('.sslip.io', '').replace(/-/g, '.');
        return `http://${ip}:8085`;
      }
      if (host.includes('amazonaws.com')) {
        return 'http://52.200.87.76:8085';
      }
      return `http://${host}:8085`;
    }
    return (environment as any).kafkaUiUrl || 'http://localhost:8085';
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
        setTimeout(() => this.loadKafkaAnalytics(), 600);
      },
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  simulateKafkaDlt(): void {
    this.http.post(`${environment.apiUrl}/stream/kafka/trigger-dlt-error`, {}).subscribe({
      next: (res) => {
        this.apiData.set({ action: 'PRUEBA DEAD LETTER TOPIC (DLT) - FALLO CONTROLADO + REINTENTOS', response: res });
        setTimeout(() => this.loadKafkaAnalytics(), 2500);
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
