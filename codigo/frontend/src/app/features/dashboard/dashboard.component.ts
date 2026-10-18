import { Component, inject, signal } from '@angular/core';
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
          <h1 class="dashboard-title">CAMPUSLAB // PANEL DE CONTROL</h1>
          <p class="dashboard-subtitle">Arquitectura Cloud Native - Mensajería Asíncrona (RabbitMQ Cluster + DLQ)</p>
        </div>
        <button class="btn-logout" (click)="onLogout()">[ CERRAR SESIÓN ]</button>
      </header>

      <!-- SECCIÓN 1: EVENTOS Y MENSAJERÍA -->
      <section class="section">
        <h2 class="section-title">01 // DISPARADORES DE MENSAJERÍA ASÍNCRONA</h2>
        <div class="grid">
          <!-- Notificación (Direct Exchange) -->
          <div class="ryoku-card">
            <div class="card-header">EXCHANGE: DIRECT // NOTIFICACIONES</div>
            <div class="card-body">
              <p class="info-text">
                Encola un evento hacia <code>campuslab.direct</code> con routing key <code>routing.notifications</code>.
              </p>
              <button class="btn-action" (click)="sendNotification()">
                > DISPARAR NOTIFICACIÓN (ACK)
              </button>
            </div>
          </div>

          <!-- Reportes (Topic Exchange) -->
          <div class="ryoku-card">
            <div class="card-header">EXCHANGE: TOPIC // REPORTES</div>
            <div class="card-body">
              <p class="info-text">
                Encola solicitud a <code>campuslab.topic</code> con routing key <code>routing.reports.generate</code>.
              </p>
              <button class="btn-action" (click)="sendReport()">
                > SOLICITAR REPORTE (ACK)
              </button>
            </div>
          </div>

          <!-- Auditoría (Topic Exchange + DB Persistence) -->
          <div class="ryoku-card">
            <div class="card-header">EXCHANGE: TOPIC // AUDITORÍA + BD</div>
            <div class="card-body">
              <p class="info-text">
                Encola evento a <code>campuslab.topic</code> con persistencia automática en PostgreSQL Cloud.
              </p>
              <div class="btn-group">
                <button class="btn-action" (click)="sendAudit()">> REGISTRAR AUDITORÍA</button>
                <button class="btn-action secondary" (click)="loadAudits()">> CONSULTAR REGISTROS BD</button>
              </div>
            </div>
          </div>

          <!-- DLQ Simulator (NACK + Dead Letter Queue) -->
          <div class="ryoku-card card-alert">
            <div class="card-header text-danger">DEMO DLQ // SIMULACIÓN DE FALLO</div>
            <div class="card-body">
              <p class="info-text">
                Envía carga con palabra clave <code>error</code>. El consumidor ejecutará <code>basicNack(requeue=false)</code> redirigiendo a DLQ.
              </p>
              <button class="btn-action danger" (click)="simulateDlqError()">
                > ACTIVAR FALLO Y ENVIAR A DLQ
              </button>
            </div>
          </div>
        </div>
      </section>

      <!-- SECCIÓN 2: ADMINISTRACIÓN DINÁMICA DE RABBITMQ (EP3/EP4) -->
      <section class="section">
        <h2 class="section-title">02 // MICROSERVICIO ADMINISTRADOR DE RABBITMQ</h2>
        <div class="grid admin-grid">
          <!-- Crear Cola -->
          <div class="ryoku-card">
            <div class="card-header">ADMIN // DECLARAR COLA</div>
            <div class="card-body">
              <input type="text" class="input-terminal" [(ngModel)]="newQueueName" placeholder="nombre.de.la.cola" />
              <button class="btn-action mt-2" (click)="createQueue()">> POST /api/admin/queues</button>
            </div>
          </div>

          <!-- Eliminar Cola -->
          <div class="ryoku-card">
            <div class="card-header">ADMIN // ELIMINAR COLA</div>
            <div class="card-body">
              <input type="text" class="input-terminal" [(ngModel)]="deleteQueueName" placeholder="nombre.de.la.cola" />
              <button class="btn-action danger mt-2" (click)="deleteQueue()">> DELETE /api/admin/queues</button>
            </div>
          </div>

          <!-- Crear Exchange -->
          <div class="ryoku-card">
            <div class="card-header">ADMIN // DECLARAR EXCHANGE</div>
            <div class="card-body">
              <input type="text" class="input-terminal" [(ngModel)]="newExchangeName" placeholder="nombre.del.exchange" />
              <select class="input-terminal mt-1" [(ngModel)]="newExchangeType">
                <option value="direct">direct</option>
                <option value="topic">topic</option>
                <option value="fanout">fanout</option>
              </select>
              <button class="btn-action mt-2" (click)="createExchange()">> POST /api/admin/exchanges</button>
            </div>
          </div>
        </div>
      </section>

      <!-- SECCIÓN 3: CONSOLA DE SALIDA -->
      @if (apiData()) {
        <div class="ryoku-card console-card">
          <div class="card-header console-header">
            <span>03 // RESPUESTA DEL BACKEND / API GATEWAY</span>
            <button class="btn-clear" (click)="apiData.set(null)">[ LIMPIAR ]</button>
          </div>
          <div class="card-body">
            <pre class="terminal-output">{{ apiData() | json }}</pre>
          </div>
        </div>
      }
    </div>
  `,
  styles: [`
    .dashboard-container { display: flex; flex-direction: column; gap: 32px; max-width: 1200px; margin: 0 auto; padding: 24px 16px; font-family: 'JetBrains Mono', monospace; }
    .dashboard-header { display: flex; justify-content: space-between; align-items: flex-start; border-bottom: 1px solid #33302d; padding-bottom: 20px; }
    .dashboard-title { font-size: 2rem; font-weight: 700; margin: 0; color: #ffffff; letter-spacing: -0.02em; }
    .dashboard-subtitle { color: #8c857e; font-size: 0.85rem; margin: 6px 0 0 0; }
    
    .btn-logout { background: transparent; border: 1px solid #dc3545; color: #dc3545; padding: 6px 12px; font-family: inherit; font-size: 0.75rem; cursor: pointer; transition: all 0.2s; }
    .btn-logout:hover { background: #dc3545; color: #fff; }

    .section { display: flex; flex-direction: column; gap: 16px; }
    .section-title { font-size: 1rem; color: #cdc4ba; margin: 0; letter-spacing: 1px; }

    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); gap: 16px; }
    .admin-grid { grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); }

    .ryoku-card { border: 1px solid #33302d; background: rgba(205, 196, 186, 0.02); display: flex; flex-direction: column; }
    .card-alert { border-color: #e06c75; }
    .card-header { font-size: 0.75rem; font-weight: 700; padding: 12px 16px; border-bottom: 1px solid #33302d; color: #8c857e; letter-spacing: 1px; }
    .text-danger { color: #e06c75; }
    .card-body { padding: 18px; display: flex; flex-direction: column; flex-grow: 1; justify-content: space-between; }
    .info-text { font-size: 0.8rem; color: #cdc4ba; margin-top: 0; margin-bottom: 16px; line-height: 1.5; }
    
    .btn-action { background: transparent; color: #cdc4ba; border: 1px solid #50fa7b; padding: 10px 14px; font-size: 0.8rem; font-family: inherit; cursor: pointer; transition: background 0.2s; width: 100%; text-align: left; }
    .btn-action:hover { background: rgba(80, 250, 123, 0.1); color: #50fa7b; }
    .btn-action.secondary { border-color: #8be9fd; margin-top: 8px; }
    .btn-action.secondary:hover { background: rgba(139, 233, 253, 0.1); color: #8be9fd; }
    .btn-action.danger { border-color: #e06c75; color: #e06c75; }
    .btn-action.danger:hover { background: rgba(224, 108, 117, 0.1); }

    .input-terminal { background: #121212; border: 1px solid #33302d; color: #cdc4ba; padding: 8px 12px; font-family: inherit; font-size: 0.8rem; width: 100%; box-sizing: border-box; }
    .input-terminal:focus { outline: none; border-color: #50fa7b; }
    select.input-terminal { height: 35px; }

    .mt-1 { margin-top: 8px; }
    .mt-2 { margin-top: 12px; }

    .console-card { margin-top: 10px; border-color: #50fa7b; }
    .console-header { display: flex; justify-content: space-between; align-items: center; }
    .btn-clear { background: transparent; border: none; color: #8c857e; cursor: pointer; font-family: inherit; font-size: 0.75rem; }
    .btn-clear:hover { color: #fff; }
    .terminal-output { font-size: 0.8rem; color: #50fa7b; background: #080808; padding: 16px; margin: 0; overflow-x: auto; border: 1px solid #222; max-height: 350px; }
  `]
})
export class DashboardComponent {
  private http = inject(HttpClient);
  private authService = inject(AuthService);
  
  apiData = signal<any>(null);

  newQueueName = 'campuslab.labs.telemetria';
  deleteQueueName = 'campuslab.labs.telemetria';
  newExchangeName = 'campuslab.custom.events';
  newExchangeType = 'topic';

  sendNotification(): void {
    const payload = {
      userEmail: 'estudiante@campuslab.cl',
      message: 'Confirmación de reserva de laboratorio computación nube #101',
      type: 'BOOKING_NOTIFICATION'
    };
    this.http.post(`${environment.apiUrl}/events/notification`, payload).subscribe({
      next: (res) => this.apiData.set({ action: 'NOTIFICACIÓN PUBLICADA', response: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  sendReport(): void {
    const payload = {
      reportName: 'ReporteUsoLaboratorios_Octubre2026',
      format: 'PDF',
      requestedBy: 'DocenteEvaluador'
    };
    this.http.post(`${environment.apiUrl}/events/report`, payload).subscribe({
      next: (res) => this.apiData.set({ action: 'REPORTE ENCOLADO', response: res }),
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
      next: (res) => this.apiData.set({ action: 'AUDITORÍA PUBLICADA', response: res }),
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
      next: (res) => this.apiData.set({ action: 'PRUEBA DEAD LETTER QUEUE (DLQ)', result: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  createQueue(): void {
    const payload = { name: this.newQueueName, durable: true };
    this.http.post(`${environment.apiUrl}/admin/queues`, payload).subscribe({
      next: (res) => this.apiData.set({ action: 'COLA CREADA DINÁMICAMENTE', response: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  deleteQueue(): void {
    this.http.delete(`${environment.apiUrl}/admin/queues/${this.deleteQueueName}`).subscribe({
      next: (res) => this.apiData.set({ action: 'COLA ELIMINADA', response: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  createExchange(): void {
    const payload = { name: this.newExchangeName, type: this.newExchangeType, durable: true };
    this.http.post(`${environment.apiUrl}/admin/exchanges`, payload).subscribe({
      next: (res) => this.apiData.set({ action: 'EXCHANGE CREADO DINÁMICAMENTE', response: res }),
      error: (err) => this.apiData.set({ error: err.message, details: err.error })
    });
  }

  onLogout(): void {
    this.authService.logout();
  }
}
