import { Component, inject } from '@angular/core';
import { AuthService } from '../../core/auth/auth.service';

@Component({
  selector: 'app-login',
  standalone: true,
  template: `
    <div class="login-wrapper">
      <div class="login-box">
        <h1 class="brand-title">CAMPUS_LAB_V2</h1>
        <p class="brand-subtitle">// ACCESO AL SISTEMA ASÍNCRONO</p>
        
        <div class="actions">
          <button class="btn-primary" (click)="onLogin()">
            >_ INICIAR SESIÓN CON IDAAS
          </button>
        </div>
      </div>
    </div>
  `,
  styles: [`
    .login-wrapper {
      display: flex;
      justify-content: center;
      align-items: center;
      min-height: 80vh;
    }
    .login-box {
      border: 1px solid #33302d;
      padding: 40px;
      background: rgba(205, 196, 186, 0.02);
      text-align: center;
      width: 100%;
      max-width: 500px;
    }
    .brand-title {
      font-size: 2.5rem;
      margin: 0 0 10px 0;
      color: #ffffff;
      letter-spacing: -0.02em;
    }
    .brand-subtitle {
      font-family: 'JetBrains Mono', monospace;
      color: #8c857e;
      font-size: 0.85rem;
      margin-bottom: 40px;
    }
    .btn-primary {
      background: transparent;
      color: #cdc4ba;
      border: 1px solid #cdc4ba;
      padding: 12px 24px;
      font-size: 0.9rem;
      transition: all 0.2s ease;
      width: 100%;
    }
    .btn-primary:hover {
      background: #cdc4ba;
      color: #000000;
    }
  `]
})
export class LoginComponent {
  private authService = inject(AuthService);

  onLogin(): void {
    this.authService.login();
  }
}
