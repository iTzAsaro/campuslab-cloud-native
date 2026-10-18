import { Component, inject } from '@angular/core';
import { Router } from '@angular/router';
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
            >_ INICIAR SESIÓN CON IDAAS (AZURE ENTRA ID)
          </button>
          <button class="btn-secondary mt-3" (click)="onLoginDemo()">
            [ MODO DEMOSTRACIÓN EN VIVO (ACCESO DIRECTO) ]
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
      margin-bottom: 30px;
    }
    .btn-primary {
      background: transparent;
      color: #50fa7b;
      border: 1px solid #50fa7b;
      padding: 12px 24px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.85rem;
      cursor: pointer;
      transition: all 0.2s ease;
      width: 100%;
    }
    .btn-primary:hover {
      background: rgba(80, 250, 123, 0.1);
    }
    .btn-secondary {
      background: transparent;
      color: #8c857e;
      border: 1px dashed #33302d;
      padding: 10px 24px;
      font-family: 'JetBrains Mono', monospace;
      font-size: 0.75rem;
      cursor: pointer;
      transition: all 0.2s ease;
      width: 100%;
    }
    .btn-secondary:hover {
      color: #fff;
      border-color: #8c857e;
    }
    .mt-3 {
      margin-top: 14px;
    }
  `]
})
export class LoginComponent {
  private authService = inject(AuthService);
  private router = inject(Router);

  onLogin(): void {
    this.authService.login();
  }

  onLoginDemo(): void {
    this.authService.loginDemo();
    this.router.navigate(['/dashboard']);
  }
}
