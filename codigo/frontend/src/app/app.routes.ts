import { Routes } from '@angular/router';
import { LoginComponent } from './features/login/login.component';
import { DashboardComponent } from './features/dashboard/dashboard.component';
import { inject } from '@angular/core';
import { AuthService } from './core/auth/auth.service';
import { Router } from '@angular/router';

const authGuard = async () => {
  const authService = inject(AuthService);
  const router = inject(Router);
  
  if (authService.isAuthenticated()) {
    return true;
  }

  // Esperar a que el SDK procese el callback con ?code= de Microsoft Entra ID
  await authService.ensureLoaded();

  if (authService.isAuthenticated() || authService.hasValidToken()) {
    return true;
  }
  
  return router.parseUrl('/login');
};

export const routes: Routes = [
  { path: 'login', component: LoginComponent },
  { path: 'dashboard', component: DashboardComponent, canActivate: [authGuard] },
  { path: '', redirectTo: '/login', pathMatch: 'full' },
  { path: '**', redirectTo: '/login' }
];
