import { Injectable, signal } from '@angular/core';
import { OAuthService, AuthConfig } from 'angular-oauth2-oidc';
import { environment } from '../../../environments/environment';

const authConfig: AuthConfig = {
  issuer: environment.oidc.issuer,
  redirectUri: environment.oidc.redirectUri,
  clientId: environment.oidc.clientId,
  responseType: 'code',
  scope: 'openid profile email',
  showDebugInformation: !environment.production,
  strictDiscoveryDocumentValidation: false, // Requerido para Microsoft Entra ID
};

@Injectable({
  providedIn: 'root'
})
export class AuthService {
  readonly isAuthenticated = signal<boolean>(false);

  constructor(private oauthService: OAuthService) {
    this.configureAuth();
  }

  private configureAuth() {
    this.oauthService.configure(authConfig);
    this.oauthService.loadDiscoveryDocumentAndTryLogin().then(() => {
      this.isAuthenticated.set(this.oauthService.hasValidAccessToken());
    }).catch(err => {
      console.warn('OIDC Discovery info:', err);
    });
  }

  login(): void {
    this.oauthService.initCodeFlow();
  }

  loginDemo(): void {
    this.isAuthenticated.set(true);
  }

  logout(): void {
    this.oauthService.logOut();
    this.isAuthenticated.set(false);
  }

  getToken(): string | null {
    return this.oauthService.getAccessToken();
  }
}
