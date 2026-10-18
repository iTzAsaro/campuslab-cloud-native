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
  private initialLoadingPromise: Promise<boolean>;

  constructor(private oauthService: OAuthService) {
    this.initialLoadingPromise = this.configureAuth();
  }

  private async configureAuth(): Promise<boolean> {
    this.oauthService.configure(authConfig);
    try {
      await this.oauthService.loadDiscoveryDocumentAndTryLogin();
      const valid = this.hasValidToken();
      this.isAuthenticated.set(valid);
      return valid;
    } catch (err) {
      console.warn('OIDC Discovery / Login info:', err);
      return false;
    }
  }

  async ensureLoaded(): Promise<boolean> {
    return this.initialLoadingPromise;
  }

  hasValidToken(): boolean {
    return this.oauthService.hasValidAccessToken() || this.oauthService.hasValidIdToken();
  }

  login(): void {
    if (!this.oauthService.discoveryDocumentLoaded) {
      this.oauthService.loadDiscoveryDocument().then(() => {
        this.oauthService.initCodeFlow();
      }).catch(err => {
        console.error('Error cargando documento OIDC:', err);
        // Fallback directo a initCodeFlow
        this.oauthService.initCodeFlow();
      });
    } else {
      this.oauthService.initCodeFlow();
    }
  }

  loginDemo(): void {
    this.isAuthenticated.set(true);
  }

  logout(): void {
    this.oauthService.logOut();
    this.isAuthenticated.set(false);
  }

  getToken(): string | null {
    return this.oauthService.getIdToken() || this.oauthService.getAccessToken();
  }
}
