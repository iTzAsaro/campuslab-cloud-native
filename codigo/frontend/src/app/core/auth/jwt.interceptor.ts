import { HttpInterceptorFn } from '@angular/common/http';

export const jwtInterceptor: HttpInterceptorFn = (req, next) => {
  // Evitar inyección circular (NG0200) leyendo el token directamente del storage
  const token = sessionStorage.getItem('id_token') || sessionStorage.getItem('access_token') || localStorage.getItem('access_token');

  if (token && !req.url.includes('microsoftonline.com') && !req.url.includes('.well-known')) {
    req = req.clone({
      setHeaders: {
        Authorization: `Bearer ${token}`
      }
    });
  }

  return next(req);
};
