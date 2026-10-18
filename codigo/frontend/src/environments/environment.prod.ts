export const environment = {
  production: true,
  apiUrl: 'https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api',
  oidc: {
    issuer: 'https://login.microsoftonline.com/72fd0b5a-8a6a-4cff-89f6-bde961f7e250/v2.0',
    clientId: 'b4aa5522-ed83-4c44-8eae-dd60e09d67f7',
    redirectUri: window.location.origin + '/dashboard'
  }
};
