export const environment = {
  production: true,
  apiUrl: 'https://hygoaj1glh.execute-api.us-east-1.amazonaws.com/api',
  kafkaUiUrl: 'http://52.200.87.76:8085',
  oidc: {
    issuer: 'https://login.microsoftonline.com/4b9ee4c5-a799-40af-88fe-81d4ef8aaba3/v2.0',
    clientId: '618a6485-6e73-4faf-803c-8e120dd362f4',
    redirectUri: window.location.origin + '/dashboard'
  }
};
