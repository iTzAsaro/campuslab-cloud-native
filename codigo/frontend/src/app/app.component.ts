import { Component } from '@angular/core';
import { RouterOutlet } from '@angular/router';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [RouterOutlet],
  template: `
    <main class="app-container">
      <router-outlet></router-outlet>
    </main>
  `,
  styles: [`
    .app-container {
      max-width: 1200px;
      margin: 0 auto;
      padding: 40px 20px;
    }
  `]
})
export class AppComponent {
  title = 'frontend';
}
