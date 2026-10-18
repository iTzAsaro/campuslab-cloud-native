# Instrucciones Agente OpenCode

## Resumen Repositorio
- **Tipo**: Proyecto académico/curso (DSY1107).
- **Estructura**: 
  - `Docs/`: Contiene instrucciones y presentaciones (ej. EP3, EP4).
  - `codigo/`: Reservado para código fuente (vacío).

## Restricciones Archivos
- **PDFs grandes**: Archivos en `Docs/` (ej. `EP3...pdf`) exceden límite de contexto. 
- **Solución**: Si lectura PDF falla por tamaño, no reintentar. Pedir a usuario texto plano o usar CLI PDF-a-texto si disponible.

## Flujo de Trabajo
- Todo código fuente va en directorio `codigo/`.
- Mantener separación estricta: documentación en `Docs/`, código en `codigo/`.
