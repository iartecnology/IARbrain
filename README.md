# IARbrain

Infraestructura y servicios de IAR.

## Componentes

| Carpeta | Que es |
|---|---|
| [`opencode-bridge/`](opencode-bridge/README.md) | Puente HTTP OpenAI/Ollama sobre Antigravity CLI (`agy`). Expone `antigravity-session` con tool calling emulado. |

## Convenciones

- Secretos (tokens, sesiones OAuth, binarios) nunca se versionan. Ver `.gitignore` de cada componente y las plantillas `.env.example`.
- Cada componente trae su propio README con despliegue, operacion y riesgos conocidos.
- Cambios de comportamiento de API se describen en la seccion de errores del README del componente.
