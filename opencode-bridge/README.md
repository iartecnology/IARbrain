# opencode-bridge

Puente HTTP compatible con OpenAI y Ollama que expone **Antigravity CLI (`agy`)** como un modelo
(`antigravity-session`). Sirve para que clientes como APX usen `agy` sin API key propia.

```
cliente (APX, etc.)
   │  POST /v1/chat/completions   (Bearer BRIDGE_TOKEN)
   ▼
opencode-bridge  (server.py, solo stdlib de Python)
   │  subproceso por llamada, prompt por stdin (stream-json)
   ▼
agy  (Antigravity CLI, sesion OAuth en AGY_HOME)
```

## Funcionalidad

- **Texto libre**: `messages` → `agy --print`, respuesta en `choices[0].message.content`.
- **Tool calling emulado**: si el request trae `tools`, `agy` decide una accion con `--json-schema`
  (`tool_call` o `answer`). El puente valida el nombre de la tool y sus argumentos contra el JSON Schema
  de la tool, reintenta (`DECISION_RETRIES`) si no cumple, y devuelve `tool_calls` estilo OpenAI.
  El cliente ejecuta la tool y vuelve a llamar con `role: "tool"`.
- **Streaming SSE** con keepalive mientras `agy` piensa.
- **Cancelacion**: si el cliente se desconecta, se mata el proceso `agy`.
- **Concurrencia limitada**: `MAX_CONCURRENCY` llamadas simultaneas; el resto recibe `429` con `Retry-After`.
- **Errores tipados** (ver tabla). Nunca devuelve un error de `agy` como si fuera una respuesta.
- **Retencion**: borra conversaciones y logs de `agy` mas viejos que `AGY_RETENTION_DAYS`, solo con el puente inactivo.
- **Metricas** Prometheus en `/metrics` y **health** en `/health` (con chequeos de `agy`, del home de `agy` y de auth reciente).
- **Alertas** opcionales por Telegram cuando `agy` pierde la sesion o agota cuota.

## Endpoints

| Metodo | Ruta | Auth | Uso |
|---|---|---|---|
| GET | `/health` | no | Estado: `ok` o `degraded` (503). `?deep=1` hace una llamada real a `agy` (requiere auth). |
| GET | `/metrics` | no | Metricas Prometheus. |
| POST | `/v1/chat/completions`, `/chat/completions` | si | Chat OpenAI, con o sin `tools`, con o sin `stream`. |
| POST | `/api/chat`, `/api/generate` | si | Equivalentes Ollama. |
| GET | `/api/tags`, `/api/ps`, `/v1/models`, `/models`, `/api/version` | si | Listado de modelos. |
| POST | `/api/show` | si | Metadatos del modelo. |

## Errores

Formato: `{"error": {"message", "type", "code", "request_id"}}`

| HTTP | type | Significado | Accion sugerida |
|---|---|---|---|
| 400 | `invalid_json` | Cuerpo no es JSON objeto | Corregir el cliente |
| 401 | `unauthorized` | Falta o es invalido `BRIDGE_TOKEN` | Revisar la key del cliente |
| 404 | `not_found` | Endpoint no soportado (p. ej. `/responses`) | — |
| 413 | `prompt_too_large` | Supera `MAX_PROMPT_CHARS` | Reducir contexto |
| 429 | `busy` | Concurrencia llena (`Retry-After: 5`) | Reintentar |
| 429 | `agy_quota` | Cuota del modelo agotada (`Retry-After: 60`) | Esperar o cambiar modelo |
| 499 | `client_closed` | El cliente se fue durante la llamada | — |
| 502 | `invalid_decision` | El modelo no dio una tool valida tras los reintentos | Reintentar |
| 502 | `agy_error` | `agy` fallo sin resultado util | Ver logs |
| 503 | `agy_auth` | Sesion OAuth de `agy` invalida o expirada | Re-login (ver operacion) |
| 503 | `agy_unavailable` | No se pudo lanzar `agy` | Revisar imagen |
| 504 | `agy_timeout` | `agy` supero `AGY_TIMEOUT` | Reintentar o subir el limite |

## Configuracion

Ver `.env.example`. Variables clave:

- `BRIDGE_TOKEN` **obligatorio** (o `BRIDGE_ALLOW_NOAUTH=1`, no recomendado; el servidor no arranca sin ninguno de los dos).
- `AGY_MODEL` modelo de `agy` (vacio = por defecto de `agy`). Listar con `agy models`.
- `MAX_CONCURRENCY`, `AGY_TIMEOUT`, `MAX_PROMPT_CHARS`, `DECISION_RETRIES`.
- `AGY_SKIP_PERMISSIONS` solo afecta al modo texto libre (sin tools).
- `AGY_RETENTION_DAYS` (7 por defecto).
- `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` para alertas.
- `DEBUG_DUMP_REQUESTS` deja de ser `0` solo para depurar: guarda el contenido del request en `/state`.

## Despliegue

```bash
cd opencode-bridge
# 1. Binario de agy (no versionado), misma version que AGY_VERSION del Dockerfile
mkdir -p vendor && cp /ruta/a/agy vendor/agy
# 2. Configuracion
cp .env.example .env && $EDITOR .env          # definir BRIDGE_TOKEN
# 3. Arranque
docker compose up -d --build
# 4. Verificacion
BRIDGE_URL=http://127.0.0.1:11444 BRIDGE_TOKEN=... ./scripts/smoke.sh
```

El build falla si `vendor/agy` no reporta la version `AGY_VERSION`, para evitar desplegar un binario distinto al probado.

### Migracion desde el contenedor manual (`opencode-cli`)

El contenedor anterior guardaba la sesion en `/root/.gemini` dentro del volumen `apx-opencode-cli-yxhi6c_opencode-root`.
Para reutilizar la sesion sin volver a loguear:

```bash
docker volume create opencode-bridge_agy-home
docker run --rm -v apx-opencode-cli-yxhi6c_opencode-root:/src:ro -v opencode-bridge_agy-home:/dst alpine \
  sh -c 'cp -a /src/.gemini/. /dst/'
```

Si `agy` pide login otra vez, ver "Re-login" abajo.

## Operacion

```bash
docker compose logs -f bridge                       # logs JSON
curl -s http://127.0.0.1:11444/health | python3 -m json.tool
curl -s http://127.0.0.1:11444/metrics | head -30
docker compose exec bridge agy models               # modelos disponibles
```

**Re-login de agy** (cuando `/health` devuelve `agy_auth` o `recent_auth_error`):
```bash
docker compose exec -it bridge agy login    # sigue el flujo OAuth que imprime
```

**Actualizar agy**: cambiar `vendor/agy` y `AGY_VERSION` a la vez, reconstruir y verificar con `smoke.sh`.

## Riesgos conocidos

- `AGY_SKIP_PERMISSIONS=1` ejecuta `agy` con permisos automaticos en modo texto libre. Mientras el puente
  sea accesible desde Internet, esto es una superficie de ataque. Recomendado: publicar solo en red interna
  o desactivar esa variable si el cliente solo usa tools.
- El contenedor corre como `root` porque la sesion de `agy` vive en `/root/.gemini`. Hardening pendiente:
  usuario no root con la sesion migrada y `chown`.
- Un modelo con tool calling debil puede proponer argumentos incorrectos que pasen la validacion de tipos.
  La validacion comprueba forma, no semantica: las acciones con dinero deben tener confirmacion aguas arriba.
