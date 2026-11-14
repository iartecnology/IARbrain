#!/usr/bin/env bash
# Smoke test contra un puente en marcha.
# Uso: BRIDGE_URL=http://host:11444 BRIDGE_TOKEN=... ./scripts/smoke.sh
set -euo pipefail

URL="${BRIDGE_URL:-http://127.0.0.1:11444}"
TOKEN="${BRIDGE_TOKEN:?define BRIDGE_TOKEN}"
AUTH=(-H "Authorization: Bearer ${TOKEN}")

step() { printf '%-28s ' "$1"; }

step "health"
curl -fsS "${URL}/health" >/dev/null && echo OK

step "rechaza sin token"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "${URL}/v1/chat/completions" \
  -H 'Content-Type: application/json' -d '{"messages":[{"role":"user","content":"x"}]}')
[ "$code" = "401" ] && echo "OK (401)" || { echo "FALLO ($code)"; exit 1; }

step "respuesta de chat"
out=$(curl -fsS "${URL}/v1/chat/completions" "${AUTH[@]}" -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Responde solo: ok"}]}')
echo "$out" | python3 -c 'import sys,json; c=json.load(sys.stdin)["choices"][0]["message"]["content"]; assert c, "vacio"; print("OK:", c[:60])'

step "metricas"
curl -fsS "${URL}/metrics" | grep -q bridge_http_requests_total && echo OK

echo "smoke test completo"
