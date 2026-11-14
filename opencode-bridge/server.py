#!/usr/bin/env python3
"""Puente HTTP compatible con OpenAI y Ollama sobre Antigravity CLI (agy).

Endpoints:
  - OpenAI:  POST /v1/chat/completions y /chat/completions (stream y no stream)
  - Ollama:  GET /api/tags, /api/ps, /api/version, POST /api/show, /api/chat, /api/generate
  - Estado:  GET /health (publico), GET /health?deep=1 (auth, gasta una llamada a agy), GET /metrics (publico)

Tool calling:
  Si el request trae "tools", el puente no deja que agy ejecute nada. Le pide una
  decision estructurada (tool_call | answer) con --json-schema, valida la tool y sus
  argumentos, y devuelve tool_calls en formato OpenAI. El cliente ejecuta la tool y
  vuelve a llamar con el resultado (role="tool").

Solo usa la biblioteca estandar de Python.
"""
import hmac
import http.server
import json
import os
import re
import select
import shutil
import signal
import socket
import socketserver
import subprocess
import threading
import time
import urllib.request
import uuid
from urllib.parse import urlsplit

VERSION = "0.2.0"


def _env_int(name, default):
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def _env_bool(name, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


CONFIG = {
    "host": os.environ.get("HOST", "0.0.0.0"),
    "port": _env_int("PORT", 11444),
    "model_name": os.environ.get("MODEL_NAME", "antigravity-session"),
    "agy_bin": os.environ.get("AGY_BIN", "agy"),
    "agy_model": os.environ.get("AGY_MODEL", ""),
    "agy_timeout": _env_int("AGY_TIMEOUT", 900),
    "agy_home": os.environ.get("AGY_HOME", os.path.expanduser("~/.gemini/antigravity-cli")),
    "bridge_token": os.environ.get("BRIDGE_TOKEN") or None,
    "allow_noauth": _env_bool("BRIDGE_ALLOW_NOAUTH", False),
    "max_concurrency": max(1, _env_int("MAX_CONCURRENCY", 2)),
    "max_prompt_chars": _env_int("MAX_PROMPT_CHARS", 300000),
    "decision_retries": max(0, _env_int("DECISION_RETRIES", 1)),
    "skip_permissions": _env_bool("AGY_SKIP_PERMISSIONS", True),
    "chat_mode": os.environ.get("CHAT_MODE", "plan"),
    "opencode_enabled": _env_bool("OPENCODE_ENABLED", False),
    "opencode_bin": os.environ.get("OPENCODE_BIN", "opencode"),
    "opencode_model": os.environ.get("OPENCODE_MODEL", "opencode/big-pickle"),
    "opencode_timeout": _env_int("OPENCODE_TIMEOUT", 600),
    "opencode_uid": _env_int("OPENCODE_UID", 0),
    "opencode_home": os.environ.get("OPENCODE_HOME", "/var/lib/ocdata"),
    "opencode_permission": os.environ.get(
        "OPENCODE_PERMISSION",
        '{"permission":{"bash":"deny","edit":"deny","write":"deny","webfetch":"deny"}}'),
    "opencode_max_chars": _env_int("OPENCODE_MAX_CHARS", 100000),
    "retention_days": _env_int("AGY_RETENTION_DAYS", 7),
    "dump_requests": _env_bool("DEBUG_DUMP_REQUESTS", False),
    "state_dir": os.environ.get("STATE_DIR", "/state"),
    "log_format": os.environ.get("LOG_FORMAT", "json"),
    "telegram_token": os.environ.get("TELEGRAM_BOT_TOKEN") or None,
    "telegram_chat_id": os.environ.get("TELEGRAM_CHAT_ID") or None,
}

DECISION_SCHEMA = {
    "type": "object",
    "properties": {
        "action": {"type": "string", "enum": ["tool_call", "answer"]},
        "tool": {"type": "string"},
        "arguments": {"type": "object"},
        "content": {"type": "string"},
    },
    "required": ["action"],
    "additionalProperties": False,
}

DECISION_PREAMBLE = """You are the model backend behind an OpenAI-compatible API.
You do NOT execute tools yourself: the caller executes them and sends you the result.
Decide ONLY the next step and reply with ONE JSON object matching the schema.

Protocol:
- To call a tool: {"action":"tool_call","tool":"<name>","arguments":{...}}
- To answer the user: {"action":"answer","content":"<final answer>"}

Rules:
- Call at most ONE tool per turn.
- Use exact tool names and arguments from the schemas below.
- Never invent tool results; wait for the caller to send them.
- Do NOT use your own shell, file, browser, MCP or subagent tools.
- If a tool result is already in the conversation, continue from it.
"""

AUTH_HINTS = ("oauth", "not logged in", "unauthenticated", "unauthorized", "sign in", "login required",
              "token expired", "credentials")
QUOTA_HINTS = ("exhausted your quota", "quota exceeded", "rate limit")
JSON_TYPES = {"string": str, "number": (int, float), "integer": int, "boolean": bool,
              "array": list, "object": dict}


class BridgeError(Exception):
    def __init__(self, status, code, message, retry_after=None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.retry_after = retry_after


def log(event, request_id="-", **fields):
    record = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "event": event, "request_id": request_id}
    record.update(fields)
    if CONFIG["log_format"] == "json":
        line = json.dumps(record, ensure_ascii=False, default=str)
    else:
        line = " ".join("%s=%s" % (k, v) for k, v in record.items())
    print(line, flush=True)


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.http = {}
        self.agy = {}
        self.tokens = {"input": 0, "output": 0}
        self.agy_seconds_sum = 0.0
        self.agy_seconds_count = 0
        self.in_flight = 0
        self.rejected_busy = 0
        self.decision_retries = 0
        self.last_auth_error = 0.0
        self.pruned_entries = 0

    def count(self, table, key, n=1):
        with self.lock:
            table[key] = table.get(key, 0) + n

    def add_usage(self, usage):
        with self.lock:
            self.tokens["input"] += usage.get("prompt_tokens", 0)
            self.tokens["output"] += usage.get("completion_tokens", 0)

    def observe_agy(self, seconds):
        with self.lock:
            self.agy_seconds_sum += seconds
            self.agy_seconds_count += 1

    def render(self):
        with self.lock:
            lines = ["# TYPE bridge_http_requests_total counter"]
            for (path, code), n in sorted(self.http.items()):
                lines.append('bridge_http_requests_total{path="%s",code="%s"} %d' % (path, code, n))
            lines.append("# TYPE bridge_agy_calls_total counter")
            for status, n in sorted(self.agy.items()):
                lines.append('bridge_agy_calls_total{status="%s"} %d' % (status, n))
            lines.append("# TYPE bridge_tokens_total counter")
            lines.append('bridge_tokens_total{direction="input"} %d' % self.tokens["input"])
            lines.append('bridge_tokens_total{direction="output"} %d' % self.tokens["output"])
            lines.append("# TYPE bridge_agy_seconds summary")
            lines.append("bridge_agy_seconds_sum %.3f" % self.agy_seconds_sum)
            lines.append("bridge_agy_seconds_count %d" % self.agy_seconds_count)
            lines.append("# TYPE bridge_in_flight gauge")
            lines.append("bridge_in_flight %d" % self.in_flight)
            lines.append("# TYPE bridge_rejected_busy_total counter")
            lines.append("bridge_rejected_busy_total %d" % self.rejected_busy)
            lines.append("# TYPE bridge_decision_retries_total counter")
            lines.append("bridge_decision_retries_total %d" % self.decision_retries)
            lines.append("# TYPE bridge_pruned_entries_total counter")
            lines.append("bridge_pruned_entries_total %d" % self.pruned_entries)
            return "\n".join(lines) + "\n"


METRICS = Metrics()
GATE = threading.BoundedSemaphore(CONFIG["max_concurrency"])
_last_notify = {"at": 0.0}


def notify(text):
    token, chat = CONFIG["telegram_token"], CONFIG["telegram_chat_id"]
    if not token or not chat:
        return

    def send():
        try:
            data = json.dumps({"chat_id": chat, "text": text[:3500]}).encode("utf-8")
            req = urllib.request.Request(
                "https://api.telegram.org/bot%s/sendMessage" % token,
                data=data, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=10).read()
        except Exception as exc:  # noqa: BLE001
            log("notify_failed", error=type(exc).__name__)

    threading.Thread(target=send, daemon=True).start()


def notify_throttled(text, every=1800):
    now = time.time()
    if now - _last_notify["at"] < every:
        return
    _last_notify["at"] = now
    notify(text)


def looks_like_auth_error(text):
    lowered = (text or "").lower()
    return any(hint in lowered for hint in AUTH_HINTS)


def looks_like_quota_error(text):
    lowered = (text or "").lower()
    return any(hint in lowered for hint in QUOTA_HINTS)


def agy_available():
    return shutil.which(CONFIG["agy_bin"]) is not None or os.path.isfile(CONFIG["agy_bin"])


def client_gone(sock):
    try:
        readable, _, _ = select.select([sock], [], [], 0)
        if readable:
            return sock.recv(1, socket.MSG_PEEK) == b""
    except (OSError, ValueError):
        return True
    return False


def _kill(proc):
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except Exception:  # noqa: BLE001
        try:
            proc.kill()
        except Exception:  # noqa: BLE001
            pass


def call_agy(prompt, schema=None, request_id="-", keepalive=None, cancelled=None):
    """Ejecuta agy con el prompt por stdin y devuelve el evento result como dict.

    keepalive() devuelve False si el cliente ya no esta. cancelled() avisa de cancelacion.
    Lanza BridgeError en timeout, cancelacion, auth o fallo de agy.
    """
    cmd = [CONFIG["agy_bin"], "--print", "", "--input-format", "stream-json",
           "--output-format", "stream-json", "--disable-slash-commands"]
    if CONFIG["agy_model"]:
        cmd += ["--model", CONFIG["agy_model"]]
    if schema is not None:
        cmd += ["--json-schema", json.dumps(schema)]
    elif CONFIG["skip_permissions"]:
        cmd.append("--dangerously-skip-permissions")

    started = time.time()
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, encoding="utf-8",
                                errors="replace", start_new_session=True)
    except OSError as exc:
        METRICS.count(METRICS.agy, "unavailable")
        raise BridgeError(503, "agy_unavailable", "no se pudo ejecutar agy: %s" % exc)

    payload = json.dumps({"event": "user", "message": {"role": "user", "content": prompt}}) + "\n"
    holder = {}

    def worker():
        try:
            holder["out"], holder["err"] = proc.communicate(input=payload)
        except Exception as exc:  # noqa: BLE001
            holder["exc"] = exc

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    deadline = started + CONFIG["agy_timeout"]
    outcome = None
    while thread.is_alive():
        thread.join(5.0)
        if not thread.is_alive():
            break
        if cancelled and cancelled():
            outcome = "cancelled"
            break
        if time.time() > deadline:
            outcome = "timeout"
            break
        if keepalive and not keepalive():
            outcome = "cancelled"
            break
    if outcome:
        _kill(proc)
        thread.join(5)
        METRICS.count(METRICS.agy, outcome)
        METRICS.observe_agy(time.time() - started)
        log("agy_" + outcome, request_id, prompt_chars=len(prompt))
        if outcome == "timeout":
            raise BridgeError(504, "agy_timeout", "agy excedio el tiempo de espera.")
        raise BridgeError(499, "client_closed", "cliente desconectado; llamada a agy cancelada.")

    METRICS.observe_agy(time.time() - started)
    out = holder.get("out") or ""
    err = (holder.get("err") or "").strip()
    result = None
    for raw in reversed(out.splitlines()):
        raw = raw.strip()
        if not raw:
            continue
        try:
            event = json.loads(raw)
        except ValueError:
            continue
        if isinstance(event, dict) and event.get("event") == "result":
            result = event.get("result") or {}
            break

    if result is None:
        METRICS.count(METRICS.agy, "no_result")
        log("agy_no_result", request_id, prompt_chars=len(prompt), stderr=err[:300])
        if looks_like_auth_error(err):
            METRICS.last_auth_error = time.time()
            notify_throttled("opencode-bridge: agy sin sesion valida. Revisar login OAuth.\n%s" % err[:300])
            raise BridgeError(503, "agy_auth", "agy no tiene sesion valida: %s" % err[:300])
        raise BridgeError(502, "agy_error", err or "agy no devolvio un resultado")

    status = result.get("status") or "ERROR"
    response = (result.get("response") or "").strip()
    error_text = str(result.get("error") or "")
    METRICS.count(METRICS.agy, status)
    usage = usage_of(result)
    METRICS.add_usage(usage)
    log("agy_done", request_id, status=status, prompt_chars=len(prompt),
        response_chars=len(response), total_tokens=usage["total_tokens"])

    if status != "SUCCESS":
        if looks_like_quota_error(error_text):
            raise BridgeError(429, "agy_quota", "agy: cuota agotada en el modelo.", retry_after=60)
        if looks_like_auth_error(error_text) or looks_like_auth_error(err):
            METRICS.last_auth_error = time.time()
            notify_throttled("opencode-bridge: agy reporto error de auth.\n%s" % error_text[:300])
            raise BridgeError(503, "agy_auth", "agy: %s" % (error_text or status))
        raise BridgeError(502, "agy_error", "agy status=%s: %s" % (status, error_text or err[:300]))

    return {
        "response": response,
        "structured": result.get("structured_output"),
        "usage": usage,
    }


CONV_ID_RE = re.compile(
    r"^(ses_[A-Za-z0-9]{6,64}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$")
INDEX_LOCK = threading.Lock()
CONV_LOCKS = {}


def engine_for_model(model):
    if model and model == CONFIG["opencode_model"]:
        return "opencode"
    return "agy"


def model_names():
    names = [CONFIG["model_name"]]
    if CONFIG["opencode_enabled"]:
        names.append(CONFIG["opencode_model"])
    return names


def _now():
    return time.strftime("%Y-%m-%dT%H:%M:%S")


def index_path():
    return os.path.join(CONFIG["state_dir"], "conversations.json")


def index_load():
    try:
        with open(index_path(), encoding="utf-8") as fh:
            data = json.load(fh)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _index_save(data):
    os.makedirs(CONFIG["state_dir"], exist_ok=True)
    tmp = index_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False)
    os.replace(tmp, index_path())


def index_touch(cid, engine="agy"):
    with INDEX_LOCK:
        data = index_load()
        entry = data.get(cid) or {"id": cid, "created": _now(), "turns": 0, "engine": engine}
        entry["last_used"] = _now()
        entry["turns"] = int(entry.get("turns", 0)) + 1
        data[cid] = entry
        _index_save(data)


def index_remove(cid):
    with INDEX_LOCK:
        data = index_load()
        if cid not in data:
            return False
        del data[cid]
        _index_save(data)
        return True


def _wait_turn(proc, out_thread, timeout, cancelled, keepalive, request_id, label, started):
    """Espera al proceso de un turno. Lanza BridgeError en timeout o cancelacion."""
    deadline = started + timeout
    outcome = None
    last_keepalive = time.time()
    while True:
        try:
            proc.wait(timeout=2)
            break
        except subprocess.TimeoutExpired:
            pass
        if cancelled():
            outcome = "cancelled"
            break
        if time.time() > deadline:
            outcome = "timeout"
            break
        if keepalive and time.time() - last_keepalive >= 5:
            last_keepalive = time.time()
            if not keepalive():
                outcome = "cancelled"
                break
    if outcome:
        _kill(proc)
        out_thread.join(5)
        METRICS.count(METRICS.agy, outcome)
        log(label + "_" + outcome, request_id)
        if outcome == "timeout":
            raise BridgeError(504, label + "_timeout", "%s excedio el tiempo de espera." % label)
        raise BridgeError(499, "client_closed", "cliente desconectado; turno cancelado.")
    out_thread.join(10)


def run_opencode_turn(message, session_id, request_id, on_init, on_delta, cancelled, keepalive=None):
    """Un turno con OpenCode (`opencode run --format json`). Memoria por sesion (ses_...).

    El proceso corre sin privilegios (OPENCODE_UID) y con entorno vacio: no hereda el token del puente
    ni credenciales del contenedor. Los permisos de bash/edit/write/webfetch estan denegados.
    """
    if not CONFIG["opencode_enabled"]:
        raise BridgeError(404, "model_not_available", "OpenCode no esta habilitado en este puente.")
    if len(message) > CONFIG["opencode_max_chars"]:
        raise BridgeError(413, "prompt_too_large", "mensaje demasiado largo para OpenCode (%d caracteres)."
                          % CONFIG["opencode_max_chars"])
    if message.startswith("-"):
        message = " " + message
    home = CONFIG["opencode_home"]
    cmd = [CONFIG["opencode_bin"], "run", "-m", CONFIG["opencode_model"], "--format", "json"]
    if session_id:
        cmd += ["--session", session_id]
    cmd.append(message)
    if CONFIG["opencode_uid"]:
        cmd = ["setpriv", "--reuid", str(CONFIG["opencode_uid"]), "--regid", str(CONFIG["opencode_uid"]),
               "--clear-groups"] + cmd
    env = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "HOME": home,
        "XDG_DATA_HOME": home + "/data",
        "XDG_STATE_HOME": home + "/state",
        "XDG_CONFIG_HOME": home + "/config",
        "XDG_CACHE_HOME": "/tmp/oc-cache",
        "OPENCODE_CONFIG_CONTENT": CONFIG["opencode_permission"],
    }
    started = time.time()
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, encoding="utf-8", errors="replace", env=env, start_new_session=True)
    except OSError as exc:
        METRICS.count(METRICS.agy, "unavailable")
        raise BridgeError(503, "opencode_unavailable", "no se pudo ejecutar opencode: %s" % exc)

    state = {"cid": session_id, "texts": [], "error": None, "gone": False, "in": 0, "out": 0}

    def send_init(cid):
        if cid and not state.get("init_sent"):
            state["init_sent"] = True
            on_init(cid)

    def reader():
        for raw in proc.stdout:
            raw = raw.strip()
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(event, dict):
                continue
            if event.get("sessionID") and not state["cid"]:
                state["cid"] = event["sessionID"]
                send_init(state["cid"])
            kind = event.get("type")
            part = event.get("part") or {}
            if kind == "text" and part.get("text"):
                state["texts"].append(part["text"])
                if on_delta and not state["gone"]:
                    try:
                        on_delta(part["text"])
                    except Exception:  # noqa: BLE001
                        state["gone"] = True
            elif kind == "step_finish":
                tokens = part.get("tokens") or {}
                state["in"] += int(tokens.get("input") or 0)
                state["out"] += int(tokens.get("output") or 0)
            elif kind == "error":
                state["error"] = event.get("error") or {}

    out_thread = threading.Thread(target=reader, daemon=True)
    err_lines = []
    err_thread = threading.Thread(target=lambda: err_lines.append(proc.stderr.read()), daemon=True)
    out_thread.start()
    err_thread.start()
    send_init(session_id)
    _wait_turn(proc, out_thread, CONFIG["opencode_timeout"], lambda: state["gone"] or cancelled(),
               keepalive, request_id, "opencode", started)
    err_thread.join(5)
    METRICS.observe_agy(time.time() - started)

    err = state["error"]
    if err:
        text = str(err.get("message") or err.get("type") or "error de OpenCode")
        status = (err.get("status") or 0)
        if err.get("type") == "provider.free-limit" or status == 429:
            retry_ms = int(err.get("retryAfterMs") or 0)
            retry = min(3600, max(1, retry_ms // 1000)) if retry_ms else 60
            METRICS.count(METRICS.agy, "opencode_quota")
            raise BridgeError(429, "opencode_quota", "OpenCode: " + text, retry_after=retry)
        METRICS.count(METRICS.agy, "opencode_error")
        raise BridgeError(502, "opencode_error", "OpenCode: " + text)
    if not state["texts"]:
        METRICS.count(METRICS.agy, "opencode_no_result")
        raise BridgeError(502, "opencode_no_result", "OpenCode no devolvio texto. " + (err_lines[0][:300] if err_lines else ""))

    cid = state["cid"]
    send_init(cid)
    usage = {"prompt_tokens": state["in"], "completion_tokens": state["out"],
             "total_tokens": state["in"] + state["out"]}
    METRICS.count(METRICS.agy, "SUCCESS")
    METRICS.add_usage(usage)
    log("opencode_done", request_id, session=cid, total_tokens=usage["total_tokens"])
    return {"conversation_id": cid, "response": "\n".join(state["texts"]).strip(), "usage": usage,
            "status": "SUCCESS"}


def run_turn(engine, message, conversation_id, request_id, on_init, on_delta, cancelled, keepalive=None):
    if engine == "opencode":
        return run_opencode_turn(message, conversation_id, request_id, on_init, on_delta, cancelled, keepalive)
    return run_chat_turn(message, conversation_id, request_id, on_init, on_delta, cancelled, keepalive)


def run_chat_turn(message, conversation_id, request_id, on_init, on_delta, cancelled, keepalive=None):
    """Un turno de chat con agy, como en su chat interactivo. Mantiene la memoria con --conversation.

    on_init(cid) se llama al conocer el id de conversacion; on_delta(text) por cada fragmento de texto.
    Ninguna escritura de cliente puede tumbar el turno: si on_delta falla, se cancela agy.
    """
    cmd = [CONFIG["agy_bin"], "--print", "", "--input-format", "stream-json", "--output-format", "stream-json"]
    if conversation_id:
        cmd += ["--conversation", conversation_id]
    if CONFIG["chat_mode"] == "plan":
        cmd += ["--mode", "plan"]

    started = time.time()
    try:
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True, encoding="utf-8",
                                errors="replace", start_new_session=True)
    except OSError as exc:
        METRICS.count(METRICS.agy, "unavailable")
        raise BridgeError(503, "agy_unavailable", "no se pudo ejecutar agy: %s" % exc)

    try:
        proc.stdin.write(json.dumps({"event": "user", "message": {"role": "user", "content": message}}) + "\n")
        proc.stdin.close()
    except OSError:
        pass

    state = {"cid": conversation_id, "result": None, "init_sent": False, "gone": False}

    def send_init(cid):
        if cid and not state["init_sent"]:
            state["init_sent"] = True
            on_init(cid)

    def reader():
        for raw in proc.stdout:
            raw = raw.strip()
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except ValueError:
                continue
            if not isinstance(event, dict):
                continue
            kind = event.get("event")
            if kind == "init" and not state["cid"]:
                state["cid"] = event.get("conversation_id")
                send_init(state["cid"])
            elif kind == "step_update":
                step = event.get("step_update") or {}
                text = step.get("text_delta")
                if step.get("step_type") == "agent_response" and text and on_delta and not state["gone"]:
                    try:
                        on_delta(text)
                    except Exception:  # noqa: BLE001
                        state["gone"] = True
            elif kind == "result":
                state["result"] = event.get("result") or {}
                state["cid"] = state["cid"] or state["result"].get("conversation_id")

    send_init(conversation_id)
    err_lines = []
    err_thread = threading.Thread(target=lambda: err_lines.append(proc.stderr.read()), daemon=True)
    out_thread = threading.Thread(target=reader, daemon=True)
    err_thread.start()
    out_thread.start()

    _wait_turn(proc, out_thread, CONFIG["agy_timeout"], lambda: state["gone"] or cancelled(),
               keepalive, request_id, "agy", started)
    err_thread.join(5)
    METRICS.observe_agy(time.time() - started)
    cid = state["cid"]
    send_init(cid)
    err = (err_lines[0] if err_lines else "").strip()
    result = state["result"]
    if result is None:
        METRICS.count(METRICS.agy, "no_result")
        if looks_like_auth_error(err):
            METRICS.last_auth_error = time.time()
            raise BridgeError(503, "agy_auth", "agy no tiene sesion valida: %s" % err[:300])
        raise BridgeError(502, "agy_error", err or "agy no devolvio un resultado")

    status = result.get("status") or "ERROR"
    error_text = str(result.get("error") or "")
    METRICS.count(METRICS.agy, status)
    usage = usage_of(result)
    METRICS.add_usage(usage)
    log("chat_done", request_id, status=status, conversation_id=cid, total_tokens=usage["total_tokens"])
    if status != "SUCCESS":
        if looks_like_quota_error(error_text):
            raise BridgeError(429, "agy_quota", "agy: cuota agotada en el modelo.", retry_after=60)
        if looks_like_auth_error(error_text) or looks_like_auth_error(err):
            METRICS.last_auth_error = time.time()
            raise BridgeError(503, "agy_auth", "agy: %s" % (error_text or status))
        raise BridgeError(502, "agy_error", "agy status=%s: %s" % (status, error_text or err[:300]))
    return {"conversation_id": cid, "response": (result.get("response") or "").strip(),
            "usage": usage, "status": status}


def content_to_text(content):
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        bits = []
        for part in content:
            if isinstance(part, str):
                bits.append(part)
            elif isinstance(part, dict) and (part.get("type") == "text" or "text" in part):
                bits.append(str(part.get("text", "")))
        return "\n".join(bits)
    if content is None:
        return ""
    return str(content)


def build_transcript(payload):
    lines = []
    for msg in payload.get("messages") or []:
        if not isinstance(msg, dict):
            continue
        role = str(msg.get("role", "user"))
        calls = msg.get("tool_calls") or []
        if role == "assistant" and calls:
            for call in calls:
                fn = (call or {}).get("function") or {}
                lines.append("Assistant called tool %s with arguments %s"
                             % (fn.get("name"), fn.get("arguments")))
        text = content_to_text(msg.get("content"))
        if role == "tool":
            label = "Tool result"
            if msg.get("name"):
                label += " (%s)" % msg.get("name")
            elif msg.get("tool_call_id"):
                label += " (%s)" % msg.get("tool_call_id")
            if text:
                lines.append(label + ": " + text)
        elif text:
            lines.append(role.capitalize() + ": " + text)
    transcript = "\n".join(lines)
    if not transcript:
        transcript = content_to_text(payload.get("prompt"))
    return transcript


def tool_specs(tools):
    """Devuelve {nombre: parametros} de las tools del request, en formato OpenAI."""
    specs = {}
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        fn = tool.get("function") if tool.get("type") == "function" else tool
        if isinstance(fn, dict) and fn.get("name"):
            specs[fn["name"]] = fn.get("parameters") or {"type": "object", "properties": {}}
    return specs


def build_decision_prompt(tools, tool_choice, payload):
    specs = [{"name": name, "parameters": params} for name, params in tool_specs(tools).items()]
    preamble = DECISION_PREAMBLE
    if isinstance(tool_choice, dict):
        forced = (tool_choice.get("function") or {}).get("name")
        if forced:
            preamble += '\nYou MUST call the tool "%s" in this turn.\n' % forced
    return (preamble + "\nAvailable tools (JSON schemas):\n" + json.dumps(specs, ensure_ascii=False)
            + "\n\nConversation:\n" + build_transcript(payload)
            + "\n\nSystem: Reply now with the next-step JSON object only.")


def validate_arguments(args, schema):
    """Comprueba argumentos contra el subconjunto de JSON Schema que usan las tools. None si es valido."""
    if not isinstance(args, dict):
        return "arguments debe ser un objeto"
    for key in schema.get("required") or []:
        if key not in args:
            return "falta el argumento requerido %r" % key
    props = schema.get("properties") or {}
    for key, value in args.items():
        spec = props.get(key)
        if spec is None:
            if schema.get("additionalProperties") is False:
                return "argumento desconocido %r" % key
            continue
        kind = spec.get("type")
        if kind in ("integer", "number") and isinstance(value, bool):
            return "argumento %r debe ser %s" % (key, kind)
        expected = JSON_TYPES.get(kind)
        if expected and not isinstance(value, expected):
            return "argumento %r debe ser %s" % (key, kind)
        if "enum" in spec and value not in spec["enum"]:
            return "argumento %r fuera de los valores permitidos" % key
    return None


def decide(tools, tool_choice, payload, request_id, keepalive, cancelled):
    """Pide a agy la siguiente accion y la valida. Devuelve (texto, tool_calls, usage)."""
    specs = tool_specs(tools)
    prompt = build_decision_prompt(tools, tool_choice, payload)
    usage_total = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
    feedback = ""
    for attempt in range(CONFIG["decision_retries"] + 1):
        current = prompt
        if feedback:
            METRICS.count(METRICS.agy, "decision_retry")
            METRICS.decision_retries += 1
            current += ("\n\nSystem: your previous decision was rejected (%s). "
                        "Reply again with ONE valid JSON object." % feedback)
        result = call_agy(current, DECISION_SCHEMA, request_id, keepalive, cancelled)
        for key in usage_total:
            usage_total[key] += result["usage"].get(key, 0)
        structured = result.get("structured") or {}
        action = structured.get("action")

        if action == "tool_call":
            name = structured.get("tool")
            if name not in specs:
                feedback = "tool desconocida %r" % name
            else:
                args = structured.get("arguments") or {}
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except ValueError:
                        args = None
                problem = validate_arguments(args, specs[name])
                if problem is None:
                    call = {
                        "id": "call_" + uuid.uuid4().hex[:24],
                        "type": "function",
                        "function": {"name": name, "arguments": json.dumps(args, ensure_ascii=False)},
                    }
                    return "", [call], usage_total
                feedback = "tool %r: %s" % (name, problem)
        elif action == "answer":
            content = structured.get("content") or result["response"]
            if content:
                return content, [], usage_total
            feedback = "respuesta vacia"
        else:
            if result["response"]:
                return result["response"], [], usage_total
            feedback = "falta el campo action"
        log("decision_rejected", request_id, attempt=attempt, reason=feedback)

    raise BridgeError(502, "invalid_decision",
                      "el modelo no devolvio una decision valida: %s" % feedback)


def usage_of(result):
    usage = result.get("usage") or {}
    return {
        "prompt_tokens": int(usage.get("input_tokens") or 0),
        "completion_tokens": int(usage.get("output_tokens") or 0),
        "total_tokens": int(usage.get("total_tokens") or 0),
    }


def prune_agy_state():
    """Borra entradas viejas de conversaciones y logs de agy. Solo con el puente inactivo."""
    cutoff = time.time() - CONFIG["retention_days"] * 86400
    removed = 0
    keep = list(index_load().keys())
    for sub in ("conversations", "brain", "log"):
        base = os.path.join(CONFIG["agy_home"], sub)
        if not os.path.isdir(base):
            continue
        for name in os.listdir(base):
            path = os.path.join(base, name)
            try:
                if os.path.getmtime(path) >= cutoff or os.path.islink(path):
                    continue
                if any(cid in name for cid in keep):
                    continue
                if os.path.isdir(path) and not os.path.islink(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
                removed += 1
            except OSError:
                continue
    if removed:
        with METRICS.lock:
            METRICS.pruned_entries += removed
        log("state_pruned", removed=removed, retention_days=CONFIG["retention_days"])


def _pruner_loop():
    while True:
        time.sleep(3600)
        if CONFIG["retention_days"] <= 0:
            continue
        with METRICS.lock:
            busy = METRICS.in_flight > 0
        if not busy:
            try:
                prune_agy_state()
            except Exception as exc:  # noqa: BLE001
                log("prune_failed", error=type(exc).__name__)


MODEL_ENTRY_DETAILS = {
    "parent_model": "",
    "format": "gguf",
    "family": "llama",
    "families": ["llama"],
    "parameter_size": "8B",
    "quantization_level": "Q4_0",
}


def model_entry(model_id):
    name = model_id + ":latest"
    return {"name": name, "model": name, "modified_at": "2026-11-07T00:00:00Z",
            "size": 7365960935, "digest": "sha256:1234567890abcdef", "details": MODEL_ENTRY_DETAILS}


def openai_model_entry(model_id):
    owner = "opencode" if model_id == CONFIG["opencode_model"] else "antigravity"
    return {"id": model_id, "object": "model", "created": 1700000000, "owned_by": owner}


class BridgeHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    server_version = "AntigravityBridge/%s" % VERSION

    def setup(self):
        super().setup()
        self.request_id = uuid.uuid4().hex[:12]
        self.status_sent = 0

    # ---------- helpers ----------
    def _path(self):
        path = urlsplit(self.path).path
        if len(path) > 1:
            path = path.rstrip("/")
        return path

    def _query(self):
        return urlsplit(self.path).query

    def _headers(self, status=200, content_type="application/json", extra=None):
        self.status_sent = status
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        for key, value in (extra or {}).items():
            self.send_header(key, value)
        self.end_headers()

    def _json(self, obj, status=200, extra=None):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self._headers(status, extra=extra)
        self.wfile.write(body)

    def _error(self, err):
        extra = {"Retry-After": str(err.retry_after)} if err.retry_after else None
        self._json({"error": {"message": err.message, "type": err.code, "code": err.code,
                              "request_id": self.request_id}}, err.status, extra)

    def _authorized(self):
        if not CONFIG["bridge_token"]:
            return True
        header = self.headers.get("Authorization") or ""
        token = header[7:] if header.startswith("Bearer ") else header
        if hmac.compare_digest(token.encode("utf-8"), CONFIG["bridge_token"].encode("utf-8")):
            return True
        log("unauthorized", self.request_id, method=self.command, path=self._path())
        raise BridgeError(401, "unauthorized", "token invalido o ausente")

    def _sse(self, obj):
        self.wfile.write(("data: " + json.dumps(obj, ensure_ascii=False) + "\n\n").encode("utf-8"))
        self.wfile.flush()

    def _keepalive(self):
        try:
            self.wfile.write(b": keepalive\n\n")
            self.wfile.flush()
            return True
        except OSError:
            return False

    def _chunk(self, rid, created, delta=None, finish=None, usage=None):
        obj = {"id": rid, "object": "chat.completion.chunk", "created": created,
               "model": CONFIG["model_name"],
               "choices": [{"index": 0, "delta": delta or {}, "finish_reason": finish}]}
        if usage:
            obj["usage"] = usage
        return obj

    def log_message(self, fmt, *args):
        return

    def _route_done(self, path, code):
        METRICS.count(METRICS.http, (path, str(code)))

    # ---------- GET ----------
    def do_GET(self):
        path = self._path()
        try:
            if path in ("/", "/health"):
                self._health()
            elif path == "/metrics":
                body = METRICS.render().encode("utf-8")
                self._headers(200, "text/plain; version=0.0.4")
                self.wfile.write(body)
            else:
                self._authorized()
                if path == "/chat/conversations":
                    items = sorted(index_load().values(), key=lambda e: e.get("last_used", ""), reverse=True)
                    self._json({"conversations": items})
                elif path == "/api/version":
                    self._json({"version": "0.1.32", "status": "ok", "model": CONFIG["model_name"],
                                "tool_calling": True})
                elif path in ("/api/tags", "/api/ps"):
                    self._json({"models": [model_entry(name) for name in model_names()]})
                elif path in ("/v1/models", "/models"):
                    self._json({"object": "list", "data": [openai_model_entry(name) for name in model_names()]})
                elif path == "/api/show":
                    self._json(self._show_body())
                else:
                    raise BridgeError(404, "not_found", "Not Found")
        except BridgeError as err:
            self._error(err)
        finally:
            self._route_done(path, self.status_sent)

    def _health(self):
        checks = {
            "agy_present": agy_available(),
            "agy_home": os.path.isdir(CONFIG["agy_home"]),
            "recent_auth_error": (time.time() - METRICS.last_auth_error) < 600,
        }
        ok = checks["agy_present"] and checks["agy_home"] and not checks["recent_auth_error"]
        checks["opencode"] = (not CONFIG["opencode_enabled"]) or os.path.isfile(CONFIG["opencode_bin"]) \
            or shutil.which(CONFIG["opencode_bin"]) is not None
        ok = ok and checks["opencode"]
        body = {"status": "ok" if ok else "degraded", "version": VERSION, "model": CONFIG["model_name"],
                "models": model_names(), "tool_calling": True, "chat": {"mode": CONFIG["chat_mode"]},
                "checks": checks, "in_flight": METRICS.in_flight,
                "max_concurrency": CONFIG["max_concurrency"]}
        query = self._query()
        if "deep=1" in query:
            self._authorized()
            try:
                call_agy("Reply with the single word: ok", request_id=self.request_id)
                body["deep"] = "ok"
            except BridgeError as err:
                body["deep"] = err.code
                ok = False
        self._json(body, 200 if ok else 503)

    def do_DELETE(self):
        path = self._path()
        try:
            self._authorized()
            prefix = "/chat/conversations/"
            if not path.startswith(prefix):
                raise BridgeError(404, "not_found", "Not Found")
            cid = path[len(prefix):]
            if not CONV_ID_RE.match(cid):
                raise BridgeError(400, "invalid_request", "conversation_id invalido")
            if not index_remove(cid):
                raise BridgeError(404, "conversation_not_found", "conversacion no encontrada")
            self._json({"deleted": cid})
        except BridgeError as err:
            self._error(err)
        finally:
            self._route_done(path, self.status_sent)

    def _chat(self, payload):
        message = payload.get("message")
        if not isinstance(message, str) or not message.strip():
            raise BridgeError(400, "invalid_request", "message es obligatorio")
        if message.lstrip().startswith("/"):
            raise BridgeError(400, "slash_commands_disabled",
                              "los comandos / no estan permitidos en el chat remoto")
        self._check_prompt(message)
        engine = engine_for_model(payload.get("model"))
        if engine == "opencode" and not CONFIG["opencode_enabled"]:
            raise BridgeError(404, "model_not_available", "OpenCode no esta habilitado en este puente.")
        cid = payload.get("conversation_id")
        lock = None
        if cid is not None:
            if not isinstance(cid, str) or not CONV_ID_RE.match(cid):
                raise BridgeError(400, "invalid_request", "conversation_id invalido")
            entry = index_load().get(cid)
            if entry is None:
                raise BridgeError(404, "conversation_not_found", "conversacion no encontrada")
            if entry.get("engine", "agy") != engine:
                raise BridgeError(400, "engine_mismatch",
                                  "la conversacion pertenece a otro modelo (%s)." % entry.get("engine", "agy"))
            lock = CONV_LOCKS.setdefault(cid, threading.Lock())
            if not lock.acquire(blocking=False):
                raise BridgeError(409, "conversation_busy", "la conversacion tiene un turno en curso")
        try:
            if payload.get("stream"):
                self._chat_stream(message, cid, engine)
            else:
                result = run_turn(engine, message, cid, self.request_id, lambda c: None, None,
                                  lambda: client_gone(self.connection))
                index_touch(result["conversation_id"], engine)
                self._json({"conversation_id": result["conversation_id"], "response": result["response"],
                            "status": result["status"], "usage": result["usage"]})
        finally:
            if lock:
                lock.release()

    def _chat_stream(self, message, cid, engine="agy"):
        self._headers(200, "text/event-stream")
        alive = {"ok": True}

        def send(obj):
            try:
                self._sse(obj)
                return True
            except OSError:
                alive["ok"] = False
                return False

        def on_delta(text):
            if not send({"type": "delta", "text": text}):
                raise OSError("cliente desconectado")

        def keepalive():
            if client_gone(self.connection):
                alive["ok"] = False
                return False
            try:
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
                return True
            except OSError:
                alive["ok"] = False
                return False

        try:
            result = run_turn(engine, message, cid, self.request_id,
                              lambda c: send({"type": "conversation", "conversation_id": c}),
                              on_delta,
                              lambda: not alive["ok"] or client_gone(self.connection),
                              keepalive)
            index_touch(result["conversation_id"], engine)
            send({"type": "done", "conversation_id": result["conversation_id"],
                  "response": result["response"], "status": result["status"], "usage": result["usage"]})
        except BridgeError as err:
            log("stream_error", self.request_id, code=err.code)
            send({"type": "error", "code": err.code, "message": err.message})
        try:
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except OSError:
            pass

    def _show_body(self):
        return {"license": "", "modelfile": "FROM " + CONFIG["model_name"], "parameters": "",
                "template": "{{ .Prompt }}", "details": MODEL_ENTRY_DETAILS,
                "capabilities": ["completion", "tools"]}

    def do_OPTIONS(self):
        self._headers(200)

    # ---------- POST ----------
    def do_POST(self):
        path = self._path()
        try:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length > 0 else b"{}"
            self._authorized()
            try:
                payload = json.loads(raw.decode("utf-8") or "{}")
            except ValueError:
                raise BridgeError(400, "invalid_json", "cuerpo JSON invalido")
            if not isinstance(payload, dict):
                raise BridgeError(400, "invalid_json", "el cuerpo debe ser un objeto JSON")

            tools = payload.get("tools") or []
            log("request", self.request_id, path=path, model=payload.get("model"),
                stream=bool(payload.get("stream")),
                tools=len(tools) if isinstance(tools, list) else 0, bytes=len(raw))
            if path in ("/v1/chat/completions", "/chat/completions") and CONFIG["dump_requests"]:
                self._dump(raw)

            if path == "/chat":
                self._guarded(lambda: self._chat(payload))
            elif path == "/api/generate":
                self._guarded(lambda: self._ollama_generate(payload))
            elif path == "/api/chat":
                self._guarded(lambda: self._ollama_chat(payload))
            elif path in ("/v1/chat/completions", "/chat/completions"):
                self._openai_chat(payload)
            elif path == "/api/show":
                self._json(self._show_body())
            else:
                raise BridgeError(404, "not_found", "endpoint no soportado")
        except BridgeError as err:
            self._error(err)
        finally:
            self._route_done(path, self.status_sent)

    def _dump(self, raw):
        try:
            os.makedirs(CONFIG["state_dir"], exist_ok=True)
            path = os.path.join(CONFIG["state_dir"], "last_request.json")
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "wb") as fh:
                fh.write(raw)
        except OSError:
            pass

    def _guarded(self, fn):
        if not GATE.acquire(blocking=False):
            METRICS.count(METRICS.agy, "busy")
            METRICS.rejected_busy += 1
            raise BridgeError(429, "busy", "puente ocupado, reintenta en unos segundos.", retry_after=5)
        with METRICS.lock:
            METRICS.in_flight += 1
        try:
            fn()
        finally:
            with METRICS.lock:
                METRICS.in_flight -= 1
            GATE.release()

    def _check_prompt(self, prompt):
        if len(prompt) > CONFIG["max_prompt_chars"]:
            raise BridgeError(413, "prompt_too_large",
                              "prompt de %d caracteres supera el limite de %d"
                              % (len(prompt), CONFIG["max_prompt_chars"]))

    # ---------- endpoints ----------
    def _ollama_generate(self, payload):
        prompt = content_to_text(payload.get("prompt"))
        self._check_prompt(prompt)
        result = call_agy(prompt, request_id=self.request_id)
        self._json({"model": CONFIG["model_name"],
                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "response": result["response"], "done": True,
                    "prompt_eval_count": result["usage"]["prompt_tokens"],
                    "eval_count": result["usage"]["completion_tokens"]})

    def _ollama_chat(self, payload):
        prompt = build_transcript(payload)
        self._check_prompt(prompt)
        result = call_agy(prompt, request_id=self.request_id)
        self._json({"model": CONFIG["model_name"],
                    "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "message": {"role": "assistant", "content": result["response"]}, "done": True,
                    "prompt_eval_count": result["usage"]["prompt_tokens"],
                    "eval_count": result["usage"]["completion_tokens"]})

    def _openai_chat(self, payload):
        if engine_for_model(payload.get("model")) == "opencode":
            self._openai_opencode(payload)
            return
        tools = [t for t in (payload.get("tools") or []) if isinstance(t, dict)]
        tool_choice = payload.get("tool_choice")
        use_tools = bool(tools) and tool_choice != "none"
        stream = bool(payload.get("stream"))
        created = int(time.time())
        rid = "chatcmpl-%s" % self.request_id

        prompt = build_decision_prompt(tools, tool_choice, payload) if use_tools else build_transcript(payload)
        self._check_prompt(prompt)
        self._guarded(lambda: self._run_openai(payload, tools, tool_choice, use_tools, stream, created, rid))

    def _openai_opencode(self, payload):
        if not CONFIG["opencode_enabled"]:
            raise BridgeError(404, "model_not_available", "OpenCode no esta habilitado en este puente.")
        if [t for t in (payload.get("tools") or []) if isinstance(t, dict)] and payload.get("tool_choice") != "none":
            raise BridgeError(400, "tools_not_supported",
                              "el modelo %s no admite tools; usa %s." % (CONFIG["opencode_model"], CONFIG["model_name"]))
        prompt = build_transcript(payload)
        if len(prompt) > CONFIG["opencode_max_chars"]:
            raise BridgeError(413, "prompt_too_large", "conversacion demasiado larga para OpenCode.")
        stream = bool(payload.get("stream"))
        created = int(time.time())
        rid = "chatcmpl-%s" % self.request_id
        model = payload.get("model") or CONFIG["opencode_model"]
        self._guarded(lambda: self._run_openai_opencode(prompt, stream, created, rid, model))

    def _run_openai_opencode(self, prompt, stream, created, rid, model):
        if stream:
            self._headers(200, "text/event-stream")
            self._sse({"id": rid, "object": "chat.completion.chunk", "created": created, "model": model,
                       "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}]})
        alive = {"ok": True}

        def chunk(delta, finish=None, usage=None):
            obj = {"id": rid, "object": "chat.completion.chunk", "created": created, "model": model,
                   "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
            if usage:
                obj["usage"] = usage
            return obj

        def on_delta(text):
            try:
                self._sse(chunk({"content": text}))
            except OSError:
                alive["ok"] = False
                raise

        def keepalive():
            if client_gone(self.connection):
                alive["ok"] = False
                return False
            try:
                self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
                return True
            except OSError:
                alive["ok"] = False
                return False

        try:
            result = run_opencode_turn(prompt, None, self.request_id, lambda c: None,
                                       on_delta if stream else None,
                                       lambda: not alive["ok"] or client_gone(self.connection),
                                       keepalive if stream else None)
        except BridgeError as err:
            if not stream:
                raise
            self._sse({"error": {"message": err.message, "type": err.code, "code": err.code}})
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return
        usage = {"prompt_tokens": result["usage"]["prompt_tokens"],
                 "completion_tokens": result["usage"]["completion_tokens"],
                 "total_tokens": result["usage"]["total_tokens"]}
        if stream:
            self._sse(chunk({}, finish="stop", usage=usage))
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return
        self._json({"id": rid, "object": "chat.completion", "created": created, "model": model,
                    "choices": [{"index": 0, "message": {"role": "assistant", "content": result["response"]},
                                 "finish_reason": "stop"}],
                    "usage": usage})

    def _run_openai(self, payload, tools, tool_choice, use_tools, stream, created, rid):
        if stream:
            self._headers(200, "text/event-stream")
            self._sse(self._chunk(rid, created, delta={"role": "assistant"}))

        alive = {"ok": True}

        def keepalive():
            if not stream:
                return not client_gone(self.connection)
            if client_gone(self.connection) or not self._keepalive():
                alive["ok"] = False
            return alive["ok"]

        def cancelled():
            return client_gone(self.connection) if not stream else not alive["ok"]

        try:
            if use_tools:
                text, tool_calls, usage = decide(tools, tool_choice, payload, self.request_id,
                                                 keepalive, cancelled)
            else:
                result = call_agy(build_transcript(payload), None, self.request_id, keepalive, cancelled)
                text, tool_calls, usage = result["response"], [], result["usage"]
        except BridgeError as err:
            if not stream:
                raise
            log("stream_error", self.request_id, code=err.code)
            self._sse({"error": {"message": err.message, "type": err.code, "code": err.code}})
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return

        if not stream:
            if tool_calls:
                body = {"id": rid, "object": "chat.completion", "created": created,
                        "model": CONFIG["model_name"],
                        "choices": [{"index": 0,
                                     "message": {"role": "assistant", "content": None, "tool_calls": tool_calls},
                                     "finish_reason": "tool_calls"}],
                        "usage": usage}
            else:
                body = {"id": rid, "object": "chat.completion", "created": created,
                        "model": CONFIG["model_name"],
                        "choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                                     "finish_reason": "stop"}],
                        "usage": usage}
            self._json(body)
            return

        if tool_calls:
            for index, call in enumerate(tool_calls):
                self._sse(self._chunk(rid, created, delta={"tool_calls": [{
                    "index": index, "id": call["id"], "type": "function", "function": call["function"]}]}))
            self._sse(self._chunk(rid, created, delta={}, finish="tool_calls", usage=usage))
        else:
            if text:
                self._sse(self._chunk(rid, created, delta={"content": text}))
            self._sse(self._chunk(rid, created, delta={}, finish="stop", usage=usage))
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


class ThreadingServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


def serve():
    if not CONFIG["bridge_token"] and not CONFIG["allow_noauth"]:
        raise SystemExit("BRIDGE_TOKEN no esta definido. Define BRIDGE_TOKEN o BRIDGE_ALLOW_NOAUTH=1 (no recomendado).")
    log("startup", version=VERSION, port=CONFIG["port"], model=CONFIG["model_name"],
        agy_bin=CONFIG["agy_bin"], agy_model=CONFIG["agy_model"] or "(default)",
        max_concurrency=CONFIG["max_concurrency"], auth="token" if CONFIG["bridge_token"] else "none",
        telegram="on" if CONFIG["telegram_token"] and CONFIG["telegram_chat_id"] else "off",
        retention_days=CONFIG["retention_days"])
    if CONFIG["retention_days"] > 0:
        threading.Thread(target=_pruner_loop, daemon=True).start()
    httpd = ThreadingServer((CONFIG["host"], CONFIG["port"]), BridgeHandler)

    def stop(*_):
        log("shutdown")
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()


if __name__ == "__main__":
    serve()
