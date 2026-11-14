"""Pruebas del puente con un agy falso. Ejecutar: python3 -m unittest discover -s tests -v"""
import json
import os
import re
import socketserver
import stat
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import server  # noqa: E402

FAKE_AGY = r'''#!/usr/bin/env python3
import json, os, sys, time, uuid
argv = sys.argv
mode = os.environ.get("FAKE_AGY_MODE", "answer")
line = sys.stdin.readline()
try:
    user_text = json.loads(line)["message"]["content"]
except Exception:
    user_text = ""
if "--conversation" in argv:
    cid = argv[argv.index("--conversation") + 1]
else:
    cid = str(uuid.uuid4())
usage = {"input_tokens": 11, "output_tokens": 7, "total_tokens": 18}

def emit(result):
    print(json.dumps({"event": "init", "conversation_id": cid, "init": {"cwd": "/app"}}), flush=True)
    text = result.get("response") or ""
    if text:
        half = max(1, len(text) // 2)
        for piece in (text[:half], text[half:]):
            if piece:
                print(json.dumps({"event": "step_update", "step_update": {
                    "conversation_id": cid, "step_type": "agent_response", "state": "ACTIVE",
                    "text_delta": piece}}), flush=True)
    result = dict(result, conversation_id=cid)
    print(json.dumps({"event": "result", "result": result}), flush=True)

if mode == "hang":
    time.sleep(60)
elif mode == "noresult":
    sys.stderr.write("boom\n"); sys.exit(1)
elif mode == "auth":
    emit({"status": "ERROR", "error": "OAuth token expired, please login again", "usage": usage})
elif mode == "quota":
    emit({"status": "ERROR", "error": "You have exhausted your quota on this model.", "usage": usage})
elif mode == "text":
    emit({"status": "SUCCESS", "response": "texto libre", "usage": usage})
elif mode == "echo":
    emit({"status": "SUCCESS", "response": "eco: " + user_text, "usage": usage})
elif mode == "tool":
    emit({"status": "SUCCESS", "response": "", "usage": usage,
          "structured_output": {"action": "tool_call", "tool": "get_account", "arguments": {"id": "abc"}}})
elif mode == "badargs":
    emit({"status": "SUCCESS", "response": "", "usage": usage,
          "structured_output": {"action": "tool_call", "tool": "get_account", "arguments": {}}})
elif mode == "retry":
    counter = os.environ["FAKE_AGY_COUNTER"]
    n = int(open(counter).read()) if os.path.exists(counter) else 0
    open(counter, "w").write(str(n + 1))
    if n == 0:
        emit({"status": "SUCCESS", "response": "", "usage": usage,
              "structured_output": {"action": "tool_call", "tool": "nope", "arguments": {}}})
    else:
        emit({"status": "SUCCESS", "response": "", "usage": usage,
              "structured_output": {"action": "answer", "content": "listo"}})
else:
    emit({"status": "SUCCESS", "response": "hola", "usage": usage,
          "structured_output": {"action": "answer", "content": "hola"}})
'''

FAKE_OPENCODE = r'''#!/usr/bin/env python3
import json, random, string, sys, time
argv = sys.argv[1:]
message = argv[-1]
if "--session" in argv:
    sid = argv[argv.index("--session") + 1]
else:
    sid = "ses_" + "".join(random.choice(string.ascii_letters + string.digits) for _ in range(20))

def ev(obj):
    print(json.dumps(obj), flush=True)

if "[[hang]]" in message:
    time.sleep(60)
if "[[quota]]" in message:
    ev({"type": "step_start", "sessionID": sid, "part": {"type": "step-start"}})
    ev({"type": "error", "sessionID": sid, "error": {"type": "provider.free-limit", "status": 429,
        "message": "Free usage limit reached.", "retryAfterMs": 120000}})
    sys.exit(0)
text = ("eco: " + message) if "[[echo]]" in message else "hola desde opencode"
ev({"type": "step_start", "sessionID": sid, "part": {"type": "step-start"}})
ev({"type": "text", "sessionID": sid, "part": {"type": "text", "text": text}})
ev({"type": "step_finish", "sessionID": sid, "part": {"type": "step-finish", "tokens": {"input": 7, "output": 3}}})
'''

TOOLS = [{"type": "function", "function": {
    "name": "get_account", "description": "cuenta",
    "parameters": {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}}}]
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class BridgeTest(unittest.TestCase):
    token = "test-token"

    @classmethod
    def setUpClass(cls):
        cls.dir = tempfile.mkdtemp()
        cls.agy = os.path.join(cls.dir, "agy")
        with open(cls.agy, "w") as fh:
            fh.write(FAKE_AGY)
        os.chmod(cls.agy, os.stat(cls.agy).st_mode | stat.S_IEXEC)
        cls.oc = os.path.join(cls.dir, "opencode")
        with open(cls.oc, "w") as fh:
            fh.write(FAKE_OPENCODE)
        os.chmod(cls.oc, os.stat(cls.oc).st_mode | stat.S_IEXEC)
        server.CONFIG.update({
            "agy_bin": cls.agy, "bridge_token": cls.token, "agy_timeout": 3,
            "max_concurrency": 2, "decision_retries": 1, "retention_days": 0,
            "agy_home": cls.dir, "state_dir": cls.dir, "log_format": "text",
            "max_prompt_chars": 300000, "telegram_token": None, "telegram_chat_id": None,
            "chat_mode": "plan",
            "opencode_enabled": True, "opencode_bin": cls.oc, "opencode_timeout": 3,
            "opencode_model": "opencode/big-pickle", "opencode_uid": 0, "opencode_home": cls.dir,
            "opencode_max_chars": 100000,
        })
        server.GATE = threading.BoundedSemaphore(server.CONFIG["max_concurrency"])
        cls.httpd = socketserver.ThreadingTCPServer(("127.0.0.1", 0), server.BridgeHandler)
        cls.httpd.daemon_threads = True
        cls.port = cls.httpd.server_address[1]
        threading.Thread(target=cls.httpd.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()

    def setUp(self):
        os.environ["FAKE_AGY_MODE"] = "answer"
        counter = os.path.join(self.dir, "counter")
        if os.path.exists(counter):
            os.remove(counter)
        os.environ["FAKE_AGY_COUNTER"] = counter
        server.CONFIG["agy_timeout"] = 3
        server.CONFIG["decision_retries"] = 1
        server.CONFIG["chat_mode"] = "plan"
        server.GATE = threading.BoundedSemaphore(server.CONFIG["max_concurrency"])
        index = os.path.join(self.dir, "conversations.json")
        if os.path.exists(index):
            os.remove(index)

    def request(self, path, body=None, token=None, method=None):
        url = "http://127.0.0.1:%d%s" % (self.port, path)
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, method=method or ("POST" if data else "GET"))
        req.add_header("Content-Type", "application/json")
        if token is not None:
            req.add_header("Authorization", "Bearer " + token)
        try:
            with urllib.request.urlopen(req, timeout=20) as resp:
                raw = resp.read().decode("utf-8")
                ctype = resp.headers.get("Content-Type", "")
                return resp.status, (json.loads(raw) if "json" in ctype else raw), resp.headers
        except urllib.error.HTTPError as err:
            raw = err.read().decode("utf-8")
            try:
                return err.code, json.loads(raw), err.headers
            except ValueError:
                return err.code, raw, err.headers

    def chat(self, **extra):
        body = {"model": "antigravity-session", "messages": [{"role": "user", "content": "hola"}]}
        body.update(extra)
        return self.request("/v1/chat/completions", body, token=self.token)

    def stream_post(self, path, body):
        url = "http://127.0.0.1:%d%s" % (self.port, path)
        req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", "Bearer " + self.token)
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.read().decode("utf-8")

    # auth
    def test_rejects_missing_token(self):
        status, body, _ = self.request("/v1/chat/completions",
                                       {"messages": [{"role": "user", "content": "x"}]})
        self.assertEqual(status, 401)
        self.assertEqual(body["error"]["type"], "unauthorized")

    def test_rejects_wrong_token(self):
        status, _, _ = self.request("/api/tags", token="nope")
        self.assertEqual(status, 401)

    def test_health_and_metrics_are_public(self):
        status, body, _ = self.request("/health")
        self.assertIn(status, (200, 503))
        self.assertIn("checks", body)
        self.assertEqual(body["chat"]["mode"], "plan")
        status, text, _ = self.request("/metrics")
        self.assertEqual(status, 200)
        self.assertIn("bridge_http_requests_total", text)

    # texto libre / OpenAI
    def test_plain_answer(self):
        status, body, _ = self.chat()
        self.assertEqual(status, 200)
        self.assertEqual(body["choices"][0]["message"]["content"], "hola")
        self.assertEqual(body["choices"][0]["finish_reason"], "stop")

    def test_text_mode_without_schema(self):
        os.environ["FAKE_AGY_MODE"] = "text"
        status, body, _ = self.chat()
        self.assertEqual(body["choices"][0]["message"]["content"], "texto libre")

    def test_valid_tool_call(self):
        os.environ["FAKE_AGY_MODE"] = "tool"
        status, body, _ = self.chat(tools=TOOLS)
        self.assertEqual(status, 200)
        choice = body["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        call = choice["message"]["tool_calls"][0]
        self.assertEqual(call["function"]["name"], "get_account")
        self.assertEqual(json.loads(call["function"]["arguments"]), {"id": "abc"})

    def test_invalid_args_fail_after_retries(self):
        os.environ["FAKE_AGY_MODE"] = "badargs"
        status, body, _ = self.chat(tools=TOOLS)
        self.assertEqual(status, 502)
        self.assertEqual(body["error"]["type"], "invalid_decision")

    def test_unknown_tool_is_retried_then_accepted(self):
        os.environ["FAKE_AGY_MODE"] = "retry"
        status, body, _ = self.chat(tools=TOOLS)
        self.assertEqual(status, 200)
        self.assertEqual(body["choices"][0]["message"]["content"], "listo")

    def test_streaming_tool_call(self):
        os.environ["FAKE_AGY_MODE"] = "tool"
        text = self.stream_post("/v1/chat/completions",
                                {"messages": [{"role": "user", "content": "x"}], "tools": TOOLS, "stream": True})
        self.assertIn('"tool_calls"', text)
        self.assertIn("[DONE]", text)

    # chat de sesion (como el chat de agy)
    def test_chat_new_conversation(self):
        status, body, _ = self.request("/chat", {"message": "hola"}, token=self.token)
        self.assertEqual(status, 200)
        self.assertRegex(body["conversation_id"], UUID_RE)
        self.assertEqual(body["response"], "hola")

    def test_chat_resume_keeps_id_and_counts_turns(self):
        os.environ["FAKE_AGY_MODE"] = "echo"
        _, first, _ = self.request("/chat", {"message": "uno"}, token=self.token)
        cid = first["conversation_id"]
        status, second, _ = self.request("/chat", {"message": "dos", "conversation_id": cid}, token=self.token)
        self.assertEqual(status, 200)
        self.assertEqual(second["conversation_id"], cid)
        self.assertEqual(second["response"], "eco: dos")
        status, listing, _ = self.request("/chat/conversations", token=self.token)
        entry = [c for c in listing["conversations"] if c["id"] == cid][0]
        self.assertEqual(entry["turns"], 2)

    def test_chat_unknown_conversation_404(self):
        status, body, _ = self.request("/chat", {"message": "x", "conversation_id":
                                                 "00000000-0000-0000-0000-000000000000"}, token=self.token)
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["type"], "conversation_not_found")

    def test_chat_invalid_conversation_id_400(self):
        status, body, _ = self.request("/chat", {"message": "x", "conversation_id": "../../etc"},
                                       token=self.token)
        self.assertEqual(status, 400)

    def test_chat_rejects_slash_commands(self):
        status, body, _ = self.request("/chat", {"message": "/mode auto"}, token=self.token)
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["type"], "slash_commands_disabled")

    def test_chat_requires_message(self):
        status, _, _ = self.request("/chat", {"message": "   "}, token=self.token)
        self.assertEqual(status, 400)

    def test_chat_stream_emits_deltas_and_done(self):
        os.environ["FAKE_AGY_MODE"] = "echo"
        text = self.stream_post("/chat", {"message": "stream", "stream": True})
        self.assertIn('"type": "conversation"', text)
        self.assertIn('"type": "delta"', text)
        self.assertIn('"type": "done"', text)
        self.assertIn("[DONE]", text)

    def test_chat_delete_then_gone(self):
        _, first, _ = self.request("/chat", {"message": "uno"}, token=self.token)
        cid = first["conversation_id"]
        status, _, _ = self.request("/chat/conversations/" + cid, token=self.token, method="DELETE")
        self.assertEqual(status, 200)
        status, body, _ = self.request("/chat/conversations/" + cid, token=self.token, method="DELETE")
        self.assertEqual(status, 404)

    def test_chat_busy_conversation_409(self):
        _, first, _ = self.request("/chat", {"message": "uno"}, token=self.token)
        cid = first["conversation_id"]
        os.environ["FAKE_AGY_MODE"] = "hang"
        server.CONFIG["agy_timeout"] = 4
        result = {}

        def slow():
            result["first"] = self.request("/chat", {"message": "lento", "conversation_id": cid},
                                           token=self.token)

        worker = threading.Thread(target=slow)
        worker.start()
        time.sleep(0.5)
        status, body, _ = self.request("/chat", {"message": "otro", "conversation_id": cid}, token=self.token)
        worker.join(30)
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["type"], "conversation_busy")

    # OpenCode como segundo modelo del mismo puerto
    def test_models_lists_both(self):
        status, body, _ = self.request("/v1/models", token=self.token)
        ids = [m["id"] for m in body["data"]]
        self.assertEqual(ids, ["antigravity-session", "opencode/big-pickle"])

    def test_models_hides_opencode_when_disabled(self):
        server.CONFIG["opencode_enabled"] = False
        try:
            _, body, _ = self.request("/v1/models", token=self.token)
        finally:
            server.CONFIG["opencode_enabled"] = True
        self.assertEqual([m["id"] for m in body["data"]], ["antigravity-session"])

    def test_openai_opencode_model_answer(self):
        status, body, _ = self.request("/v1/chat/completions",
                                       {"model": "opencode/big-pickle",
                                        "messages": [{"role": "user", "content": "hola"}]}, token=self.token)
        self.assertEqual(status, 200)
        self.assertEqual(body["model"], "opencode/big-pickle")
        self.assertEqual(body["choices"][0]["message"]["content"], "hola desde opencode")
        self.assertEqual(body["usage"]["prompt_tokens"], 7)

    def test_openai_opencode_stream(self):
        text = self.stream_post("/v1/chat/completions",
                                {"model": "opencode/big-pickle", "stream": True,
                                 "messages": [{"role": "user", "content": "[[echo]] x"}]})
        self.assertIn('"content": "eco: ', text)
        self.assertIn('"finish_reason": "stop"', text)
        self.assertIn("[DONE]", text)

    def test_openai_opencode_rejects_tools(self):
        status, body, _ = self.request("/v1/chat/completions",
                                       {"model": "opencode/big-pickle", "tools": TOOLS,
                                        "messages": [{"role": "user", "content": "x"}]}, token=self.token)
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["type"], "tools_not_supported")

    def test_opencode_disabled_returns_404(self):
        server.CONFIG["opencode_enabled"] = False
        try:
            status, body, _ = self.request("/v1/chat/completions",
                                           {"model": "opencode/big-pickle",
                                            "messages": [{"role": "user", "content": "x"}]}, token=self.token)
        finally:
            server.CONFIG["opencode_enabled"] = True
        self.assertEqual(status, 404)
        self.assertEqual(body["error"]["type"], "model_not_available")

    def test_opencode_quota_maps_429_with_retry(self):
        status, body, headers = self.request("/v1/chat/completions",
                                             {"model": "opencode/big-pickle",
                                              "messages": [{"role": "user", "content": "[[quota]]"}]},
                                             token=self.token)
        self.assertEqual(status, 429)
        self.assertEqual(body["error"]["type"], "opencode_quota")
        self.assertEqual(headers.get("Retry-After"), "120")

    def test_chat_opencode_new_and_resume(self):
        status, first, _ = self.request("/chat", {"model": "opencode/big-pickle", "message": "[[echo]] uno"},
                                        token=self.token)
        self.assertEqual(status, 200)
        cid = first["conversation_id"]
        self.assertTrue(cid.startswith("ses_"))
        status, second, _ = self.request("/chat", {"model": "opencode/big-pickle", "conversation_id": cid,
                                                   "message": "[[echo]] dos"}, token=self.token)
        self.assertEqual(status, 200)
        self.assertEqual(second["conversation_id"], cid)
        self.assertEqual(second["response"], "eco: [[echo]] dos")

    def test_chat_engine_mismatch_400(self):
        _, agy_conv, _ = self.request("/chat", {"message": "hola"}, token=self.token)
        status, body, _ = self.request("/chat", {"model": "opencode/big-pickle",
                                                 "conversation_id": agy_conv["conversation_id"],
                                                 "message": "x"}, token=self.token)
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["type"], "engine_mismatch")

    def test_opencode_message_starting_with_dash(self):
        status, body, _ = self.request("/chat", {"model": "opencode/big-pickle", "message": "-x [[echo]]"},
                                       token=self.token)
        self.assertEqual(status, 200)

    # errores
    def test_auth_error_maps_to_503(self):
        os.environ["FAKE_AGY_MODE"] = "auth"
        status, body, _ = self.chat()
        self.assertEqual(status, 503)
        self.assertEqual(body["error"]["type"], "agy_auth")

    def test_quota_error_maps_to_429(self):
        os.environ["FAKE_AGY_MODE"] = "quota"
        status, body, headers = self.chat()
        self.assertEqual(status, 429)
        self.assertEqual(body["error"]["type"], "agy_quota")
        self.assertEqual(headers.get("Retry-After"), "60")

    def test_no_result_maps_to_502(self):
        os.environ["FAKE_AGY_MODE"] = "noresult"
        status, body, _ = self.chat()
        self.assertEqual(status, 502)
        self.assertEqual(body["error"]["type"], "agy_error")

    def test_timeout_maps_to_504(self):
        os.environ["FAKE_AGY_MODE"] = "hang"
        server.CONFIG["agy_timeout"] = 2
        started = time.time()
        status, body, _ = self.chat()
        self.assertEqual(status, 504)
        self.assertEqual(body["error"]["type"], "agy_timeout")
        self.assertLess(time.time() - started, 20)

    def test_prompt_too_large_maps_to_413(self):
        server.CONFIG["max_prompt_chars"] = 100
        try:
            status, body, _ = self.request("/v1/chat/completions",
                                           {"messages": [{"role": "user", "content": "x" * 500}]},
                                           token=self.token)
        finally:
            server.CONFIG["max_prompt_chars"] = 300000
        self.assertEqual(status, 413)
        self.assertEqual(body["error"]["type"], "prompt_too_large")

    def test_invalid_json_maps_to_400(self):
        req = urllib.request.Request("http://127.0.0.1:%d/v1/chat/completions" % self.port,
                                     data=b"{nope", method="POST")
        req.add_header("Authorization", "Bearer " + self.token)
        try:
            urllib.request.urlopen(req, timeout=10)
            self.fail("expected 400")
        except urllib.error.HTTPError as err:
            self.assertEqual(err.code, 400)

    def test_busy_returns_429_with_retry_after(self):
        os.environ["FAKE_AGY_MODE"] = "hang"
        server.CONFIG["agy_timeout"] = 4
        server.GATE = threading.BoundedSemaphore(1)
        server.GATE.acquire()
        try:
            status, body, headers = self.chat()
        finally:
            server.GATE.release()
        self.assertEqual(status, 429)
        self.assertEqual(body["error"]["type"], "busy")
        self.assertEqual(headers.get("Retry-After"), "5")

    def test_ollama_chat(self):
        status, body, _ = self.request("/api/chat",
                                       {"messages": [{"role": "user", "content": "hola"}]},
                                       token=self.token)
        self.assertEqual(status, 200)
        self.assertEqual(body["message"]["content"], "hola")

    def test_unknown_endpoint_404(self):
        status, _, _ = self.request("/responses", {"x": 1}, token=self.token)
        self.assertEqual(status, 404)


class ValidateTest(unittest.TestCase):
    schema = {"type": "object", "properties": {"id": {"type": "string"}, "qty": {"type": "integer"},
                                              "side": {"type": "string", "enum": ["buy", "sell"]}},
              "required": ["id"]}

    def test_ok(self):
        self.assertIsNone(server.validate_arguments({"id": "a", "qty": 2, "side": "buy"}, self.schema))

    def test_missing_required(self):
        self.assertIn("requerido", server.validate_arguments({}, self.schema))

    def test_wrong_type(self):
        self.assertIn("integer", server.validate_arguments({"id": "a", "qty": "2"}, self.schema))

    def test_bool_is_not_integer(self):
        self.assertIn("integer", server.validate_arguments({"id": "a", "qty": True}, self.schema))

    def test_enum(self):
        self.assertIn("permitidos", server.validate_arguments({"id": "a", "side": "hold"}, self.schema))


if __name__ == "__main__":
    unittest.main()
