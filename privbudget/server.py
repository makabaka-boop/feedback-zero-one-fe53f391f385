"""HTTP API for the budget service (standard library only).

Endpoints (JSON in, JSON out)::

    POST /initialize  {"bits": {...}, "groups": {...}, "limits": {...}}
                      -> {"state":"initialized"} | {"error":"<jsonpath>"}
    POST /start       {"job":"j","group":"g","epsilon":"1/1"}
                      -> {"state":"pending"} | {"error":...}
    POST /status      {"job":"j"} -> {"state":"..."}
    POST /read        {"job":"j"} -> {"value": <float>}
    POST /cancel      {"job":"j"} -> {"state":"cancelled"}
    GET  /budgets     -> {"a": {"spent":"0/1","reserved":"1/1"}, ...}

``start``/``status``/``read``/``cancel`` also accept ``{job: ...}`` keys only;
unknown extra keys are ignored.  Errors describe the offending value with a
zero-based JSONPath relative to the request body (or, for ``initialize``, to
the configuration object itself).

Long-polling status: a ``status`` request may include ``"wait": <seconds>``
(0..60); the service returns as soon as the job leaves ``pending`` or the
timeout elapses.  This keeps clients responsive without changing semantics.
"""

from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_BODY = 1 << 20

READ_METHODS = {"start", "status", "read", "cancel"}


class Api:
    def __init__(self, service):
        self.service = service
        self.cond = threading.Condition()

    def touch(self):
        with self.cond:
            self.cond.notify_all()

    def wait_for_state(self, job, wait_seconds):
        deadline = time.monotonic() + wait_seconds
        while True:
            current = self.service.status(job)
            state = current.get("state")
            if state != "pending" or time.monotonic() >= deadline:
                return current
            remaining = deadline - time.monotonic()
            with self.cond:
                self.cond.wait(min(remaining, 0.2))

    def initialize(self, body):
        return self.service.initialize(body)

    def dispatch(self, name, body):
        svc = self.service
        if not isinstance(body, dict):
            return {"error": "$"}
        if name == "start":
            return svc.start(
                body.get("job"), body.get("group"), body.get("epsilon"),
                noise=self._noise(body),
            )
        if name == "status":
            try:
                wait = body.get("wait", 0)
            except AttributeError:
                return {"error": "$"}
            if isinstance(wait, bool) or not isinstance(wait, (int, float)):
                wait = 0
            wait = max(0.0, min(float(wait), 60.0))
            if wait > 0:
                return self.wait_for_state(body.get("job"), wait)
            return svc.status(body.get("job"))
        if name == "read":
            return svc.read(body.get("job"))
        if name == "cancel":
            return svc.cancel(body.get("job"))
        raise KeyError(name)

    def _noise(self, body):
        if body.get("noise") is None:
            return None
        return body["noise"]

    def budgets(self):
        return self.service.budgets()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "privbudget/1"

    def log_message(self, fmt, *args):
        pass

    def _send(self, code, obj):
        data = json.dumps(obj, separators=(",", ":"), allow_nan=False).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            self.wfile.flush()
        except OSError:
            pass

    def do_GET(self):
        if self.path.rstrip("/") == "/budgets":
            self._send(200, self.server.api.budgets())
            return
        if self.path.rstrip("/") in ("/health", ""):
            self._send(200, {"ok": True})
            return
        self._send(404, {"error": "$"})

    def do_POST(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if length < 0 or length > MAX_BODY:
            self._send(413, {"error": "$"})
            return
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw) if raw else {}
        except ValueError:
            self._send(200, {"error": "$"})
            return
        if not isinstance(body, dict):
            self._send(200, {"error": "$"})
            return
        name = self.path.strip("/")
        if name == "initialize":
            if self.server.api.service.is_initialized():
                self._send(200, {"error": "$"})
                return
        try:
            if name == "initialize":
                result = self.server.api.initialize(body)
            elif name in READ_METHODS:
                result = self.server.api.dispatch(name, body)
            elif name == "flush":
                self.server.api.service.replay_pending()
                result = {"state": "ok"}
            else:
                self._send(404, {"error": "$"})
                return
        except Exception as exc:  # noqa: BLE001 - never leak a stack trace
            self._send(500, {"error": "$"})
            return
        self.server.api.touch()
        self._send(200, result)


class Server:
    def __init__(self, service, host="127.0.0.1", port=0):
        self.api = Api(service)
        self.httpd = ThreadingHTTPServer((host, port), Handler)
        self.httpd.daemon_threads = True
        self.httpd.api = self.api
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       kwargs={"poll_interval": 0.1},
                                       name="privbudget-http", daemon=True)
        self.thread.start()

    @property
    def port(self):
        return self.httpd.server_address[1]

    @property
    def url(self):
        return "http://127.0.0.1:%d" % self.port

    def close(self):
        try:
            self.httpd.shutdown()
        except Exception:
            pass
        try:
            self.httpd.server_close()
        except Exception:
            pass
